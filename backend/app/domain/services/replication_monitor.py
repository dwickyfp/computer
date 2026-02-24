"""
Replication Monitor service for background monitoring.

Implements PostgreSQL replication and publication status monitoring.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_session_context
from app.core.security import decrypt_value
from app.core.logging import get_logger
from app.domain.models.source import Source
from app.domain.repositories.source import SourceRepository

logger = get_logger(__name__)


class ReplicationMonitorService:
    """
    Service for monitoring PostgreSQL replication and publication status.

    Periodically checks replication slots and publication status for all registered sources
    and updates the source records.
    """

    # Check replication slot existence
    REPLICATION_SLOT_QUERY = """
        SELECT count(*) as count
        FROM pg_replication_slots 
        WHERE slot_name = %(slot_name)s
    """

    # Check publication and count tables
    PUBLICATION_QUERY = """
        SELECT count(*) as total_tables
        FROM pg_publication_tables pt
        JOIN pg_publication p ON pt.pubname = p.pubname
        JOIN pg_class c ON c.relname = pt.tablename
        WHERE p.pubname = %(pubname)s;
    """

    def __init__(self):
        """Initialize replication monitor service."""
        self.settings = get_settings()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def check_source_status(self, source: Source) -> dict:
        """
        Check replication and publication status for a specific source.

        Args:
            source: Source to check

        Returns:
            Dictionary with status results
        """

        def _check_sync():
            """Synchronous check using psycopg2."""
            connection = None
            try:
                # Create connection to source database
                connection = psycopg2.connect(
                    host=source.pg_host,
                    port=source.pg_port,
                    database=source.pg_database,
                    user=source.pg_username,
                    password=decrypt_value(source.pg_password) if source.pg_password else None,
                    connect_timeout=self.settings.wal_monitor_timeout_seconds, # Reuse timeout setting
                )

                slot_name = source.replication_name
                
                logger.debug(
                    "Checking replication status",
                    extra={
                        "source_id": source.id,
                        "slot_name": slot_name,
                        "publication": source.publication_name,
                    },
                )

                verification_result = {
                    "is_replication_enabled": False,
                    "is_publication_enabled": False,
                    "total_tables": 0
                }

                with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Check replication slot
                    cursor.execute(self.REPLICATION_SLOT_QUERY, {"slot_name": slot_name})
                    result = cursor.fetchone()
                    if result and result["count"] > 0:
                        verification_result["is_replication_enabled"] = True

                    # Check publication
                    cursor.execute(self.PUBLICATION_QUERY, {"pubname": source.publication_name})
                    result = cursor.fetchone()
                    if result:
                        # If count > 0 it implies the publication exists AND has tables
                        # But user specified: "if exist, than count total tables, update the column"
                        # The query joins pg_publication, so if no rows returned, it effectively means no tables or no publication (or empty publication).
                        # Ideally check pg_publication existence separately, but current query counts tables in it.
                        # If count > 0, then publication definitely exists.
                        # If count == 0, it could be publication exists but no tables.
                        
                        # Let's adjust logic: 
                        # To verify if publication exists strictly, we should query pg_publication directly.
                        # But based on user request "check publication slot... (query)... if exist, than count total tables"
                        # The provided query already joins everything.
                        # If the output shows tables, then publication is enabled/valid.
                        
                        # Let's check specifically if publication exists regardless of tables
                        cursor.execute("SELECT 1 FROM pg_publication WHERE pubname = %(pubname)s", {"pubname": source.publication_name})
                        pub_exists = cursor.fetchone()
                        
                        if pub_exists:
                            verification_result["is_publication_enabled"] = True
                            # usage of previous result for count
                            verification_result["total_tables"] = result["total_tables"]
                        else:
                            verification_result["is_publication_enabled"] = False
                            verification_result["total_tables"] = 0

                return verification_result

            except Exception as e:
                logger.error(
                    "Error checking source status",
                    extra={"source_id": source.id, "error": str(e)},
                )
                # In case of connection failure, we probably shouldn't set enabled to false if it was true,
                # or maybe we should? The user wants to "check if exists". If we can't connect, we can't verify.
                # Returning empty/false dict seems executed for now, but logs will show error.
                raise e
            finally:
                if connection:
                    connection.close()

        return await asyncio.to_thread(_check_sync)

    async def monitor_source(self, source: Source, db: Session) -> None:
        """
        Monitor and update status for a single source.

        Args:
            source: Source to monitor
            db: Database session
        """
        try:
            status_result = await self.check_source_status(source)
            
            # Update source record
            # We need to fetch the fresh object from current session or merge
            # source is likely detached or from another session if passed from get_all outside
            # But here we pass 'db' which is used to get sources in 'monitor_all_sources'
            
            source.is_replication_enabled = status_result["is_replication_enabled"]
            source.is_publication_enabled = status_result["is_publication_enabled"]
            source.total_tables = status_result["total_tables"]
            source.last_check_replication_publication = datetime.now(timezone(timedelta(hours=7)))
            
            db.add(source)
            db.commit()

            logger.info(
                "Source replication status updated",
                extra={
                    "source_id": source.id,
                    "replication": source.is_replication_enabled,
                    "publication": source.is_publication_enabled,
                    "tables": source.total_tables
                },
            )

        except Exception as e:
            logger.error(
                "Failed to update source status",
                extra={"source_id": source.id, "error": str(e)},
            )
            # We still update the check timestamp? Or fail completely?
            # User said "every task running, always update : last_check_replication_publication"
            # So even on failure we might want to update the timestamp to show we tried?
            # But if we failed to connect, maybe we shouldn't update the status flags.
            try:
                source.last_check_replication_publication = datetime.now(timezone(timedelta(hours=7)))
                db.add(source)
                db.commit()
            except:
                pass

    async def monitor_all_sources(self) -> None:
        """
        Monitor all sources.
        """
        try:
            logger.info("Starting replication monitoring cycle")

            with get_session_context() as db:
                source_repo = SourceRepository(db)
                sources = source_repo.get_all(skip=0, limit=1000)

                if not sources:
                    return

                tasks = [self.monitor_source(source, db) for source in sources]
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error("Error in replication monitoring cycle", extra={"error": str(e)})

    async def start(self) -> None:
        """Start the background task."""
        if self._running:
            return

        self._running = True
        logger.info("Starting replication monitor")
        self._task = asyncio.create_task(self._monitoring_loop())

    def stop(self) -> None:
        """Stop the background task."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            # Cannot await in sync method
            self._task = None
        logger.info("Replication monitor stopped")

    async def _monitoring_loop(self) -> None:
        """Main loop."""
        while self._running:
            try:
                await self.monitor_all_sources()
                # Use same interval as WAL monitor for now, or default to 60s
                interval = getattr(self.settings, 'replication_monitor_interval_seconds', 60)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in replication monitoring loop", extra={"error": str(e)})
                await asyncio.sleep(60)
