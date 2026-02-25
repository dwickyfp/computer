"""
Background worker to monitor the health of Catalog Tables' Redis Streams.
"""

import logging
import threading
import time
from datetime import datetime, timezone
import redis

from config.config import get_config
from core.database import DatabaseSession

logger = logging.getLogger(__name__)


class CatalogHealthWorker:
    """
    Periodically checks if the Redis Streams for Catalog Tables exist and are healthy.
    Updates the table's status in the PostgreSQL database.
    """

    def __init__(self, check_interval_seconds: int = 60):
        self.check_interval_seconds = check_interval_seconds
        self.config = get_config()

        # Initialize Redis connection from DLQ redis_url
        try:
            self.redis_client = redis.Redis.from_url(
                self.config.dlq.redis_url,
                decode_responses=True,
                socket_timeout=5.0,
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize Redis client in CatalogHealthWorker: {e}"
            )
            self.redis_client = None

    def run(self, stop_event: threading.Event) -> None:
        """Run the health check loop."""
        logger.info(
            f"Catalog health worker started (interval={self.check_interval_seconds}s)"
        )

        if not self.redis_client:
            logger.error("Redis client not initialized, exiting CatalogHealthWorker")
            return

        while not stop_event.is_set():
            try:
                self._check_health()
            except Exception as e:
                logger.error(
                    f"Error in Catalog health worker iteration: {e}", exc_info=True
                )

            stop_event.wait(self.check_interval_seconds)

    def _check_health(self) -> None:
        """Perform the actual health check against the DB and Redis."""
        try:
            with DatabaseSession(autocommit=True) as check_session:
                check_session.execute("SELECT id, stream_name FROM catalog_tables")
                tables = check_session.fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch catalog tables for health check: {e}")
            return

        if not tables:
            return  # Nothing to check

        now = datetime.now(timezone.utc)
        updates = []

        for table in tables:
            stream_name = table["stream_name"]
            try:
                # Check if stream exists and has elements
                # XINFO STREAM returns info if exists, raises redis.exceptions.ResponseError if not
                stream_info = self.redis_client.xinfo_stream(stream_name)
                length = stream_info.get("length", 0)
                status = "ACTIVE" if length > 0 else "IDLE"
            except redis.exceptions.ResponseError as e:
                # no such key
                if "no such key" in str(e).lower():
                    status = "MISSING"
                else:
                    status = "ERROR"
            except Exception as e:
                logger.debug(f"Error checking stream {stream_name}: {e}")
                status = "ERROR"

            updates.append(
                {"status": status, "last_health_check_at": now, "table_id": table["id"]}
            )

        if updates:
            try:
                with DatabaseSession(autocommit=False) as update_session:
                    update_session.executemany(
                        """
                        UPDATE catalog_tables 
                        SET status = %(status)s, last_health_check_at = %(last_health_check_at)s
                        WHERE id = %(table_id)s
                        """,
                        updates,
                    )
            except Exception as e:
                logger.error(f"Failed to update catalog tables health status: {e}")
