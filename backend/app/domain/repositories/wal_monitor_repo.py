"""
WAL Monitor repository with upsert support.

Implements upsert pattern to maintain 1 row per source.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, joinedload

from app.domain.models.wal_monitor import WALMonitor
from app.domain.repositories.base import BaseRepository
from app.core.logging import get_logger

logger = get_logger(__name__)


class WALMonitorRepository(BaseRepository[WALMonitor]):
    """
    Repository for WALMonitor entity.

    Provides data access with upsert capability for maintaining
    single row per source.
    """

    def __init__(self, db: Session):
        """Initialize WAL monitor repository."""
        super().__init__(WALMonitor, db)
    def get_by_source(self, source_id: int) -> Optional[WALMonitor]:
        """
        Get WAL monitor record for a specific source.

        Args:
            source_id: Source identifier

        Returns:
            WAL monitor record or None if not exists
        """
        result = self.db.execute(
            select(WALMonitor)
            .options(joinedload(WALMonitor.source))
            .where(WALMonitor.source_id == source_id)
        )
        return result.scalar_one_or_none()

    def get_all_monitors(self) -> List[WALMonitor]:
        """
        Get all WAL monitor records.

        Returns:
            List of all WAL monitor records
        """
        result = self.db.execute(
            select(WALMonitor).options(joinedload(WALMonitor.source))
        )
        return list(result.scalars().all())

    def upsert_monitor(
        self,
        source_id: int,
        wal_lsn: Optional[str] = None,
        wal_position: Optional[int] = None,
        last_wal_received: Optional[datetime] = None,
        last_transaction_time: Optional[datetime] = None,
        replication_slot_name: Optional[str] = None,
        replication_lag_bytes: Optional[int] = None,
        total_wal_size: Optional[str] = None,
        status: str = "ACTIVE",
        error_message: Optional[str] = None,
    ) -> WALMonitor:
        """
        Insert or update WAL monitor record.

        Uses PostgreSQL's INSERT ... ON CONFLICT ... DO UPDATE to ensure
        only one row exists per source_id.

        Args:
            source_id: Source identifier
            wal_lsn: Log Sequence Number
            wal_position: WAL position as numeric
            last_wal_received: Last WAL receive timestamp
            last_transaction_time: Last transaction timestamp
            replication_slot_name: Replication slot name
            replication_lag_bytes: Replication lag in bytes
            total_wal_size: Total WAL size
            status: Monitor status (ACTIVE, IDLE, ERROR)
            error_message: Error details if any

        Returns:
            Created or updated WAL monitor record
        """
        now = datetime.now(ZoneInfo('Asia/Jakarta'))

        # Build the insert statement with ON CONFLICT
        stmt = insert(WALMonitor).values(
            source_id=source_id,
            wal_lsn=wal_lsn,
            wal_position=wal_position,
            last_wal_received=last_wal_received or now,
            last_transaction_time=last_transaction_time,
            replication_slot_name=replication_slot_name,
            replication_lag_bytes=replication_lag_bytes,
            total_wal_size=total_wal_size,
            status=status,
            error_message=error_message,
            created_at=now,
            updated_at=now,
        )

        # On conflict, update all fields except id and source_id
        stmt = stmt.on_conflict_do_update(
            constraint="unique_source_wal",
            set_={
                "wal_lsn": stmt.excluded.wal_lsn,
                "wal_position": stmt.excluded.wal_position,
                "last_wal_received": stmt.excluded.last_wal_received,
                "last_transaction_time": stmt.excluded.last_transaction_time,
                "replication_slot_name": stmt.excluded.replication_slot_name,
                "replication_lag_bytes": stmt.excluded.replication_lag_bytes,
                "total_wal_size": stmt.excluded.total_wal_size,
                "status": stmt.excluded.status,
                "error_message": stmt.excluded.error_message,
                "updated_at": now,
            },
        ).returning(WALMonitor)

        # Execute and return the result
        result = self.db.execute(stmt)
        monitor = result.scalar_one()

        self.db.flush()
        self.db.refresh(monitor)

        logger.info(
            "WAL monitor upserted",
            extra={
                "source_id": source_id,
                "monitor_id": monitor.id,
                "status": status,
            },
        )

        return monitor

    def update_status(
        self,
        source_id: int,
        status: str,
        error_message: Optional[str] = None,
    ) -> Optional[WALMonitor]:
        """
        Update only the status of a WAL monitor record.

        Args:
            source_id: Source identifier
            status: New status (ACTIVE, IDLE, ERROR)
            error_message: Error message if status is ERROR

        Returns:
            Updated WAL monitor record or None if not exists
        """
        monitor = self.get_by_source(source_id)
        if not monitor:
            return None

        monitor.status = status
        monitor.error_message = error_message
        monitor.updated_at = datetime.now(ZoneInfo('Asia/Jakarta'))

        self.db.flush()
        self.db.refresh(monitor)

        logger.info(
            "WAL monitor status updated",
            extra={
                "source_id": source_id,
                "status": status,
            },
        )

        return monitor

    def delete_by_source(self, source_id: int) -> bool:
        """
        Delete WAL monitor record for a source.

        Args:
            source_id: Source identifier

        Returns:
            True if deleted, False if not found
        """
        monitor = self.get_by_source(source_id)
        if not monitor:
            return False

        self.db.delete(monitor)
        self.db.flush()

        logger.info("WAL monitor deleted", extra={"source_id": source_id})

        return True
