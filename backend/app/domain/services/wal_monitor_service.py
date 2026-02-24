"""
WAL Monitor service containing business logic.

Implements business rules and orchestrates repository operations for WAL monitoring.
"""

from typing import List, Optional

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.wal_monitor import WALMonitor
from app.domain.repositories.wal_monitor_repo import WALMonitorRepository
from app.domain.schemas.wal_monitor import (
    WALMonitorCreate,
    WALMonitorStatusUpdate,
    WALMonitorUpdate,
)

logger = get_logger(__name__)


class WALMonitorService:
    """
    Service layer for WAL Monitor entity.

    Implements business logic for managing WAL monitor records.
    Ensures 1 source = 1 monitor record using upsert pattern.
    """

    def __init__(self, db: Session):
        """Initialize WAL monitor service."""
        self.db = db
        self.repository = WALMonitorRepository(db)

    def upsert_monitor(self, monitor_data: WALMonitorCreate) -> WALMonitor:
        """
        Create or update WAL monitor record.

        Uses upsert to ensure only one record per source.

        Args:
            monitor_data: WAL monitor creation data

        Returns:
            Created or updated WAL monitor
        """
        logger.info(
            "Upserting WAL monitor",
            extra={"source_id": monitor_data.source_id, "status": monitor_data.status},
        )

        monitor = self.repository.upsert_monitor(
            source_id=monitor_data.source_id,
            wal_lsn=monitor_data.wal_lsn,
            wal_position=monitor_data.wal_position,
            last_wal_received=monitor_data.last_wal_received,
            last_transaction_time=monitor_data.last_transaction_time,
            replication_slot_name=monitor_data.replication_slot_name,
            replication_lag_bytes=monitor_data.replication_lag_bytes,
            status=monitor_data.status,
            error_message=monitor_data.error_message,
        )

        logger.info(
            "WAL monitor upserted successfully",
            extra={"monitor_id": monitor.id, "source_id": monitor_data.source_id},
        )

        return monitor

    def get_monitor_by_source(self, source_id: int) -> Optional[WALMonitor]:
        """
        Get WAL monitor record for a specific source.

        Args:
            source_id: Source identifier

        Returns:
            WAL monitor record or None if not exists
        """
        return self.repository.get_by_source(source_id)

    def get_monitor(self, monitor_id: int) -> WALMonitor:
        """
        Get WAL monitor by ID.

        Args:
            monitor_id: Monitor identifier

        Returns:
            WAL monitor entity
        """
        return self.repository.get_by_id(monitor_id)

    def list_monitors(self) -> List[WALMonitor]:
        """
        List all WAL monitor records.

        Returns:
            List of all WAL monitors
        """
        return self.repository.get_all_monitors()

    def update_monitor_status(
        self, source_id: int, status_data: WALMonitorStatusUpdate
    ) -> Optional[WALMonitor]:
        """
        Update only the status of a WAL monitor.

        Args:
            source_id: Source identifier
            status_data: Status update data

        Returns:
            Updated WAL monitor or None if not exists
        """
        logger.info(
            "Updating WAL monitor status",
            extra={"source_id": source_id, "status": status_data.status},
        )

        monitor = self.repository.update_status(
            source_id=source_id,
            status=status_data.status,
            error_message=status_data.error_message,
        )

        if monitor:
            logger.info(
                "WAL monitor status updated",
                extra={"monitor_id": monitor.id, "source_id": source_id},
            )

        return monitor

    def delete_monitor(self, monitor_id: int) -> None:
        """
        Delete WAL monitor by ID.

        Args:
            monitor_id: Monitor identifier
        """
        logger.info("Deleting WAL monitor", extra={"monitor_id": monitor_id})

        self.repository.delete(monitor_id)

        logger.info("WAL monitor deleted", extra={"monitor_id": monitor_id})

    def count_monitors(self) -> int:
        """
        Count total number of WAL monitors.

        Returns:
            Total count
        """
        return self.repository.count()
