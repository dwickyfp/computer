"""
WAL Metric repository for data access operations.

Extends base repository with WAL metric-specific queries.
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.models.wal_metric import WALMetric
from app.domain.repositories.base import BaseRepository


class WALMetricRepository(BaseRepository[WALMetric]):
    """
    Repository for WALMetric entity.

    Provides data access methods for WAL size metrics.
    """

    def __init__(self, db: Session):
        """Initialize WAL metric repository."""
        super().__init__(WALMetric, db)

    def get_by_source(self, source_id: int, limit: int = 100) -> List[WALMetric]:
        """
        Get WAL metrics for a specific source.

        Args:
            source_id: Source identifier
            limit: Maximum number of metrics to return

        Returns:
            List of WAL metrics ordered by timestamp (newest first)
        """
        result = self.db.execute(
            select(WALMetric)
            .where(WALMetric.source_id == source_id)
            .order_by(desc(WALMetric.recorded_at))
            .limit(limit)
        )
        return list(result.scalars().all())

    def get_by_time_range(
        self,
        source_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[WALMetric]:
        """
        Get WAL metrics within a time range.

        Args:
            source_id: Optional source identifier to filter by
            start_date: Start of time range
            end_date: End of time range
            limit: Maximum number of metrics to return

        Returns:
            List of WAL metrics ordered by timestamp (newest first)
        """
        query = select(WALMetric)

        if source_id is not None:
            query = query.where(WALMetric.source_id == source_id)

        if start_date is not None:
            query = query.where(WALMetric.recorded_at >= start_date)

        if end_date is not None:
            query = query.where(WALMetric.recorded_at <= end_date)

        query = query.order_by(desc(WALMetric.recorded_at)).limit(limit)

        result = self.db.execute(query)
        return list(result.scalars().all())

    def get_latest_by_source(self, source_id: int) -> Optional[WALMetric]:
        """
        Get the latest WAL metric for a source.

        Args:
            source_id: Source identifier

        Returns:
            Latest WAL metric or None if no metrics exist
        """
        result = self.db.execute(
            select(WALMetric)
            .where(WALMetric.source_id == source_id)
            .order_by(desc(WALMetric.recorded_at))
            .limit(1)
        )
        return result.scalar_one_or_none()

    def record_metric(
        self, source_id: int, size_bytes: int, recorded_at: Optional[datetime] = None
    ) -> WALMetric:
        """
        Record a new WAL metric.

        Args:
            source_id: Source identifier
            size_bytes: WAL size in bytes
            recorded_at: Optional timestamp (defaults to now)

        Returns:
            Created WAL metric
        """
        metric = WALMetric.from_bytes(
            source_id=source_id, size_bytes=size_bytes, recorded_at=recorded_at
        )
        self.db.add(metric)
        self.db.flush()
        self.db.refresh(metric)
        return metric
