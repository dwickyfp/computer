"""
WAL Metric model - PostgreSQL Write-Ahead Log size tracking.

Stores historical WAL size data for monitoring and analysis.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo
from app.domain.models.base import Base

if TYPE_CHECKING:
    from app.domain.models.source import Source


class WALMetric(Base):
    """
    PostgreSQL WAL (Write-Ahead Log) size metric.

    Tracks WAL size over time for monitoring disk usage and
    detecting replication lag or WAL accumulation issues.
    """

    __tablename__ = "wal_metrics"
    __table_args__ = {"comment": "PostgreSQL WAL size metrics for monitoring"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique metric identifier",
    )

    # Source Reference
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to source being monitored",
    )

    # WAL Metrics
    size_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="WAL size in bytes"
    )

    # Timestamp
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
        comment="When this metric was recorded",
    )

    # Relationships
    source: Mapped["Source"] = relationship("Source", back_populates="wal_metrics")

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"WALMetric(id={self.id}, source_id={self.source_id}, "
            f"size_bytes={self.size_bytes}, recorded_at={self.recorded_at})"
        )

    @property
    def size_mb(self) -> float:
        """Get WAL size in megabytes."""
        return self.size_bytes / (1024 * 1024)

    @property
    def size_gb(self) -> float:
        """Get WAL size in gigabytes."""
        return self.size_bytes / (1024 * 1024 * 1024)

    @classmethod
    def from_bytes(
        cls, source_id: int, size_bytes: int, recorded_at: datetime | None = None
    ) -> "WALMetric":
        """
        Create WAL metric from byte count.

        Args:
            source_id: ID of the source being monitored
            size_bytes: WAL size in bytes
            recorded_at: Optional timestamp (defaults to now)

        Returns:
            New WALMetric instance
        """
        return cls(
            source_id=source_id,
            size_bytes=size_bytes,
            recorded_at=recorded_at or datetime.now(ZoneInfo('Asia/Jakarta'))(),
        )
