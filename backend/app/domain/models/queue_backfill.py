"""
Queue Backfill Data models - Manages backfill job queue.

Represents backfill jobs for historical data synchronization.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.pipeline import Pipeline
    from app.domain.models.source import Source


class BackfillStatus(str, Enum):
    """Backfill job status."""

    PENDING = "PENDING"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class QueueBackfillData(Base, TimestampMixin):
    """
    Backfill job queue.

    Manages historical data backfill operations with DuckDB.
    """

    __tablename__ = "queue_backfill_data"
    __table_args__ = {"comment": "Queue for backfill data jobs"}

    # Primary Key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign Keys
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Job Configuration
    table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    filter_sql: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Job Status
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=BackfillStatus.PENDING.value, index=True
    )
    count_record: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_record: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    is_error: Mapped[bool] = mapped_column(default=False, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline", back_populates="backfill_jobs"
    )
    source: Mapped["Source"] = relationship("Source", back_populates="backfill_jobs")

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<QueueBackfillData(id={self.id}, pipeline_id={self.pipeline_id}, "
            f"table_name='{self.table_name}', status='{self.status}')>"
        )
