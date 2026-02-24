"""
Job Metric model.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.models.base import Base


class JobMetric(Base):
    """
    Job Metric model.

    Tracks the last execution time of background jobs.
    """
    __tablename__ = "job_metrics_monitoring"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    key_job_scheduler: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    last_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')), 
        onupdate=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False
    )
