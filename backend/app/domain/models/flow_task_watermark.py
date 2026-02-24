"""
Flow Task Watermark model — incremental execution state tracking.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.models.base import Base, TimestampMixin


class FlowTaskWatermark(Base, TimestampMixin):
    """Watermark tracking for incremental flow task execution."""

    __tablename__ = "flow_task_watermarks"
    __table_args__ = (
        UniqueConstraint("flow_task_id", "node_id", name="uq_flow_task_watermark"),
        {"comment": "Watermark tracking for incremental flow task execution"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_task_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("flow_tasks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    watermark_column: Mapped[str] = mapped_column(String(255), nullable=False)
    last_watermark_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    watermark_type: Mapped[str] = mapped_column(String(50), default="TIMESTAMP")
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    record_count: Mapped[int] = mapped_column(BigInteger, default=0)
