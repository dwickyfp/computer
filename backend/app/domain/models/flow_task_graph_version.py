"""
Flow Task Graph Version model — versioned snapshots for rollback.
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from zoneinfo import ZoneInfo

from app.domain.models.base import Base


class FlowTaskGraphVersion(Base):
    """Versioned snapshot of a flow task graph for rollback support."""

    __tablename__ = "flow_task_graph_version"
    __table_args__ = (
        UniqueConstraint("flow_task_id", "version", name="uq_flow_task_graph_version"),
        {"comment": "Versioned graph snapshots for flow task rollback"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_task_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("flow_tasks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    nodes_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=list)
    edges_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=list)
    change_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
        nullable=False,
    )
