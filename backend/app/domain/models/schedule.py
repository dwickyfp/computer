"""
Schedule models — cron-based job scheduling.

Each schedule triggers a flow_task or linked_task on a cron expression.
APScheduler loads ACTIVE schedules at startup and keeps them in sync
with DB state via the DynamicSchedulerService.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo

from app.domain.models.base import Base, TimestampMixin


class ScheduleTaskType(str, Enum):
    """Which kind of task a schedule targets."""

    FLOW_TASK = "FLOW_TASK"
    LINKED_TASK = "LINKED_TASK"


class ScheduleStatus(str, Enum):
    """Operational status of a schedule."""

    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"


class ScheduleRunStatus(str, Enum):
    """Per-run execution status."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Schedule(Base, TimestampMixin):
    """
    Cron-based job schedule definition.

    Each row represents one user-configured schedule that fires a
    flow_task or linked_task according to a 5-part cron expression.
    """

    __tablename__ = "schedules"
    __table_args__ = (
        UniqueConstraint("name", name="uq_schedules_name"),
        {"comment": "Cron-based job schedules — triggers flow_task or linked_task"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )
    task_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="FLOW_TASK | LINKED_TASK",
    )
    task_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="FK to flow_tasks.id or linked_tasks.id based on task_type",
    )
    cron_expression: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Standard 5-part crontab, e.g. '*/5 * * * *'",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=ScheduleStatus.ACTIVE,
        comment="ACTIVE | PAUSED",
    )
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    next_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    run_history: Mapped[List["ScheduleRunHistory"]] = relationship(
        "ScheduleRunHistory",
        back_populates="schedule",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="ScheduleRunHistory.triggered_at.desc()",
    )


class ScheduleRunHistory(Base, TimestampMixin):
    """
    Execution run history for a cron schedule.

    One row per triggered run — cascade-deleted when parent schedule is deleted.
    """

    __tablename__ = "schedule_run_history"
    __table_args__ = {"comment": "Execution history for scheduled jobs"}

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    schedule_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("schedules.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    task_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )
    task_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=ScheduleRunStatus.RUNNING,
        comment="RUNNING | SUCCESS | FAILED",
    )
    message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    # Relationship
    schedule: Mapped["Schedule"] = relationship(
        "Schedule",
        back_populates="run_history",
    )
