"""
Linked Task models — Flow Task Orchestration DAG.

Allows chaining multiple flow tasks in sequential and parallel patterns
with configurable dependency conditions.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.flow_task import FlowTask, FlowTaskRunHistory


# ─── Enums ───────────────────────────────────────────────────────────────────


class LinkedTaskStatus(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class LinkedTaskRunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class LinkedTaskStepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class LinkedTaskEdgeCondition(str, Enum):
    ON_SUCCESS = "ON_SUCCESS"   # run target only if source succeeded
    ALWAYS = "ALWAYS"           # run target regardless of source result


# ─── Models ──────────────────────────────────────────────────────────────────


class LinkedTask(Base, TimestampMixin):
    """
    Orchestration DAG that chains multiple flow tasks.

    Each row is one user-defined orchestration flow.
    """

    __tablename__ = "linked_tasks"
    __table_args__ = (
        UniqueConstraint("name", name="uq_linked_tasks_name"),
        {"comment": "Orchestration DAGs that chain multiple flow_tasks"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True, index=True
    )
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=LinkedTaskStatus.IDLE
    )
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_run_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Relationships
    steps: Mapped[list["LinkedTaskStep"]] = relationship(
        "LinkedTaskStep",
        back_populates="linked_task",
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    edges: Mapped[list["LinkedTaskEdge"]] = relationship(
        "LinkedTaskEdge",
        back_populates="linked_task",
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    run_history: Mapped[list["LinkedTaskRunHistory"]] = relationship(
        "LinkedTaskRunHistory",
        back_populates="linked_task",
        lazy="select",
        cascade="all, delete-orphan",
        order_by="LinkedTaskRunHistory.started_at.desc()",
    )


class LinkedTaskStep(Base, TimestampMixin):
    """
    A single node (step) in the linked task DAG.

    Each step references exactly one FlowTask.
    """

    __tablename__ = "linked_task_steps"
    __table_args__ = {
        "comment": "Canvas nodes in a linked_task DAG — each references one flow_task"
    }

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    linked_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    flow_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pos_x: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pos_y: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Relationships
    linked_task: Mapped["LinkedTask"] = relationship(
        "LinkedTask", back_populates="steps", lazy="selectin"
    )
    flow_task: Mapped["FlowTask"] = relationship(
        "FlowTask", lazy="selectin", foreign_keys=[flow_task_id]
    )
    outgoing_edges: Mapped[list["LinkedTaskEdge"]] = relationship(
        "LinkedTaskEdge",
        foreign_keys="LinkedTaskEdge.source_step_id",
        back_populates="source_step",
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    incoming_edges: Mapped[list["LinkedTaskEdge"]] = relationship(
        "LinkedTaskEdge",
        foreign_keys="LinkedTaskEdge.target_step_id",
        back_populates="target_step",
        lazy="selectin",
    )


class LinkedTaskEdge(Base, TimestampMixin):
    """
    A directed dependency edge between two steps in the DAG.

    condition:
        ON_SUCCESS — run target only if source succeeded
        ALWAYS     — run target regardless of source result
    """

    __tablename__ = "linked_task_edges"
    __table_args__ = (
        UniqueConstraint("source_step_id", "target_step_id", name="uq_linked_task_edge"),
        {"comment": "DAG edges: condition controls when target runs after source"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    linked_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_step_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_task_steps.id", ondelete="CASCADE"),
        nullable=False,
    )
    target_step_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_task_steps.id", ondelete="CASCADE"),
        nullable=False,
    )
    condition: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=LinkedTaskEdgeCondition.ON_SUCCESS,
        comment="ON_SUCCESS | ALWAYS",
    )

    # Relationships
    linked_task: Mapped["LinkedTask"] = relationship(
        "LinkedTask", back_populates="edges", lazy="selectin"
    )
    source_step: Mapped["LinkedTaskStep"] = relationship(
        "LinkedTaskStep",
        foreign_keys=[source_step_id],
        back_populates="outgoing_edges",
        lazy="selectin",
    )
    target_step: Mapped["LinkedTaskStep"] = relationship(
        "LinkedTaskStep",
        foreign_keys=[target_step_id],
        back_populates="incoming_edges",
        lazy="selectin",
    )


class LinkedTaskRunHistory(Base, TimestampMixin):
    """
    One row per triggered execution of a linked task.
    """

    __tablename__ = "linked_task_run_history"
    __table_args__ = {"comment": "Execution history for each linked_task run"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    linked_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    trigger_type: Mapped[str] = mapped_column(
        String(20), nullable=False, default="MANUAL"
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=LinkedTaskRunStatus.RUNNING
    )
    celery_task_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    linked_task: Mapped["LinkedTask"] = relationship(
        "LinkedTask", back_populates="run_history", lazy="selectin"
    )
    step_logs: Mapped[list["LinkedTaskRunStepLog"]] = relationship(
        "LinkedTaskRunStepLog",
        back_populates="run_history",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="LinkedTaskRunStepLog.id",
    )


class LinkedTaskRunStepLog(Base, TimestampMixin):
    """
    Per-step execution record within a single linked task run.
    """

    __tablename__ = "linked_task_run_step_log"
    __table_args__ = {"comment": "Per-step logs within a linked_task run"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_history_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_task_run_history.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    linked_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    step_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("linked_task_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    flow_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    flow_task_run_history_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("flow_task_run_history.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=LinkedTaskStepStatus.PENDING
    )
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    run_history: Mapped["LinkedTaskRunHistory"] = relationship(
        "LinkedTaskRunHistory", back_populates="step_logs", lazy="selectin"
    )
    step: Mapped["LinkedTaskStep"] = relationship(
        "LinkedTaskStep", lazy="selectin", foreign_keys=[step_id]
    )
