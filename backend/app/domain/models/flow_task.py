"""
Flow Task models — DuckDB Visual ETL Transform Engine.

Represents user-built visual ETL graphs and their execution history.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo

from app.domain.models.base import Base, TimestampMixin


class FlowTaskStatus(str, Enum):
    """Flow task operational status."""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class FlowTaskTriggerType(str, Enum):
    """How the flow task was/will be triggered."""

    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"


class FlowTaskRunStatus(str, Enum):
    """Per-run execution status."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FlowTaskNodeStatus(str, Enum):
    """Per-node execution status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class FlowTask(Base, TimestampMixin):
    """
    Visual ETL flow task definition.

    Each row represents one user-built transform graph.
    """

    __tablename__ = "flow_tasks"
    __table_args__ = (
        UniqueConstraint("name", name="uq_flow_tasks_name"),
        {"comment": "Visual ETL flow task definitions"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique flow task identifier",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique flow task name",
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional description of the flow task",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=FlowTaskStatus.IDLE,
        comment="Current status: IDLE, RUNNING, SUCCESS, FAILED",
    )
    trigger_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=FlowTaskTriggerType.MANUAL,
        comment="Default trigger type: MANUAL or SCHEDULED",
    )
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last execution",
    )
    last_run_status: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
        comment="Status of the last run: SUCCESS or FAILED",
    )
    last_run_record_count: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Total output records from the last successful run",
    )

    # Relationships
    graph: Mapped[Optional["FlowTaskGraph"]] = relationship(
        "FlowTaskGraph",
        back_populates="flow_task",
        uselist=False,
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    run_history: Mapped[list["FlowTaskRunHistory"]] = relationship(
        "FlowTaskRunHistory",
        back_populates="flow_task",
        lazy="select",
        cascade="all, delete-orphan",
        order_by="FlowTaskRunHistory.started_at.desc()",
    )


class FlowTaskGraph(Base, TimestampMixin):
    """
    Persisted ReactFlow node/edge graph for a flow task.

    One-to-one with FlowTask. Stores node positions and configuration.
    """

    __tablename__ = "flow_task_graph"
    __table_args__ = (
        UniqueConstraint("flow_task_id", name="uq_flow_task_graph_flow_task_id"),
        {"comment": "Persisted ReactFlow graph — nodes with coordinates and edges"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
        comment="Parent flow task",
    )
    nodes_json: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        comment="ReactFlow nodes: [{id, type, position:{x,y}, data:{...}}]",
    )
    edges_json: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        comment="ReactFlow edges: [{id, source, target, sourceHandle, targetHandle}]",
    )
    version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        comment="Increments on each save",
    )

    # Relationships
    flow_task: Mapped["FlowTask"] = relationship(
        "FlowTask",
        back_populates="graph",
        lazy="selectin",
    )


class FlowTaskRunHistory(Base, TimestampMixin):
    """
    One row per triggered execution of a flow task.
    """

    __tablename__ = "flow_task_run_history"
    __table_args__ = {"comment": "Execution history for flow tasks"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Parent flow task",
    )
    trigger_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=FlowTaskTriggerType.MANUAL,
        comment="MANUAL or SCHEDULED",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=FlowTaskRunStatus.RUNNING,
        comment="RUNNING, SUCCESS, FAILED, CANCELLED",
    )
    celery_task_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Celery async task ID for status polling",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
        comment="When execution started",
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When execution finished (success or failure)",
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if run failed",
    )
    total_input_records: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        default=0,
        comment="Total records read from all Input nodes",
    )
    total_output_records: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        default=0,
        comment="Total records written to all Output nodes",
    )
    run_metadata: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Arbitrary per-run context (e.g., graph snapshot at run time)",
    )

    # Relationships
    flow_task: Mapped["FlowTask"] = relationship(
        "FlowTask",
        back_populates="run_history",
        lazy="selectin",
    )
    node_logs: Mapped[list["FlowTaskRunNodeLog"]] = relationship(
        "FlowTaskRunNodeLog",
        back_populates="run_history",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="FlowTaskRunNodeLog.id",
    )


class FlowTaskRunNodeLog(Base, TimestampMixin):
    """
    Per-node execution stats within a single flow run.

    Captures row counts and timing for each node processed.
    """

    __tablename__ = "flow_task_run_node_log"
    __table_args__ = {"comment": "Per-node execution stats for each flow run"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_history_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_task_run_history.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Parent run history record",
    )
    flow_task_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("flow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Parent flow task (denormalized for easier querying)",
    )
    node_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="ReactFlow node id",
    )
    node_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Node type: input, clean, aggregate, join, union, pivot, new_rows, output",
    )
    node_label: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Human-readable node label",
    )
    row_count_in: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        default=0,
        comment="Rows entering this node",
    )
    row_count_out: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        default=0,
        comment="Rows emitted by this node",
    )
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Node execution time in milliseconds",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=FlowTaskNodeStatus.PENDING,
        comment="PENDING, RUNNING, SUCCESS, FAILED, SKIPPED",
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if node failed",
    )

    # Relationships
    run_history: Mapped["FlowTaskRunHistory"] = relationship(
        "FlowTaskRunHistory",
        back_populates="node_logs",
        lazy="selectin",
    )
