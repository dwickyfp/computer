"""
Pipeline models - ETL pipeline configurations and metadata.

Represents the mapping between sources and destinations with runtime metadata.
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import (
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

if TYPE_CHECKING:
    from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
    from app.domain.models.destination import Destination
    from app.domain.models.queue_backfill import QueueBackfillData
    from app.domain.models.source import Source
    from app.domain.models.tag import PipelineDestinationTableSyncTag


class PipelineStatus(str, Enum):
    """Pipeline operational status."""

    START = "START"
    PAUSE = "PAUSE"
    REFRESH = "REFRESH"


class PipelineMetadataStatus(str, Enum):
    """Pipeline runtime status."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class Pipeline(Base, TimestampMixin):
    """
    ETL Pipeline configuration.

    Connects a data source to a destination and manages the ETL flow.
    """

    __tablename__ = "pipelines"
    __table_args__ = (
        UniqueConstraint("name", name="uq_pipelines_name"),
        {"comment": "ETL pipeline configurations"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique pipeline identifier",
    )

    # Pipeline Identification
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique pipeline name",
    )

    # Source Reference
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to source configuration",
    )

    # Pipeline Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=PipelineStatus.START.value,
        index=True,
        comment="Pipeline operational status: START, PAUSE, or REFRESH",
    )

    ready_refresh: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
        comment="Flag indicating pipeline needs refresh due to configuration changes",
    )

    last_refresh_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last pipeline refresh",
    )

    # Relationships
    source: Mapped["Source"] = relationship(
        "Source", back_populates="pipelines", lazy="selectin"
    )

    destinations: Mapped[list["PipelineDestination"]] = relationship(
        "PipelineDestination",
        back_populates="pipeline",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    pipeline_metadata: Mapped["PipelineMetadata"] = relationship(
        "PipelineMetadata",
        back_populates="pipeline",
        uselist=False,
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    data_flow_records: Mapped[list["DataFlowRecordMonitoring"]] = relationship(
        "DataFlowRecordMonitoring",
        back_populates="pipeline",
        cascade="all, delete-orphan",
    )

    backfill_jobs: Mapped[list["QueueBackfillData"]] = relationship(
        "QueueBackfillData",
        back_populates="pipeline",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        """String representation."""
        return f"Pipeline(id={self.id}, name={self.name!r}, " f"status={self.status!r})"

    @property
    def is_active(self) -> bool:
        """Check if pipeline is in START status."""
        return self.status == PipelineStatus.START.value

    @property
    def is_paused(self) -> bool:
        """Check if pipeline is in PAUSE status."""
        return self.status == PipelineStatus.PAUSE.value

    def start(self) -> None:
        """Set pipeline status to START."""
        self.status = PipelineStatus.START.value

    def pause(self) -> None:
        """Set pipeline status to PAUSE."""
        self.status = PipelineStatus.PAUSE.value

    def refresh(self) -> None:
        """Set pipeline status to REFRESH."""
        self.status = PipelineStatus.REFRESH.value


class PipelineDestination(Base, TimestampMixin):
    """
    Pipeline Destination configuration.

    Connects a pipeline to a destination.
    """

    __tablename__ = "pipelines_destination"
    __table_args__ = {"comment": "Pipeline destination configurations"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique pipeline destination identifier",
    )

    # Pipeline Reference
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to pipeline",
    )

    # Destination Reference
    destination_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("destinations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to destination",
    )

    # Error Tracking
    is_error: Mapped[bool] = mapped_column(
        default=False,
        nullable=False,
        comment="Whether destination is in error state",
    )

    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if in error state",
    )

    last_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last error",
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline", back_populates="destinations"
    )

    destination: Mapped["Destination"] = relationship(
        "Destination", back_populates="pipeline_destinations", lazy="selectin"
    )

    table_syncs: Mapped[list["PipelineDestinationTableSync"]] = relationship(
        "PipelineDestinationTableSync",
        back_populates="pipeline_destination",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PipelineDestination(id={self.id}, pipeline_id={self.pipeline_id}, "
            f"destination_id={self.destination_id})"
        )


class PipelineDestinationTableSync(Base, TimestampMixin):
    """
    Pipeline Destination Table Sync configuration.

    Specific settings for table synchronization.
    """

    __tablename__ = "pipelines_destination_table_sync"
    __table_args__ = {"comment": "Table synchronization settings"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique table sync identifier",
    )

    # Pipeline Destination Reference
    pipeline_destination_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines_destination.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to pipeline destination",
    )

    # Table Configuration
    table_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Name of the source table",
    )

    table_name_target: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Name of the target table (defaults to table_name)",
    )

    custom_sql: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Custom SQL for transformation",
    )

    filter_sql: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="SQL filter condition",
    )

    primary_key_column_target: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Custom primary key columns for PostgreSQL merge (semicolon-separated: key1;key2)",
    )

    # Snowflake Status Flags
    is_exists_table_landing: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Table exists in landing (Snowflake)"
    )
    is_exists_stream: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Stream exists in Snowflake"
    )
    is_exists_task: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Task exists in Snowflake"
    )
    is_exists_table_destination: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Table exists in destination (Snowflake)"
    )

    # Error Log
    is_error: Mapped[bool] = mapped_column(
        default=False,
        nullable=False,
        comment="Whether table sync is in error state",
    )

    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if in error state",
    )

    # Lineage Tracking
    lineage_metadata: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Column-level lineage metadata from SQL parsing",
    )

    lineage_status: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        default="PENDING",
        comment="Lineage generation status: PENDING, GENERATING, COMPLETED, FAILED",
    )

    lineage_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if lineage generation failed",
    )

    lineage_generated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when lineage was last generated",
    )

    # Relationships
    pipeline_destination: Mapped["PipelineDestination"] = relationship(
        "PipelineDestination", back_populates="table_syncs"
    )

    tag_associations: Mapped[list["PipelineDestinationTableSyncTag"]] = relationship(
        "PipelineDestinationTableSyncTag",
        back_populates="table_sync",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PipelineDestinationTableSync(id={self.id}, "
            f"table_name={self.table_name!r})"
        )


class PipelineMetadata(Base, TimestampMixin):
    """
    Pipeline runtime metadata.

    Tracks the runtime state and error information for pipelines.
    """

    __tablename__ = "pipeline_metadata"
    __table_args__ = {"comment": "Pipeline runtime metadata and error tracking"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique metadata identifier",
    )

    # Pipeline Reference
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
        comment="Reference to pipeline",
    )

    # Runtime Status
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=PipelineMetadataStatus.RUNNING.value,
        index=True,
        comment="Runtime status: RUNNING, PAUSED, or ERROR",
    )

    # Error Tracking
    last_error: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Last error message"
    )

    last_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Timestamp of last error"
    )

    # Execution Tracking
    last_start_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last pipeline start",
    )

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship(
        "Pipeline", back_populates="pipeline_metadata"
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PipelineMetadata(id={self.id}, pipeline_id={self.pipeline_id}, "
            f"status={self.status!r})"
        )

    def set_running(self) -> None:
        """Set status to RUNNING."""
        self.status = PipelineMetadataStatus.RUNNING.value
        self.last_start_at = datetime.now(ZoneInfo("Asia/Jakarta"))

    def set_paused(self) -> None:
        """Set status to PAUSED."""
        self.status = PipelineMetadataStatus.PAUSED.value

    def set_error(self, error_message: str) -> None:
        """
        Set status to ERROR and record error details.

        Args:
            error_message: Description of the error
        """
        self.status = PipelineMetadataStatus.ERROR.value
        self.last_error = error_message
        self.last_error_at = datetime.now(ZoneInfo("Asia/Jakarta"))

    def clear_error(self) -> None:
        """Clear error state and set to RUNNING."""
        self.status = PipelineMetadataStatus.RUNNING.value
        self.last_error = None
        self.last_error_at = None


class PipelineProgress(Base, TimestampMixin):
    """
    Pipeline initialization progress tracking.
    """

    __tablename__ = "pipelines_progress"
    __table_args__ = {"comment": "Pipeline initialization progress tracking"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique progress identifier",
    )

    # Pipeline Reference
    pipeline_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to pipeline",
    )

    # Progress Details
    progress: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        comment="Progress percentage (0-100)",
    )

    step: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Current step description",
    )

    status: Mapped[str] = mapped_column(
        String(20),
        default="PENDING",
        nullable=False,
        comment="Status: PENDING, IN_PROGRESS, COMPLETED, FAILED",
    )

    details: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Additional details about progress",
    )
