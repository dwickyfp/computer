"""
Source model.

Represents typed source configurations. PostgreSQL source rows continue to
store denormalized connection fields for existing operational paths, while the
public API contract uses ``type`` + ``config``.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.pipeline import Pipeline
    from app.domain.models.wal_metric import WALMetric
    from app.domain.models.wal_monitor import WALMonitor
    from app.domain.models.wal_monitor import WALMonitor
    from app.domain.models.table_metadata import TableMetadata
    from app.domain.models.preset import Preset


class Source(Base, TimestampMixin):
    """
    Typed source configuration.

    PostgreSQL sources retain legacy columns so existing runtime logic can
    continue to operate during the refactor. Kafka sources use ``config``.
    """

    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("name", name="uq_sources_name"),
        {"comment": "Typed source configurations"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique source identifier",
    )

    # Source Identification
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique source name",
    )

    type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="POSTGRES",
        server_default="POSTGRES",
        comment="Source type (POSTGRES, KAFKA)",
    )

    config: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="Typed source configuration (JSON)",
    )

    # PostgreSQL Connection Details (legacy storage used by the runtime)
    pg_host: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="PostgreSQL host address"
    )

    pg_port: Mapped[int | None] = mapped_column(
        Integer, nullable=True, default=5432, comment="PostgreSQL port number"
    )

    pg_database: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="PostgreSQL database name"
    )

    pg_username: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="PostgreSQL username"
    )

    pg_password: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="PostgreSQL password (encrypted)"
    )

    # Replication Configuration
    publication_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="PostgreSQL publication name for CDC"
    )

    replication_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Replication slot name"
    )

    is_publication_enabled: Mapped[bool] = mapped_column(
        default=False, nullable=False, comment="Whether publication is enabled"
    )

    is_replication_enabled: Mapped[bool] = mapped_column(
        default=False, nullable=False, comment="Whether replication is enabled"
    )

    last_check_replication_publication: Mapped[datetime | None] = mapped_column(
        nullable=True, comment="Last timestamp of replication/publication check"
    )

    total_tables: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="Total tables in publication"
    )

    # Relationships
    pipelines: Mapped[list["Pipeline"]] = relationship(
        "Pipeline",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    wal_metrics: Mapped[list["WALMetric"]] = relationship(
        "WALMetric",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="select",
    )

    wal_monitor: Mapped["WALMonitor"] = relationship(
        "WALMonitor",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="select",
        uselist=False,  # One-to-one relationship
    )

    tables: Mapped[list["TableMetadata"]] = relationship(
        "TableMetadata",
        back_populates="source",
        cascade="all, delete-orphan",
        lazy="select",
    )

    data_flow_records: Mapped[list["DataFlowRecordMonitoring"]] = relationship(
        "DataFlowRecordMonitoring",
        back_populates="source",
        cascade="all, delete-orphan",
    )

    presets: Mapped[list["Preset"]] = relationship(
        "Preset",
        # back_populates="source", # Enable if added to Preset
        cascade="all, delete-orphan",
        lazy="select",
    )

    backfill_jobs: Mapped[list["QueueBackfillData"]] = relationship(
        "QueueBackfillData",
        back_populates="source",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"Source(id={self.id}, name={self.name!r}, "
            f"type={self.type!r})"
        )

    @property
    def connection_string(self) -> str:
        """
        Generate PostgreSQL connection string.

        Returns asyncpg-compatible connection string.
        Note: In production, use secrets management for passwords.
        """
        if self.type != "POSTGRES":
            raise ValueError("Connection string is only available for POSTGRES sources")
        password_part = f":{self.pg_password}" if self.pg_password else ""
        return (
            f"postgresql://{self.pg_username}{password_part}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    @property
    def async_connection_string(self) -> str:
        """
        Generate async PostgreSQL connection string.

        Returns asyncpg driver connection string.
        """
        if self.type != "POSTGRES":
            raise ValueError(
                "Async connection string is only available for POSTGRES sources"
            )
        password_part = f":{self.pg_password}" if self.pg_password else ""
        return (
            f"postgresql+asyncpg://{self.pg_username}{password_part}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )
