"""
Rosetta Chain models - inter-instance streaming configuration.

Represents chain connections between Rosetta instances for Arrow IPC streaming.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    BigInteger,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin


class RosettaChainConfig(Base, TimestampMixin):
    """
    Rosetta Chain configuration for this instance.

    Stores the chain credential key used to authenticate
    incoming connections from other Rosetta instances.
    """

    __tablename__ = "rosetta_chain_config"
    __table_args__ = {"comment": "Rosetta Chain instance configuration"}

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique config identifier",
    )

    chain_key: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Encrypted chain credential key for this instance",
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether chain ingestion is active on this instance",
    )

    def __repr__(self) -> str:
        return f"RosettaChainConfig(id={self.id}, is_active={self.is_active})"


class RosettaChainClient(Base, TimestampMixin):
    """
    Remote Rosetta instance registration.

    Stores connection details for remote Rosetta instances
    that this instance can push data to or receive data from.
    """

    __tablename__ = "rosetta_chain_clients"
    __table_args__ = (
        UniqueConstraint("name", name="uq_rosetta_chain_clients_name"),
        {"comment": "Remote Rosetta instance registrations"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique client identifier",
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique name for this remote instance",
    )

    url: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="URL/hostname of the remote Rosetta compute service",
    )

    port: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=8001,
        comment="Port of the remote Rosetta compute service",
    )

    chain_key: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Encrypted chain credential key of the remote instance",
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether this client connection is active",
    )

    last_connected_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last successful connection",
    )

    # Relationships
    tables: Mapped[list["RosettaChainTable"]] = relationship(
        "RosettaChainTable",
        back_populates="chain_client",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    
    databases: Mapped[list["RosettaChainDatabase"]] = relationship(
        "RosettaChainDatabase",
        back_populates="chain_client",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"RosettaChainClient(id={self.id}, name={self.name!r}, "
            f"url={self.url!r}, port={self.port})"
        )


class RosettaChainTable(Base, TimestampMixin):
    """
    Virtual table received from a remote Rosetta instance.

    Represents a table that is being streamed from another Rosetta
    instance via Arrow IPC. Data for these tables lives in Redis Streams.
    """

    __tablename__ = "rosetta_chain_tables"
    __table_args__ = (
        UniqueConstraint(
            "chain_client_id",
            "table_name",
            name="uq_rosetta_chain_tables_client_table",
        ),
        {"comment": "Virtual tables from remote Rosetta instances"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique table identifier",
    )

    chain_client_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("rosetta_chain_clients.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Reference to the chain client that owns this table (NULL for cross-instance registrations)",
    )

    table_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Name of the virtual table",
    )

    table_schema: Mapped[dict] = mapped_column(
        "schema_json",
        JSONB,
        nullable=False,
        default=dict,
        comment="Table schema definition (column names, types)",
    )

    source_chain_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Identifier of the originating chain instance",
    )

    record_count: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        comment="Approximate record count in Redis Stream",
    )

    last_synced_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last data sync",
    )

    # Relationships
    chain_client: Mapped["RosettaChainClient"] = relationship(
        "RosettaChainClient",
        back_populates="tables",
    )

    def __repr__(self) -> str:
        return (
            f"RosettaChainTable(id={self.id}, "
            f"table_name={self.table_name!r}, "
            f"chain_client_id={self.chain_client_id})"
        )


class RosettaChainDatabase(Base, TimestampMixin):
    """
    Virtual database received from a remote Rosetta instance.

    Represents a database catalog that is discovered from another Rosetta
    instance.
    """

    __tablename__ = "rosetta_chain_databases"
    __table_args__ = (
        UniqueConstraint(
            "chain_client_id",
            "name",
            name="uq_rosetta_chain_databases_client_name",
        ),
        {"comment": "Virtual databases from remote Rosetta instances"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique database identifier",
    )

    chain_client_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("rosetta_chain_clients.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to the chain client that owns this database",
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Name of the virtual database",
    )

    # Relationships
    chain_client: Mapped["RosettaChainClient"] = relationship(
        "RosettaChainClient",
        back_populates="databases",
    )

    def __repr__(self) -> str:
        return (
            f"RosettaChainDatabase(id={self.id}, "
            f"name={self.name!r}, "
            f"chain_client_id={self.chain_client_id})"
        )
