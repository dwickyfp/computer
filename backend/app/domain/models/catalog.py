"""
Catalog models - represent the Database and Table definitions
for the Managed Data Lake / Warehouse architecture.
"""

from typing import Optional
from datetime import datetime

from sqlalchemy import (
    String,
    Integer,
    ForeignKey,
    UniqueConstraint,
    DateTime,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin


class CatalogDatabase(Base, TimestampMixin):
    """
    Logical database container in the Rosetta Catalog.
    """

    __tablename__ = "catalog_databases"
    __table_args__ = {"comment": "Logical database containers in the catalog"}

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique database identifier",
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Name of the database",
    )

    description: Mapped[Optional[str]] = mapped_column(
        String(1000),
        nullable=True,
        comment="Optional description of the database",
    )

    # Relationships
    tables: Mapped[list["CatalogTable"]] = relationship(
        "CatalogTable",
        back_populates="database",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"CatalogDatabase(id={self.id}, name={self.name!r})"


class CatalogTable(Base, TimestampMixin):
    """
    Table definition in the Rosetta Catalog.
    Underlying data resides in a Redis Stream.
    """

    __tablename__ = "catalog_tables"
    __table_args__ = (
        UniqueConstraint("database_id", "table_name", name="uq_catalog_tables_db_name"),
        {"comment": "Table definitions in the catalog"},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique table identifier",
    )

    database_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("catalog_databases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to the parent database",
    )

    table_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Name of the table",
    )

    schema_json: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="Table schema definition (fields, types, constraints)",
    )

    stream_name: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="Underlying Redis Stream key storing this table's data",
    )

    source_chain_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Identifier of the originating chain instance (if registered remotely)",
    )

    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="UNKNOWN",
        comment="Health status of the underlying stream (e.g., ACTIVE, INACTIVE, UNKNOWN)",
    )

    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last stream health check",
    )

    # Relationships
    database: Mapped["CatalogDatabase"] = relationship(
        "CatalogDatabase",
        back_populates="tables",
    )

    def __repr__(self) -> str:
        return (
            f"CatalogTable(id={self.id}, database_id={self.database_id}, "
            f"table_name={self.table_name!r})"
        )
