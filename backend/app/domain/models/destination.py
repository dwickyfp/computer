"""
Destination model - Snowflake data warehouse configurations.

Represents Snowflake connection configurations for ETL destinations.
"""

from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import DateTime

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.pipeline import PipelineDestination



class Destination(Base, TimestampMixin):
    """
    Snowflake destination configuration.

    Stores connection details for Snowflake data warehouses that serve
    as destinations for ETL pipelines.
    """

    __tablename__ = "destinations"
    __table_args__ = (
        UniqueConstraint("name", name="uq_destinations_name"),
        {"comment": "Snowflake destination configurations"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique destination identifier",
    )

    # Destination Identification
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique destination name",
    )

    type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="SNOWFLAKE",
        comment="Destination type (e.g. SNOWFLAKE, KAFKA)",
    )

    # Configuration (JSONB)
    config: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="Destination configuration (JSON)",
    )

    # Table list tracking
    list_tables: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        comment="Cached list of tables in this destination",
    )

    total_tables: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total number of tables in this destination",
    )

    last_table_check_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
        comment="Timestamp of last table list check",
    )

    # Relationships
    pipeline_destinations: Mapped[list["PipelineDestination"]] = relationship(
        "PipelineDestination",
        back_populates="destination",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"Destination(id={self.id}, name={self.name!r}, "
            f"type={self.type!r})"
        )

    @property
    def connection_config(self) -> dict[str, str]:
        """
        Get connection configuration from JSONB config.
        
        Returns dictionary with connection parameters.
        """
        return self.config

    @property
    def is_used_in_active_pipeline(self) -> bool:
        """Check if destination is used in any active pipeline."""
        return any(pd.pipeline.status == "START" for pd in self.pipeline_destinations)
