"""
Credit Snowflake Monitoring model.

Stores Snowflake credit usage data for destinations.
"""

from typing import TYPE_CHECKING

from decimal import Decimal
from sqlalchemy import Integer, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.destination import Destination


class CreditSnowflakeMonitoring(Base, TimestampMixin):
    """
    Credit Snowflake Monitoring model.
    
    Stores historical credit usage data retrieved from Snowflake.
    """
    
    __tablename__ = "credit_snowflake_monitoring"
    __table_args__ = (
        {"comment": "Snowflake credit usage monitoring data"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique identifier",
    )

    # Foreign Key to Destination
    destination_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("destinations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Destination identifier",
    )

    # Data
    total_credit: Mapped[Decimal] = mapped_column(
        Numeric(38, 9),
        nullable=False,
        comment="Total credits used",
    )
    
    usage_date: Mapped[DateTime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Date of usage",
    )

    # Relationships
    destination: Mapped["Destination"] = relationship(
        "Destination",
        lazy="joined",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"CreditSnowflakeMonitoring(id={self.id}, "
            f"destination_id={self.destination_id}, "
            f"usage_date={self.usage_date})"
        )
