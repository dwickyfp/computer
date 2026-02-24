"""
WAL Monitor model - Real-time WAL replication status tracking.

Stores current WAL state for each source (upsert pattern: 1 source = 1 row).
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo
from app.domain.models.base import Base

if TYPE_CHECKING:
    from app.domain.models.source import Source


class WALMonitor(Base):
    """
    PostgreSQL WAL Monitor - tracks current replication status.

    This table maintains ONE row per source (enforced by unique constraint).
    Each update overwrites the previous state for that source.
    """

    __tablename__ = "wal_monitor"
    __table_args__ = (
        UniqueConstraint("source_id", name="unique_source_wal"),
        {"comment": "Real-time WAL replication status per source"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique monitor record identifier",
    )

    # Source Reference (unique constraint ensures 1 row per source)
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to source being monitored",
    )

    # WAL Position Tracking
    wal_lsn: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Log Sequence Number (e.g., '0/1234ABCD')",
    )

    wal_position: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="WAL position as numeric value",
    )

    # Timing Information
    last_wal_received: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="Last time WAL data was received",
    )

    last_transaction_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="Last transaction timestamp",
    )

    # Replication Status
    replication_slot_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Name of the replication slot",
    )

    replication_lag_bytes: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Replication lag in bytes",
    )

    total_wal_size: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Total size of WAL files",
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="ACTIVE",
        index=True,
        comment="Monitor status: ACTIVE, IDLE, ERROR",
    )

    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error details if any",
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        comment="Record creation timestamp",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        onupdate=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        comment="Last update timestamp",
    )

    # Relationships
    source: Mapped["Source"] = relationship("Source", back_populates="wal_monitor")

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<WALMonitor(id={self.id}, source_id={self.source_id}, "
            f"status={self.status}, lsn={self.wal_lsn})>"
        )
