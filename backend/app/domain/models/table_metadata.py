"""
Table Metadata model.

This module defines the TableMetadata model representing the list of tables being monitored.
"""

from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo
from app.domain.models.base import Base


class TableMetadata(Base):
    """
    TableMetadata model.
    """

    __tablename__ = "table_metadata_list"
    __table_args__ = (
        UniqueConstraint("source_id", "table_name", name="uq_table_metadata_source_table"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False
    )
    table_name: Mapped[str] = mapped_column(String(255), nullable=True)
    schema_table: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    
    is_changes_schema: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Whether schema changes were detected"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')), 
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')), 
        onupdate=lambda: datetime.now(ZoneInfo('Asia/Jakarta')), 
        nullable=False
    )

    # Relationships
    source: Mapped["Source"] = relationship("Source", back_populates="tables")
    history_schema: Mapped[List["HistorySchemaEvolution"]] = relationship(
        "HistorySchemaEvolution", back_populates="table_metadata", cascade="all, delete-orphan"
    )
