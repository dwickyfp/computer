"""
History Schema Evolution model.

This module defines the HistorySchemaEvolution model for tracking schema changes.
"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from zoneinfo import ZoneInfo
from app.domain.models.base import Base


class HistorySchemaEvolution(Base):
    """
    HistorySchemaEvolution model.
    """

    __tablename__ = "history_schema_evolution"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    table_metadata_list_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("table_metadata_list.id", ondelete="CASCADE"), nullable=False
    )
    schema_table_old: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    schema_table_new: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    changes_type: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, comment="NEW COLUMN, DROP COLUMN, CHANGES TYPE"
    )
    version_schema: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

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
    table_metadata: Mapped["TableMetadata"] = relationship(
        "TableMetadata", back_populates="history_schema"
    )
