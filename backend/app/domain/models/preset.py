"""
Preset model.
"""

from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import ARRAY

from app.domain.models.base import Base, TimestampMixin

class Preset(Base, TimestampMixin):
    """
    Preset entity.
    Stores saved lists of tables for a source.
    """
    __tablename__ = "presets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_names: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)

    # Relationships
    # source relationship will be defined in Source model or here if needed, 
    # currently Source model doesn't have it explicitly defined as back_populates yet.
    # source: Mapped["Source"] = relationship("Source", back_populates="presets")
