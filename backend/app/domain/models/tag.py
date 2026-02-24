"""
Tag models for smart grouping feature.

Represents tags and their associations with pipeline destination table syncs.
"""

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.pipeline import PipelineDestinationTableSync


class TagList(Base, TimestampMixin):
    """
    Tag list for smart grouping.

    Stores unique tag names that can be assigned to pipeline destination table syncs.
    """

    __tablename__ = "tbltag_list"
    __table_args__ = {"comment": "Tag list for smart grouping"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique tag identifier",
    )

    # Tag Name
    tag: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique tag name",
    )

    # Relationships
    tag_associations: Mapped[list["PipelineDestinationTableSyncTag"]] = relationship(
        "PipelineDestinationTableSyncTag",
        back_populates="tag_item",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        """String representation."""
        return f"TagList(id={self.id}, tag={self.tag!r})"


class PipelineDestinationTableSyncTag(Base, TimestampMixin):
    """
    Association between pipeline destination table syncs and tags.

    Many-to-many relationship table.
    """

    __tablename__ = "pipelines_destination_table_sync_tag"
    __table_args__ = {"comment": "Association between table syncs and tags"}

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique association identifier",
    )

    # Foreign Keys
    pipelines_destination_table_sync_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pipelines_destination_table_sync.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to pipeline destination table sync",
    )

    tag_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tbltag_list.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to tag",
    )

    # Relationships
    table_sync: Mapped["PipelineDestinationTableSync"] = relationship(
        "PipelineDestinationTableSync",
        back_populates="tag_associations",
    )

    tag_item: Mapped["TagList"] = relationship(
        "TagList",
        back_populates="tag_associations",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PipelineDestinationTableSyncTag(id={self.id}, "
            f"sync_id={self.pipelines_destination_table_sync_id}, "
            f"tag_id={self.tag_id})"
        )
