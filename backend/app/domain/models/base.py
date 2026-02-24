"""
Base model for all SQLAlchemy models.

Provides common functionality and declarative base.
"""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """
    Base class for all database models.

    Provides common columns and functionality.
    """

    # Type annotation for mapped columns
    type_annotation_map = {datetime: DateTime(timezone=True)}

    def to_dict(self) -> dict[str, Any]:
        """
        Convert model to dictionary.

        Useful for serialization and logging.
        """
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    def update_from_dict(self, data: dict[str, Any]) -> None:
        """
        Update model attributes from dictionary.

        Args:
            data: Dictionary of attribute names and values
        """
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def __repr__(self) -> str:
        """String representation of model."""
        attrs = ", ".join(
            f"{col.name}={getattr(self, col.name)!r}" for col in self.__table__.columns
        )
        return f"{self.__class__.__name__}({attrs})"


class TimestampMixin:
    """
    Mixin for created_at and updated_at timestamps.

    Automatically manages creation and update timestamps.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False,
        comment="Record creation timestamp",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        onupdate=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False,
        comment="Record last update timestamp",
    )
