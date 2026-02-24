"""
Rosetta Setting Configuration model.

Stores application configuration settings that can be edited by users.
"""

from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.models.base import Base, TimestampMixin


class RosettaSettingConfiguration(Base, TimestampMixin):
    """
    Rosetta Setting Configuration model.
    
    Stores key-value configuration settings for the application.
    """
    
    __tablename__ = "rosetta_setting_configuration"
    __table_args__ = (
        {"comment": "Application configuration settings"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique identifier",
    )

    # Configuration Data
    config_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Configuration key",
    )
    
    config_value: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Configuration value",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"RosettaSettingConfiguration(id={self.id}, "
            f"config_key={self.config_key}, "
            f"config_value={self.config_value})"
        )
