"""
Internal Notification Config model.

Stores HTTP-based internal notification endpoint configurations.
Multiple configs can be created, each independently enabled/disabled.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.models.base import Base


class InternalNotificationConfig(Base):
    """
    Internal Notification Config model.

    Each row represents one internal notification endpoint configuration
    (e.g. a company email API). Multiple rows are supported; each can be
    individually enabled or disabled.
    """

    __tablename__ = "internal_notification_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # HTTP endpoint parameters (mapped from main.py reference script)
    base_url: Mapped[str] = mapped_column(Text, nullable=False)
    requester: Mapped[str] = mapped_column(String(255), nullable=False)
    menu_code: Mapped[str] = mapped_column(String(255), nullable=False)
    company_group_id: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    mail_from_code: Mapped[str] = mapped_column(String(255), nullable=False)
    mail_to: Mapped[str] = mapped_column(Text, nullable=False)  # comma-separated emails
    subject: Mapped[str] = mapped_column(String(500), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
        onupdate=lambda: datetime.now(ZoneInfo("Asia/Jakarta")),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<InternalNotificationConfig(id={self.id}, name='{self.name}', "
            f"is_enabled={self.is_enabled})>"
        )
