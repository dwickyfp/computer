"""
Notification Log model.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.models.base import Base


class NotificationLog(Base):
    """
    Notification Log model.

    Stores system notifications and alerts.
    """

    __tablename__ = "notification_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key_notification: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(String(255), nullable=False)  # 'INFO', 'WARNING', 'ERROR'
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    iteration_check: Mapped[int] = mapped_column(Integer, default=0)
    is_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    is_force_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        onupdate=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<NotificationLog(id={self.id}, title='{self.title}', type='{self.type}')>"
