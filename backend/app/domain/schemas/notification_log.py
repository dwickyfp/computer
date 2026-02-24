"""
Notification Log schema.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class NotificationLogBase(BaseModel):
    """Base schema for NotificationLog."""
    key_notification: str
    title: str
    message: str
    type: str
    is_read: bool = False
    is_deleted: bool = False
    iteration_check: int = 0
    is_sent: bool = False


class NotificationLogCreate(NotificationLogBase):
    """Schema for creating a NotificationLog."""
    pass


class NotificationLogUpdate(BaseModel):
    """Schema for updating a NotificationLog."""
    key_notification: Optional[str] = None
    title: Optional[str] = None
    message: Optional[str] = None
    type: Optional[str] = None
    is_read: Optional[bool] = None
    is_deleted: Optional[bool] = None
    iteration_check: Optional[int] = None
    is_sent: Optional[bool] = None


class NotificationLog(NotificationLogBase):
    """Schema for reading a NotificationLog."""
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
