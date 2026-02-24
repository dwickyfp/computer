"""
Notification Log repository.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.domain.models.notification_log import NotificationLog
from app.domain.models.rosetta_setting_configuration import RosettaSettingConfiguration
from app.domain.schemas.notification_log import (
    NotificationLogCreate,
    NotificationLogUpdate,
)


class NotificationLogRepository:
    """Repository for NotificationLog operations."""

    def __init__(self, db: Session):
        self.db = db

    def upsert_notification_by_key(
        self, notification_data: NotificationLogCreate
    ) -> NotificationLog:
        """
        Insert or update notification based on key_notification and iteration logic.

        Logic:
        1. Check if key exists (get latest).
        2. If exists:
           - If iteration_check < 3: Update message, reset is_read/is_deleted, increment iteration.
           - If iteration_check >= 3: Insert new record.
        3. If not exists: Insert new record.
        """
        # Get latest notification with this key
        latest_notification = (
            self.db.query(NotificationLog)
            .filter(
                NotificationLog.key_notification == notification_data.key_notification
            )
            .order_by(desc(NotificationLog.created_at))
            .first()
        )

        now = datetime.now(ZoneInfo('Asia/Jakarta'))

        # Get iteration limit from settings
        iteration_limit = 3
        try:
            setting = (
                self.db.query(RosettaSettingConfiguration)
                .filter(
                    RosettaSettingConfiguration.config_key
                    == "NOTIFICATION_ITERATION_DEFAULT"
                )
                .first()
            )
            if setting and setting.config_value:
                iteration_limit = int(setting.config_value)
        except Exception:
            # Fallback to default if any error occurs (e.g. invalid int conversion)
            iteration_limit = 3

        if (
            latest_notification
            and latest_notification.iteration_check < iteration_limit
        ):
            # Update existing
            latest_notification.message = notification_data.message
            latest_notification.title = (
                notification_data.title
            )  # Update title too just in case
            latest_notification.type = notification_data.type  # Update type
            latest_notification.is_read = False
            latest_notification.is_deleted = False
            latest_notification.iteration_check += 1
            latest_notification.updated_at = now

            self.db.commit()
            self.db.refresh(latest_notification)
            return latest_notification
        else:
            # Insert new (either not exists or iteration >= 3)
            # If inserting new when iteration >= 3, should we reset iteration to 1?
            # User said "create insert new job". Assuming new fresh record implies iteration 1.
            # But the passed `notification_data` has `iteration_check`.
            # I should ensure the new record starts with iteration 1 (or whatever is passed).

            new_notification = NotificationLog(
                key_notification=notification_data.key_notification,
                title=notification_data.title,
                message=notification_data.message,
                type=notification_data.type,
                is_read=False,
                is_deleted=False,
                iteration_check=1,  # Reset iteration for new job
                is_sent=False,
                created_at=now,
                updated_at=now,
            )
            self.db.add(new_notification)
            self.db.commit()
            self.db.refresh(new_notification)
            return new_notification

    def create(self, notification_data: NotificationLogCreate) -> NotificationLog:
        """Create a new notification log."""
        notification = NotificationLog(
            key_notification=notification_data.key_notification,
            title=notification_data.title,
            message=notification_data.message,
            type=notification_data.type,
            is_read=notification_data.is_read,
            is_deleted=notification_data.is_deleted,
            iteration_check=notification_data.iteration_check,
            is_sent=notification_data.is_sent,
        )
        self.db.add(notification)
        self.db.commit()
        self.db.refresh(notification)
        return notification

    def get_by_id(self, notification_id: int) -> Optional[NotificationLog]:
        """Get notification by ID."""
        return (
            self.db.query(NotificationLog)
            .filter(NotificationLog.id == notification_id)
            .first()
        )

    def get_all(
        self, skip: int = 0, limit: int = 100, is_read: Optional[bool] = None
    ) -> List[NotificationLog]:
        """Get all active notifications, ordered by creation time desc."""
        query = self.db.query(NotificationLog).filter(
            NotificationLog.is_deleted == False
        )

        if is_read is not None:
            query = query.filter(NotificationLog.is_read == is_read)

        return (
            query.order_by(desc(NotificationLog.created_at))
            .offset(skip)
            .limit(limit)
            .all()
        )

    def mark_as_read(self, notification_id: int) -> Optional[NotificationLog]:
        """Mark a notification as read."""
        notification = self.get_by_id(notification_id)
        if notification:
            notification.is_read = True
            notification.updated_at = datetime.now(ZoneInfo('Asia/Jakarta'))
            self.db.commit()
            self.db.refresh(notification)
        return notification

    def mark_all_as_read(self) -> int:
        """Mark all active unread notifications as read."""
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        result = (
            self.db.query(NotificationLog)
            .filter(
                NotificationLog.is_deleted == False, NotificationLog.is_read == False
            )
            .update({NotificationLog.is_read: True, NotificationLog.updated_at: now})
        )
        self.db.commit()
        return result

    def soft_delete(self, notification_id: int) -> Optional[NotificationLog]:
        """Soft delete a notification."""
        notification = self.get_by_id(notification_id)
        if notification:
            notification.is_deleted = True
            notification.updated_at = datetime.now(ZoneInfo('Asia/Jakarta'))
            self.db.commit()
            self.db.refresh(notification)
        return notification

    def soft_delete_all(self) -> int:
        """Soft delete all notifications."""
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        result = (
            self.db.query(NotificationLog)
            .filter(NotificationLog.is_deleted == False)
            .update({NotificationLog.is_deleted: True, NotificationLog.updated_at: now})
        )
        self.db.commit()
        return result

    def delete_old_notifications(self, days_to_keep: int = 30) -> int:
        """Permanently delete notifications older than specified days based on created_at.

        Args:
            days_to_keep: Number of days to keep notifications (default: 30)

        Returns:
            Number of notifications deleted
        """
        from app.core.logging import get_logger
        from zoneinfo import ZoneInfo

        logger = get_logger(__name__)

        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        cutoff_date = now - timedelta(days=days_to_keep)

        logger.info(
            f"Cleanup check - Current time: {now}, Cutoff date: {cutoff_date}, Looking for records older than {cutoff_date}"
        )

        # Query old notifications
        old_notifications = (
            self.db.query(NotificationLog)
            .filter(NotificationLog.created_at < cutoff_date)
            .all()
        )

        deleted_count = len(old_notifications)

        if deleted_count > 0:
            logger.info(f"Found {deleted_count} notifications to delete")
            for notification in old_notifications:
                logger.debug(
                    f"Deleting notification ID {notification.id} created at {notification.created_at}"
                )
        else:
            logger.info(
                f"No old notifications found to delete (older than {cutoff_date})"
            )

        # Delete them
        for notification in old_notifications:
            self.db.delete(notification)

        self.db.commit()
        return deleted_count
