"""
Notification Log repository for Compute Engine.

Uses DatabaseSession for consistent connection management and automatic
commit/rollback handling.
"""

import logging
from typing import Optional
from dataclasses import dataclass

from core.database import DatabaseSession
from core.timezone import now_in_target_tz

logger = logging.getLogger(__name__)


@dataclass
class NotificationLogCreate:
    """Schema for creating a notification log."""
    key_notification: str
    title: str
    message: str
    type: str  # 'INFO', 'WARNING', 'ERROR'
    iteration_check: int = 1
    is_read: bool = False
    is_deleted: bool = False
    is_sent: bool = False
    is_force_sent: bool = False


class NotificationLogRepository:
    """Repository for NotificationLog operations using DatabaseSession."""

    def upsert_notification_by_key(self, notification_data: NotificationLogCreate) -> Optional[int]:
        """
        Insert or update notification based on key_notification and iteration logic.

        Logic matches backend:
        1. Check if key exists (get latest).
        2. If exists and iteration_check < limit: Update message, increment iteration.
        3. Else: Insert new record.
        """
        try:
            with DatabaseSession() as session:
                # Get the latest notification with this key
                session.execute(
                    """
                    SELECT id, iteration_check
                    FROM notification_log
                    WHERE key_notification = %s AND is_deleted = FALSE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (notification_data.key_notification,)
                )
                result = session.fetchone()

                # Get iteration limit (default: 3, matching backend)
                iteration_limit = 3
                try:
                    session.execute(
                        "SELECT config_value FROM rosetta_setting_configuration WHERE config_key = 'NOTIFICATION_ITERATION_DEFAULT'"
                    )
                    setting_row = session.fetchone()
                    if setting_row:
                        iteration_limit = int(setting_row["config_value"])
                except Exception:
                    # Fallback to default
                    pass

                now = now_in_target_tz()

                if result and (result["iteration_check"] < iteration_limit or notification_data.is_force_sent):
                    notification_id = result["id"]
                    current_iteration = result["iteration_check"]

                    # Update existing
                    session.execute(
                        """
                        UPDATE notification_log
                        SET message = %s,
                            title = %s,
                            type = %s,
                            is_read = FALSE,
                            is_deleted = FALSE,
                            iteration_check = %s,
                            is_force_sent = %s,
                            is_sent = FALSE,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            notification_data.message,
                            notification_data.title,
                            notification_data.type,
                            current_iteration + 1,
                            notification_data.is_force_sent,
                            now,
                            notification_id
                        )
                    )
                    return notification_id

                else:
                    # Insert new record
                    session.execute(
                        """
                        INSERT INTO notification_log (
                            key_notification, title, message, type,
                            is_read, is_deleted, iteration_check, is_sent, is_force_sent,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            notification_data.key_notification,
                            notification_data.title,
                            notification_data.message,
                            notification_data.type,
                            False,  # is_read
                            False,  # is_deleted
                            1,      # Reset iteration to 1
                            False,  # is_sent
                            notification_data.is_force_sent,
                            now,
                            now
                        )
                    )
                    new_row = session.fetchone()
                    return new_row["id"] if new_row else None

        except Exception as e:
            logger.error(f"Failed to upsert notification log: {e}")
            return None
