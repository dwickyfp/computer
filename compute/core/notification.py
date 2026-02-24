"""
Notification Log repository for Compute Engine.
"""

from datetime import datetime, timezone, timedelta
import logging
from typing import Optional, Any
from dataclasses import dataclass

from core.database import get_db_connection, return_db_connection

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
    """Repository for NotificationLog operations using raw SQL."""

    def upsert_notification_by_key(self, notification_data: NotificationLogCreate) -> Optional[int]:
        """
        Insert or update notification based on key_notification and iteration logic.
        
        Logic matches backend:
        1. Check if key exists (get latest).
        2. If exists and iteration_check < limit: Update message, increment iteration.
        3. Else: Insert new record.
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Get the latest notification with this key
                cursor.execute(
                    """
                    SELECT id, iteration_check 
                    FROM notification_log 
                    WHERE key_notification = %s AND is_deleted = FALSE
                    ORDER BY created_at DESC 
                    LIMIT 1
                    """,
                    (notification_data.key_notification,)
                )
                result = cursor.fetchone()
                
                # Get iteration limit (hardcoded for now as simple query, or could query settings)
                # Matching backend logic which defaults to 3
                iteration_limit = 3
                
                # We could query settings table here if needed, but for performance in compute path
                # we might want to cache it or just use default. Let's try to query it once safely.
                try:
                    cursor.execute(
                        "SELECT config_value FROM rosetta_setting_configuration WHERE config_key = 'NOTIFICATION_ITERATION_DEFAULT'"
                    )
                    setting_row = cursor.fetchone()
                    if setting_row:
                        iteration_limit = int(setting_row[0])
                except Exception:
                    # Fallback to default
                    pass

                now = datetime.now(timezone(timedelta(hours=7)))

                if result and (result[1] < iteration_limit or notification_data.is_force_sent):
                    notification_id = result[0]
                    current_iteration = result[1]
                    
                    # Update existing
                    cursor.execute(
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
                    conn.commit()
                    return notification_id
                
                else:
                    # Insert new record
                    cursor.execute(
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
                            False, # is_read
                            False, # is_deleted
                            1,     # Reset iteration to 1
                            False, # is_sent
                            notification_data.is_force_sent,
                            now,
                            now
                        )
                    )
                    new_id = cursor.fetchone()[0]
                    conn.commit()
                    return new_id

        except Exception as e:
            logger.error(f"Failed to upsert notification log: {e}")
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                return_db_connection(conn)
