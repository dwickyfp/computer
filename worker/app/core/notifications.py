"""
Shared notification helper for worker tasks.

L-2 fix: The duplicated upsert-notification logic that previously existed in
both ``flow_task/executor.py`` and ``lineage/task.py`` has been extracted here.
Any future task that needs to write an error notification should call
``notify_error()`` directly rather than copy-pasting the ~40-line SQL block.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.core.database import get_db_session

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Jakarta")


def notify_error(key: str, title: str, message: str) -> None:
    """
    Upsert an ERROR notification into ``notification_log``.

    Mirrors the backend's ``upsert_notification_by_key`` logic:
    - If a row with ``key_notification = key`` already exists and its
      ``iteration_check`` is below the configured limit: UPDATE (increment).
    - Otherwise: INSERT a new row.

    The iteration limit is read from ``rosetta_setting_configuration``
    (key ``NOTIFICATION_ITERATION_DEFAULT``), defaulting to 3.

    Swallows all exceptions so a notification failure never breaks the caller.

    Args:
        key:     Unique notification key (e.g. ``"flow_task_error_42_node-abc"``).
        title:   Short human-readable title for the notification.
        message: Full error text (truncated to 2000 chars).
    """
    try:
        # Guard against excessively long messages
        message = message[:2000]
        now = datetime.now(_TZ)

        with get_db_session() as db:
            # Fetch iteration limit (default 3)
            limit_row = db.execute(
                text(
                    "SELECT config_value FROM rosetta_setting_configuration "
                    "WHERE config_key = 'NOTIFICATION_ITERATION_DEFAULT' LIMIT 1"
                )
            ).fetchone()
            max_iter = int(limit_row.config_value) if limit_row else 3

            # Fetch the latest notification for this key regardless of is_deleted
            existing = db.execute(
                text(
                    "SELECT id, iteration_check FROM notification_log "
                    "WHERE key_notification = :key "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"key": key},
            ).fetchone()

            if existing and existing.iteration_check < max_iter:
                db.execute(
                    text("""
                        UPDATE notification_log
                        SET iteration_check = iteration_check + 1,
                            title           = :title,
                            message         = :message,
                            type            = 'ERROR',
                            is_read         = FALSE,
                            is_deleted      = FALSE,
                            is_sent         = FALSE,
                            updated_at      = :now
                        WHERE id = :id
                    """),
                    {"title": title, "message": message, "now": now, "id": existing.id},
                )
            else:
                db.execute(
                    text("""
                        INSERT INTO notification_log
                            (key_notification, title, message, type,
                             is_read, is_deleted, iteration_check,
                             is_sent, is_force_sent, created_at, updated_at)
                        VALUES
                            (:key, :title, :message, 'ERROR',
                             FALSE, FALSE, 1,
                             FALSE, FALSE, :now, :now)
                    """),
                    {"key": key, "title": title, "message": message, "now": now},
                )

    except Exception as exc:
        logger.warning(
            "Failed to write error notification (non-fatal)",
            extra={"key": key, "error": str(exc)},
        )
