"""
Lineage generation Celery task.

Parses custom SQL to extract column-level lineage metadata.
"""

from datetime import datetime, timezone
from typing import Any
import json

import structlog

from app.celery_app import celery_app
from app.tasks.base import BaseTask
from app.tasks.lineage.parser import parse_lineage
from app.core.database import get_db_session

logger = structlog.get_logger(__name__)


def _notify_lineage_error(table_sync_id: int, source_table: str, error_msg: str) -> None:
    """Upsert an ERROR notification into notification_log for a lineage failure.

    Mirrors the pattern used in flow_task/executor.py's _notify_flow_task_error.
    Swallows all exceptions so a notification failure never breaks the caller.
    """
    try:
        from zoneinfo import ZoneInfo
        from sqlalchemy import text

        key = f"lineage_error_sync_{table_sync_id}"
        title = f"Lineage Generation Failed — {source_table}"
        message = (
            f"Table sync ID {table_sync_id} (table: {source_table}) lineage generation failed "
            f"in the worker. Error: {error_msg}"
        )[:2000]
        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        with get_db_session() as db:
            limit_row = db.execute(
                text(
                    "SELECT config_value FROM rosetta_setting_configuration "
                    "WHERE config_key = 'NOTIFICATION_ITERATION_DEFAULT' LIMIT 1"
                )
            ).fetchone()
            max_iter = int(limit_row.config_value) if limit_row else 3

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
            "Failed to write lineage error notification",
            table_sync_id=table_sync_id,
            error=str(exc),
        )


@celery_app.task(
    base=BaseTask,
    name="worker.lineage.generate",
    bind=True,
    max_retries=2,
    default_retry_delay=5,
    queue="default",
    acks_late=True,
)
def generate_lineage_task(
    self,
    table_sync_id: int,
    custom_sql: str | None,
    source_table: str,
    source_columns: list[str] | None = None,
) -> dict[str, Any]:
    """
    Celery task to generate lineage metadata for a table sync config.

    Args:
        table_sync_id: ID of pipelines_destination_table_sync record
        custom_sql: Custom SQL query to parse
        source_table: Primary source table name
        source_columns: List of source column names

    Returns:
        Dict with lineage metadata or error
    """
    logger.info(
        "Lineage generation started",
        task_id=self.request.id,
        table_sync_id=table_sync_id,
        source_table=source_table,
        has_sql=bool(custom_sql),
    )

    self.update_state(
        state="PROGRESS",
        meta={"status": "parsing", "table_sync_id": table_sync_id},
    )

    try:
        # Parse the SQL to extract lineage
        lineage_metadata = parse_lineage(
            sql=custom_sql,
            source_table=source_table,
            source_columns=source_columns or [],
        )

        now = datetime.now(timezone.utc)

        # Update database with lineage result
        with get_db_session() as db:
            from sqlalchemy import text

            db.execute(
                text(
                    """
                    UPDATE pipelines_destination_table_sync 
                    SET lineage_metadata = :metadata,
                        lineage_status = 'COMPLETED',
                        lineage_error = NULL,
                        lineage_generated_at = :generated_at,
                        updated_at = :updated_at
                    WHERE id = :id
                """
                ),
                {
                    "id": table_sync_id,
                    "metadata": json.dumps(lineage_metadata),
                    "generated_at": now,
                    "updated_at": now,
                },
            )

        logger.info(
            "Lineage generation completed",
            task_id=self.request.id,
            table_sync_id=table_sync_id,
            output_columns=len(lineage_metadata.get("output_columns", [])),
        )

        return {
            "success": True,
            "table_sync_id": table_sync_id,
            "lineage": lineage_metadata,
        }

    except Exception as e:
        logger.error(
            "Lineage generation failed",
            task_id=self.request.id,
            table_sync_id=table_sync_id,
            error=str(e),
        )

        # Update database with error
        try:
            with get_db_session() as db:
                from sqlalchemy import text

                db.execute(
                    text(
                        """
                        UPDATE pipelines_destination_table_sync 
                        SET lineage_status = 'FAILED',
                            lineage_error = :error,
                            updated_at = :updated_at
                        WHERE id = :id
                    """
                    ),
                    {
                        "id": table_sync_id,
                        "error": str(e)[:1000],
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
        except Exception as db_error:
            logger.error(
                "Failed to update lineage error status",
                error=str(db_error),
            )

        # Push notification for lineage failure
        _notify_lineage_error(table_sync_id, source_table, str(e))

        return {
            "success": False,
            "table_sync_id": table_sync_id,
            "error": str(e),
        }
