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
    """
    Upsert an ERROR notification for a lineage generation failure.

    L-2 fix: delegates to the shared notify_error() helper in
    app.core.notifications to eliminate the duplicated SQL block.
    """
    from app.core.notifications import notify_error

    key = f"lineage_error_sync_{table_sync_id}"
    title = f"Lineage Generation Failed \u2014 {source_table}"
    message = (
        f"Table sync ID {table_sync_id} (table: {source_table}) lineage generation failed "
        f"in the worker. Error: {error_msg}"
    )
    notify_error(key=key, title=title, message=message)


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
