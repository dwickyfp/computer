"""
Preview Celery task.

Defines the async task that executes preview queries in the worker.
"""

from typing import Any

from app.celery_app import celery_app
from app.tasks.base import BaseTask
from app.tasks.preview.executor import execute_preview

import structlog

logger = structlog.get_logger(__name__)


@celery_app.task(
    base=BaseTask,
    name="worker.preview.execute",
    bind=True,
    max_retries=1,
    default_retry_delay=5,
    queue="preview",
    acks_late=True,
)
def execute_preview_task(
    self,
    sql: str | None,
    source_id: int,
    destination_id: int,
    table_name: str,
    filter_sql: str | None = None,
    include_profiling: bool = False,
) -> dict[str, Any]:
    """
    Celery task to execute preview query.

    This task runs in the worker process and handles:
    - DuckDB connection + Postgres extension
    - SQL validation and rewriting
    - Result serialization and caching
    - Optional data profiling stats (D7)

    Args:
        sql: Optional custom SQL query
        source_id: Source database ID
        destination_id: Destination database ID
        table_name: Table name to preview
        filter_sql: Optional filter SQL
        include_profiling: If True, include column profiling statistics

    Returns:
        Dict with columns, column_types, data, error keys (+ profile if requested)
    """
    logger.info(
        "Preview task started",
        task_id=self.request.id,
        source_id=source_id,
        destination_id=destination_id,
        table_name=table_name,
        has_sql=bool(sql),
        has_filter=bool(filter_sql),
        include_profiling=include_profiling,
    )

    # Update task state to PROGRESS
    self.update_state(
        state="PROGRESS",
        meta={"status": "executing", "table_name": table_name},
    )

    result = execute_preview(
        sql=sql,
        source_id=source_id,
        destination_id=destination_id,
        table_name=table_name,
        filter_sql=filter_sql,
        include_profiling=include_profiling,
    )

    return result
