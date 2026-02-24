"""
Destination table list Celery task.

Fetches the list of tables from a destination and persists it to the config DB.
Task name: "worker.destination_table_list.fetch"
"""

from typing import Any

from app.celery_app import celery_app
from app.tasks.base import BaseTask
from app.tasks.destination_table_list.executor import execute_destination_table_list

import structlog

logger = structlog.get_logger(__name__)


@celery_app.task(
    base=BaseTask,
    name="worker.destination_table_list.fetch",
    bind=True,
    max_retries=2,
    default_retry_delay=10,
    queue="default",
    acks_late=True,
)
def fetch_destination_table_list_task(
    self,
    destination_id: int,
) -> dict[str, Any]:
    """
    Celery task to fetch and persist destination table list.

    Args:
        destination_id: Destination database ID.

    Returns:
        Dict with destination_id, total_tables, tables, error keys.
    """
    logger.info(
        "Destination table list task started",
        task_id=self.request.id,
        destination_id=destination_id,
    )

    self.update_state(
        state="PROGRESS",
        meta={"status": "fetching", "destination_id": destination_id},
    )

    try:
        result = execute_destination_table_list(destination_id=destination_id)
        return result
    except Exception as exc:
        logger.error(
            "Destination table list task failed",
            task_id=self.request.id,
            destination_id=destination_id,
            error=str(exc),
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            return {
                "destination_id": destination_id,
                "total_tables": 0,
                "tables": [],
                "error": str(exc),
            }
