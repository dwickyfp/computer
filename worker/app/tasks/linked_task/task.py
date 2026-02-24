"""
Linked Task Celery task entry point.
"""

from typing import Any

import structlog

from app.celery_app import celery_app
from app.tasks.base import BaseTask
from app.tasks.linked_task.executor import execute_linked_task

logger = structlog.get_logger(__name__)


@celery_app.task(
    base=BaseTask,
    name="worker.linked_task.execute",
    bind=True,
    max_retries=0,
    reject_on_worker_lost=False,  # Don't re-queue (would cause duplicate executions)
    queue="default",
    acks_late=True,
)
def execute_linked_task_task(
    self,
    linked_task_id: int,
    run_history_id: int,
) -> dict[str, Any]:
    """
    Execute a full linked task DAG.

    Args:
        linked_task_id: PK of the LinkedTask record.
        run_history_id: PK of the LinkedTaskRunHistory record.
    """
    logger.info(
        "Linked task execution started",
        task_id=self.request.id,
        linked_task_id=linked_task_id,
        run_history_id=run_history_id,
    )

    self.update_state(
        state="PROGRESS",
        meta={
            "status": "executing",
            "linked_task_id": linked_task_id,
            "run_history_id": run_history_id,
        },
    )

    return execute_linked_task(
        linked_task_id=linked_task_id,
        run_history_id=run_history_id,
    )
