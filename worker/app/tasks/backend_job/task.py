"""Generic worker task for backend-owned jobs executed via subprocess bridge."""

from __future__ import annotations

from typing import Any

import structlog

from app.celery_app import celery_app
from app.tasks.backend_job.executor import execute_backend_job
from app.tasks.base import BaseTask

logger = structlog.get_logger(__name__)


@celery_app.task(
    base=BaseTask,
    name="worker.backend_job.run",
    bind=True,
    queue="default",
    acks_late=True,
    max_retries=1,
    default_retry_delay=10,
)
def run_backend_job_task(
    self,
    job_name: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    logger.info(
        "Backend bridge task started",
        task_id=self.request.id,
        job_name=job_name,
    )

    self.update_state(
        state="PROGRESS",
        meta={"status": "running", "job_name": job_name},
    )

    try:
        result = execute_backend_job(job_name, payload)
        return {
            "job_name": job_name,
            "task_id": self.request.id,
            **result,
        }
    except Exception as exc:
        logger.error(
            "Backend bridge task failed",
            task_id=self.request.id,
            job_name=job_name,
            error=str(exc),
        )
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            raise RuntimeError(str(exc)) from exc
