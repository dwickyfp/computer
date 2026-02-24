"""
Base task class with error handling and lifecycle hooks.
"""

from celery import Task

import structlog

logger = structlog.get_logger(__name__)


class BaseTask(Task):
    """
    Base task with standardized error handling.

    Provides on_failure hook for logging and notification.
    """

    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails."""
        logger.error(
            "Task failed",
            task_id=task_id,
            task_name=self.name,
            error=str(exc),
            traceback=str(einfo),
        )
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Called when task is retried."""
        logger.warning(
            "Task retrying",
            task_id=task_id,
            task_name=self.name,
            error=str(exc),
        )
        super().on_retry(exc, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        """Called when task succeeds."""
        logger.info(
            "Task completed",
            task_id=task_id,
            task_name=self.name,
        )
        super().on_success(retval, task_id, args, kwargs)
