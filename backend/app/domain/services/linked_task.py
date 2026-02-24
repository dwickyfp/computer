"""
Linked Task service — orchestrates all linked task business logic.
"""

from datetime import datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from celery import Celery

from app.core.config import settings
from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.models.linked_task import (
    LinkedTask,
    LinkedTaskRunHistory,
    LinkedTaskStatus,
)
from app.domain.repositories.linked_task import (
    LinkedTaskRepository,
    LinkedTaskGraphRepository,
    LinkedTaskRunHistoryRepository,
)
from app.domain.schemas.linked_task import LinkedTaskCreate, LinkedTaskUpdate, LinkedTaskGraphSave

logger = get_logger(__name__)
TZ = ZoneInfo("Asia/Jakarta")

# Initialize Celery client for sending tasks to worker
celery_client = Celery("backend", broker=settings.celery_broker_url)


class LinkedTaskService:
    def __init__(self, db: Session):
        self.db = db
        self.repo = LinkedTaskRepository(db)
        self.graph_repo = LinkedTaskGraphRepository(db)
        self.run_repo = LinkedTaskRunHistoryRepository(db)

    # ─── CRUD ────────────────────────────────────────────────────────────────

    def list_linked_tasks(self, page: int = 1, page_size: int = 20):
        items, total = self.repo.list(page, page_size)
        return items, total

    def get_linked_task(self, linked_task_id: int) -> LinkedTask:
        task = self.repo.get(linked_task_id)
        if not task:
            raise EntityNotFoundError(f"LinkedTask {linked_task_id} not found")
        return task

    def create_linked_task(self, data: LinkedTaskCreate) -> LinkedTask:
        task = self.repo.create(name=data.name, description=data.description)
        self.db.commit()
        self.db.refresh(task)
        logger.info(f"LinkedTask created: id={task.id} name={task.name}")
        return task

    def update_linked_task(self, linked_task_id: int, data: LinkedTaskUpdate) -> LinkedTask:
        task = self.get_linked_task(linked_task_id)
        if data.name is not None:
            task.name = data.name
        if data.description is not None:
            task.description = data.description
        self.db.commit()
        self.db.refresh(task)
        return task

    def delete_linked_task(self, linked_task_id: int) -> None:
        task = self.get_linked_task(linked_task_id)
        if task.status == LinkedTaskStatus.RUNNING:
            raise ValueError(f"Cannot delete LinkedTask {linked_task_id} while it is running")
        self.repo.delete(linked_task_id)
        self.db.commit()
        logger.info(f"LinkedTask deleted: id={linked_task_id}")

    # ─── Graph ───────────────────────────────────────────────────────────────

    def save_graph(self, linked_task_id: int, data: LinkedTaskGraphSave):
        """Replace the full graph (steps + edges) for a linked task."""
        # Ensure exists
        self.get_linked_task(linked_task_id)

        steps_data = [
            {"id": s.id, "flow_task_id": s.flow_task_id, "pos_x": s.pos_x, "pos_y": s.pos_y}
            for s in data.steps
        ]
        edges_data = [
            {
                "source_step_id": e.source_step_id,
                "target_step_id": e.target_step_id,
                "condition": e.condition,
            }
            for e in data.edges
        ]
        new_steps, new_edges = self.graph_repo.replace_graph(
            linked_task_id, steps_data, edges_data
        )
        self.db.commit()
        # Refresh steps for response
        for s in new_steps:
            self.db.refresh(s)
        return new_steps, new_edges

    def get_graph(self, linked_task_id: int):
        """Return steps and edges for a linked task."""
        self.get_linked_task(linked_task_id)
        steps = self.graph_repo.get_steps(linked_task_id)
        edges = self.graph_repo.get_edges(linked_task_id)
        return steps, edges

    # ─── Run ─────────────────────────────────────────────────────────────────

    def trigger_run(self, linked_task_id: int) -> LinkedTaskRunHistory:
        task = self.get_linked_task(linked_task_id)

        # Create run history record
        run = self.run_repo.create(linked_task_id=linked_task_id)
        self.db.flush()

        # Update task status
        task.status = LinkedTaskStatus.RUNNING
        self.db.commit()
        self.db.refresh(run)

        # Dispatch Celery task using send_task to allow loose coupling
        result = celery_client.send_task(
            "worker.linked_task.execute",
            kwargs={
                "linked_task_id": linked_task_id,
                "run_history_id": run.id,
            },
            queue="default",
        )

        # Save celery task id
        run.celery_task_id = result.id
        self.db.commit()
        self.db.refresh(run)

        logger.info(f"LinkedTask triggered: id={linked_task_id} run={run.id} celery={result.id}")
        return run

    def cancel_run(self, linked_task_id: int, run_id: int) -> LinkedTaskRunHistory:
        """Cancel a running linked task by revoking its Celery task."""
        from datetime import datetime

        task = self.get_linked_task(linked_task_id)
        run = self.run_repo.get(run_id)
        if not run or run.linked_task_id != linked_task_id:
            raise EntityNotFoundError(f"Run {run_id} not found for LinkedTask {linked_task_id}")
        if run.status != "RUNNING":
            raise ValueError(f"Run {run_id} is not running (status={run.status})")

        # Revoke the Celery task
        if run.celery_task_id:
            celery_client.control.revoke(run.celery_task_id, terminate=True, signal="SIGTERM")
            logger.info(f"Revoked celery task: {run.celery_task_id}")

        # Mark run as CANCELLED
        run.status = "CANCELLED"
        run.finished_at = datetime.now(TZ)
        run.error_message = "Cancelled by user"

        # Reset linked task status to IDLE
        task.status = LinkedTaskStatus.IDLE
        self.db.commit()
        self.db.refresh(run)

        logger.info(f"LinkedTask run cancelled: linked_task_id={linked_task_id} run_id={run_id}")
        return run

    def get_run_history(self, linked_task_id: int, page: int = 1, page_size: int = 20):
        self.get_linked_task(linked_task_id)
        return self.run_repo.list(linked_task_id, page, page_size)
