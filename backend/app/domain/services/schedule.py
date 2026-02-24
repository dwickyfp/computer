"""
ScheduleService — business logic for cron schedule management.

Owns the DB session, creates repositories, keeps APScheduler in sync
via DynamicSchedulerService on every write operation.
"""

from typing import List, Optional

from sqlalchemy.orm import Session

from app.domain.models.schedule import Schedule, ScheduleRunHistory
from app.domain.repositories.schedule import (
    ScheduleRepository,
    ScheduleRunHistoryRepository,
)
from app.domain.schemas.schedule import ScheduleCreate, ScheduleUpdate
from app.infrastructure.tasks.dynamic_scheduler import dynamic_scheduler_service
from app.core.exceptions import EntityNotFoundError


class ScheduleService:
    """
    Manages schedule CRUD and keeps APScheduler in sync.

    All write methods:
      1. Persist to DB (commit)
      2. Sync APScheduler (register / unregister / reload)
    """

    def __init__(self, db: Session) -> None:
        self.db = db
        self.repository = ScheduleRepository(db)
        self.run_history_repository = ScheduleRunHistoryRepository(db)

    # ------------------------------------------------------------------
    # Read Operations
    # ------------------------------------------------------------------

    def list_schedules(self, skip: int = 0, limit: int = 100) -> List[Schedule]:
        """Return all schedules paginated, ordered by creation date desc."""
        return self.repository.get_all_paginated(skip=skip, limit=limit)

    def count_schedules(self) -> int:
        """Total count of all schedules."""
        return self.repository.count()

    def get_schedule(self, schedule_id: int) -> Schedule:
        """
        Return a single schedule with eagerly loaded run_history.
        Raises EntityNotFoundError if not found.
        """
        schedule = self.repository.get_by_id(schedule_id)
        return schedule

    def get_run_history(
        self, schedule_id: int, skip: int = 0, limit: int = 50
    ) -> List[ScheduleRunHistory]:
        """Return paginated run history for a schedule."""
        # Ensure schedule exists
        self.repository.get_by_id(schedule_id)
        return self.run_history_repository.get_by_schedule(
            schedule_id, skip=skip, limit=limit
        )

    def count_run_history(self, schedule_id: int) -> int:
        """Total count of run history rows for a schedule."""
        return self.run_history_repository.count_by_schedule(schedule_id)

    # ------------------------------------------------------------------
    # Write Operations
    # ------------------------------------------------------------------

    def create_schedule(self, data: ScheduleCreate) -> Schedule:
        """
        Create a new schedule and register it in APScheduler if ACTIVE.
        """
        self._validate_task_exists(data.task_type, data.task_id)

        schedule = self.repository.create(
            name=data.name,
            description=data.description,
            task_type=data.task_type,
            task_id=data.task_id,
            cron_expression=data.cron_expression,
            status=data.status,
        )
        self.db.commit()
        self.db.refresh(schedule)

        # Register in APScheduler (only if ACTIVE)
        dynamic_scheduler_service.register(schedule)

        return schedule

    def update_schedule(self, schedule_id: int, data: ScheduleUpdate) -> Schedule:
        """
        Update schedule fields and reload its APScheduler job.

        The old job is removed, then a new one is registered with the
        updated cron expression / status.
        """
        # Ensure exists
        existing = self.repository.get_by_id(schedule_id)

        update_kwargs = {k: v for k, v in data.dict().items() if v is not None}

        if "task_type" in update_kwargs or "task_id" in update_kwargs:
            task_type = update_kwargs.get("task_type", existing.task_type)
            task_id = update_kwargs.get("task_id", existing.task_id)
            self._validate_task_exists(task_type, task_id)

        # Always unregister first to avoid stale jobs
        dynamic_scheduler_service.unregister(schedule_id)

        schedule = self.repository.update(schedule_id, **update_kwargs)
        self.db.commit()
        self.db.refresh(schedule)

        # Re-register with updated config
        dynamic_scheduler_service.register(schedule)

        return schedule

    def delete_schedule(self, schedule_id: int) -> None:
        """
        Delete schedule + cascade-delete run history.
        APScheduler job is removed first to avoid orphaned jobs.
        """
        # Ensure exists (raises EntityNotFoundError if not)
        self.repository.get_by_id(schedule_id)

        # Unregister from APScheduler before DB delete
        dynamic_scheduler_service.unregister(schedule_id)

        self.repository.delete(schedule_id)
        self.db.commit()

    def pause_schedule(self, schedule_id: int) -> Schedule:
        """
        Set status=PAUSED and remove APScheduler job.
        The schedule remains in DB and can be resumed later.
        """
        self.repository.get_by_id(schedule_id)

        dynamic_scheduler_service.unregister(schedule_id)

        schedule = self.repository.update(schedule_id, status="PAUSED")
        self.db.commit()
        self.db.refresh(schedule)
        return schedule

    def resume_schedule(self, schedule_id: int) -> Schedule:
        """
        Set status=ACTIVE and re-register APScheduler job.
        """
        self.repository.get_by_id(schedule_id)

        schedule = self.repository.update(schedule_id, status="ACTIVE")
        self.db.commit()
        self.db.refresh(schedule)

        dynamic_scheduler_service.register(schedule)

        return schedule

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_task_exists(self, task_type: str, task_id: int) -> None:
        """
        Validate that the referenced task actually exists in the DB.
        Raises ValueError if not found.
        """
        if task_type == "FLOW_TASK":
            from app.domain.models.flow_task import FlowTask

            obj = self.db.query(FlowTask).filter(FlowTask.id == task_id).first()
            if obj is None:
                raise ValueError(f"FlowTask with id={task_id} does not exist")
        elif task_type == "LINKED_TASK":
            from app.domain.models.linked_task import LinkedTask

            obj = self.db.query(LinkedTask).filter(LinkedTask.id == task_id).first()
            if obj is None:
                raise ValueError(f"LinkedTask with id={task_id} does not exist")
        else:
            raise ValueError(f"Unknown task_type: {task_type}")
