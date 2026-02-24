"""
Schedule and ScheduleRunHistory repositories.

Extends BaseRepository with schedule-specific query methods.
"""

from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.domain.models.schedule import Schedule, ScheduleRunHistory, ScheduleRunStatus
from app.domain.repositories.base import BaseRepository


class ScheduleRepository(BaseRepository[Schedule]):
    """Repository for Schedule model CRUD and domain queries."""

    def __init__(self, db: Session) -> None:
        super().__init__(Schedule, db)

    def get_all_active(self) -> List[Schedule]:
        """Return all schedules with status ACTIVE."""
        return (
            self.db.query(Schedule)
            .filter(Schedule.status == "ACTIVE")
            .order_by(Schedule.created_at.desc())
            .all()
        )

    def get_all_paginated(self, skip: int = 0, limit: int = 100) -> List[Schedule]:
        """Return all schedules ordered by creation date descending."""
        return (
            self.db.query(Schedule)
            .order_by(Schedule.created_at.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_by_task(self, task_type: str, task_id: int) -> List[Schedule]:
        """Return schedules targeting a specific task."""
        return (
            self.db.query(Schedule)
            .filter(Schedule.task_type == task_type, Schedule.task_id == task_id)
            .all()
        )

    def update_last_run_at(self, schedule_id: int) -> None:
        """Update last_run_at timestamp to now (Jakarta TZ)."""
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        self.db.query(Schedule).filter(Schedule.id == schedule_id).update(
            {"last_run_at": now, "updated_at": now}
        )

    def update_next_run_at(
        self, schedule_id: int, next_run: Optional[datetime]
    ) -> None:
        """Update next_run_at timestamp."""
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        self.db.query(Schedule).filter(Schedule.id == schedule_id).update(
            {"next_run_at": next_run, "updated_at": now}
        )


class ScheduleRunHistoryRepository(BaseRepository[ScheduleRunHistory]):
    """Repository for ScheduleRunHistory — append-only execution log."""

    def __init__(self, db: Session) -> None:
        super().__init__(ScheduleRunHistory, db)

    def create_run(
        self, schedule_id: int, task_type: str, task_id: int
    ) -> ScheduleRunHistory:
        """
        Insert a RUNNING row for a new execution.

        Returns the persisted (not yet committed) run history record.
        """
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        run = ScheduleRunHistory(
            schedule_id=schedule_id,
            task_type=task_type,
            task_id=task_id,
            triggered_at=now,
            status=ScheduleRunStatus.RUNNING,
        )
        self.db.add(run)
        self.db.flush()
        self.db.refresh(run)
        return run

    def complete_run(
        self,
        run_id: int,
        status: str,
        message: Optional[str],
        duration_ms: Optional[int],
    ) -> Optional[ScheduleRunHistory]:
        """
        Mark a run as completed with final status, message and duration.
        """
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        run = (
            self.db.query(ScheduleRunHistory)
            .filter(ScheduleRunHistory.id == run_id)
            .first()
        )
        if run is None:
            return None
        run.status = status
        run.message = message
        run.completed_at = now
        run.duration_ms = duration_ms
        run.updated_at = now
        self.db.flush()
        self.db.refresh(run)
        return run

    def get_by_schedule(
        self, schedule_id: int, skip: int = 0, limit: int = 50
    ) -> List[ScheduleRunHistory]:
        """Return run history for a schedule ordered by triggered_at DESC."""
        return (
            self.db.query(ScheduleRunHistory)
            .filter(ScheduleRunHistory.schedule_id == schedule_id)
            .order_by(ScheduleRunHistory.triggered_at.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )

    def count_by_schedule(self, schedule_id: int) -> int:
        """Count total runs for a schedule."""
        return (
            self.db.query(ScheduleRunHistory)
            .filter(ScheduleRunHistory.schedule_id == schedule_id)
            .count()
        )
