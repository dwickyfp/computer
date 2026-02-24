"""
DynamicSchedulerService — runtime APScheduler job management for user-defined schedules.

Wraps the live BackgroundScheduler (APSBackgroundScheduler) instance and
provides register/unregister/reload helpers that are called by ScheduleService
on every CRUD mutation.

Job ID namespace: "user_schedule_{schedule_id}" — avoids collision with all
existing system jobs (wal_monitor, replication_monitor, etc.)

Execution model:
  _execute_schedule() opens its own DB session (same pattern as _run_* in
  scheduler.py), calls FlowTaskService.trigger_run() or LinkedTaskService.trigger_run(),
  writes a ScheduleRunHistory row, and always closes the session in finally.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def _execute_schedule(schedule_id: int) -> None:
    """
    APScheduler job function — called on every cron tick for a user schedule.

    Opens its own DB session, dispatches the target task, writes run history.
    """
    from app.core.database import db_manager
    from app.domain.models.schedule import Schedule, ScheduleRunStatus
    from app.domain.repositories.schedule import (
        ScheduleRepository,
        ScheduleRunHistoryRepository,
    )

    session_factory = db_manager.session_factory
    db = session_factory()
    run_id: Optional[int] = None
    start_time = datetime.now(ZoneInfo("Asia/Jakarta"))

    try:
        # ------------------------------------------------------------------
        # 1. Load the schedule
        # ------------------------------------------------------------------
        schedule_repo = ScheduleRepository(db)
        run_history_repo = ScheduleRunHistoryRepository(db)

        schedule: Optional[Schedule] = (
            db.query(Schedule).filter(Schedule.id == schedule_id).first()
        )
        if schedule is None:
            logger.warning(
                f"[DynamicScheduler] Schedule {schedule_id} not found — skipping run"
            )
            return
        if schedule.status != "ACTIVE":
            logger.info(
                f"[DynamicScheduler] Schedule {schedule_id} is PAUSED — skipping run"
            )
            return

        # ------------------------------------------------------------------
        # 2. Create RUNNING history row
        # ------------------------------------------------------------------
        run = run_history_repo.create_run(
            schedule.id, schedule.task_type, schedule.task_id
        )
        db.commit()
        run_id = run.id

        # ------------------------------------------------------------------
        # 3. Dispatch task
        # ------------------------------------------------------------------
        task_type = schedule.task_type
        task_id = schedule.task_id

        if task_type == "FLOW_TASK":
            from app.domain.services.flow_task import FlowTaskService

            FlowTaskService(db).trigger_run(task_id, trigger_type="SCHEDULED")
        elif task_type == "LINKED_TASK":
            from app.domain.services.linked_task import LinkedTaskService

            LinkedTaskService(db).trigger_run(task_id)
        else:
            raise ValueError(f"Unknown task_type: {task_type}")

        # ------------------------------------------------------------------
        # 4. Mark SUCCESS
        # ------------------------------------------------------------------
        duration_ms = int(
            (datetime.now(ZoneInfo("Asia/Jakarta")) - start_time).total_seconds() * 1000
        )
        run_history_repo.complete_run(
            run_id, ScheduleRunStatus.SUCCESS, None, duration_ms
        )
        schedule_repo.update_last_run_at(schedule_id)
        db.commit()

        logger.info(
            f"[DynamicScheduler] Schedule {schedule_id} run {run_id} completed "
            f"({task_type} #{task_id}) in {duration_ms}ms"
        )

    except Exception as exc:
        if isinstance(exc, ValueError) and "already running" in str(exc):
            logger.info(f"[DynamicScheduler] Schedule {schedule_id} run skipped: {exc}")
        else:
            logger.error(f"[DynamicScheduler] Schedule {schedule_id} run failed: {exc}")
        try:
            if run_id is not None:
                from app.domain.models.schedule import ScheduleRunStatus
                from app.domain.repositories.schedule import (
                    ScheduleRunHistoryRepository as _Repo,
                )

                duration_ms = int(
                    (
                        datetime.now(ZoneInfo("Asia/Jakarta")) - start_time
                    ).total_seconds()
                    * 1000
                )
                _Repo(db).complete_run(
                    run_id, ScheduleRunStatus.FAILED, str(exc), duration_ms
                )
                db.commit()
        except Exception as inner:
            logger.error(
                f"[DynamicScheduler] Failed to write FAILED status for run {run_id}: {inner}"
            )
            db.rollback()
    finally:
        db.close()


class DynamicSchedulerService:
    """
    Singleton service that manages runtime APScheduler jobs for user schedules.

    Lifecycle:
      1. BackgroundScheduler.start() calls set_scheduler() then load_all_from_db()
      2. ScheduleService calls register/unregister/reload on CRUD mutations
    """

    def __init__(self) -> None:
        self._scheduler = None  # APSBackgroundScheduler instance

    def set_scheduler(self, scheduler) -> None:
        """Inject the live APSBackgroundScheduler instance."""
        self._scheduler = scheduler

    @property
    def _ready(self) -> bool:
        return self._scheduler is not None and self._scheduler.running

    def _job_id(self, schedule_id: int) -> str:
        return f"user_schedule_{schedule_id}"

    def register(self, schedule) -> None:
        """
        Add (or replace) an APScheduler CronTrigger job for the given schedule.

        Safe to call even if the job already exists (replace_existing=True).
        Only registers if status is ACTIVE.
        """
        if not self._ready:
            logger.warning(
                f"[DynamicScheduler] Scheduler not ready — skipping register for schedule {schedule.id}"
            )
            return
        if schedule.status != "ACTIVE":
            return

        from apscheduler.triggers.cron import CronTrigger

        try:
            parts = schedule.cron_expression.split()
            if len(parts) != 5:
                raise ValueError(f"Invalid cron expression: {schedule.cron_expression}")

            minute, hour, day, month, day_of_week = parts
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=ZoneInfo("Asia/Jakarta"),
            )

            self._scheduler.add_job(
                func=_execute_schedule,
                trigger=trigger,
                args=[schedule.id],
                id=self._job_id(schedule.id),
                name=f"Schedule: {schedule.name}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
            )

            # Update next_run_at in background (best-effort)
            self._refresh_next_run_at(schedule.id)

            logger.info(
                f"[DynamicScheduler] Registered job {self._job_id(schedule.id)} "
                f"cron='{schedule.cron_expression}'"
            )
        except Exception as exc:
            logger.error(
                f"[DynamicScheduler] Failed to register schedule {schedule.id}: {exc}"
            )

    def unregister(self, schedule_id: int) -> None:
        """Remove the APScheduler job for a schedule. No-op if job doesn't exist."""
        if not self._ready:
            return
        job_id = self._job_id(schedule_id)
        try:
            if self._scheduler.get_job(job_id):
                self._scheduler.remove_job(job_id)
                logger.info(f"[DynamicScheduler] Unregistered job {job_id}")
        except Exception as exc:
            logger.warning(
                f"[DynamicScheduler] Could not unregister job {job_id}: {exc}"
            )

    def reload(self, schedule) -> None:
        """Remove the old job, then re-register with the updated schedule."""
        self.unregister(schedule.id)
        self.register(schedule)

    def load_all_from_db(self, session_factory) -> None:
        """
        Called once at startup after the scheduler is running.
        Loads all ACTIVE schedules from DB and registers them.
        """
        from app.domain.models.schedule import Schedule

        db = session_factory()
        try:
            active_schedules = (
                db.query(Schedule).filter(Schedule.status == "ACTIVE").all()
            )
            for schedule in active_schedules:
                self.register(schedule)
            logger.info(
                f"[DynamicScheduler] Loaded {len(active_schedules)} active schedule(s) from DB"
            )
        except Exception as exc:
            logger.error(f"[DynamicScheduler] Failed to load schedules from DB: {exc}")
        finally:
            db.close()

    def get_next_run_time(self, schedule_id: int) -> Optional[datetime]:
        """Return the next scheduled run time for a given schedule."""
        if not self._ready:
            return None
        job_id = self._job_id(schedule_id)
        job = self._scheduler.get_job(job_id)
        return job.next_run_time if job else None

    def _refresh_next_run_at(self, schedule_id: int) -> None:
        """Best-effort: update next_run_at column in DB from APScheduler."""
        try:
            from app.core.database import db_manager
            from app.domain.repositories.schedule import ScheduleRepository

            next_run = self.get_next_run_time(schedule_id)
            if next_run is None:
                return
            db = db_manager.session_factory()
            try:
                ScheduleRepository(db).update_next_run_at(schedule_id, next_run)
                db.commit()
            finally:
                db.close()
        except Exception:
            pass  # Non-critical — next_run_at is informational only


# Module-level singleton — imported everywhere
dynamic_scheduler_service = DynamicSchedulerService()
