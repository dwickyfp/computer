"""
Background task scheduler.

Manages scheduling and execution of background tasks like WAL monitoring.
"""

import asyncio
import threading
from typing import Optional

from apscheduler.schedulers.background import (
    BackgroundScheduler as APSBackgroundScheduler,
)
from apscheduler.triggers.interval import IntervalTrigger

from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.services.wal_monitor import WALMonitorService
from app.domain.services.replication_monitor import ReplicationMonitorService
from app.domain.services.schema_monitor import SchemaMonitorService
from app.domain.services.credit_monitor import CreditMonitorService
from zoneinfo import ZoneInfo

logger = get_logger(__name__)


class BackgroundScheduler:
    """
    Background task scheduler.

    Manages periodic execution of background tasks using APScheduler.
    """

    def __init__(self):
        """Initialize background scheduler."""
        self.settings = get_settings()
        self.scheduler: Optional[APSBackgroundScheduler] = None
        self.wal_monitor: Optional[WALMonitorService] = None
        self.replication_monitor: Optional[ReplicationMonitorService] = None
        self.schema_monitor: Optional[SchemaMonitorService] = None
        self.credit_monitor: Optional[CreditMonitorService] = None

        # Reusable event loop for async monitor tasks (avoids creating/destroying
        # a new loop on every scheduler invocation via asyncio.run())
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None

        # Persistent httpx client for worker health checks (avoids TCP churn)
        self._httpx_client = None

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create a persistent event loop running in a background thread."""
        if self._async_loop is None or self._async_loop.is_closed():
            self._async_loop = asyncio.new_event_loop()
            self._async_thread = threading.Thread(
                target=self._async_loop.run_forever,
                daemon=True,
                name="scheduler-async-loop",
            )
            self._async_thread.start()
            logger.info("Scheduler async event loop started")
        return self._async_loop

    def _run_async(self, coro) -> None:
        """Run an async coroutine on the shared event loop (non-blocking to caller)."""
        loop = self._get_event_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        # Wait for completion (blocks the APScheduler thread, which is expected)
        future.result(timeout=120)

    def _get_httpx_client(self):
        """Get or create a persistent httpx client with connection keep-alive."""
        if self._httpx_client is None:
            import httpx
            self._httpx_client = httpx.Client(
                timeout=10.0,
                limits=httpx.Limits(max_connections=5, max_keepalive_connections=2),
            )
        return self._httpx_client

    def _record_job_metric(self, key: str, db=None) -> None:
        """
        Record job execution time.

        Args:
            key: Job identifier
            db: Optional existing DB session to reuse (avoids opening a new one)
        """
        try:
            from app.domain.repositories.job_metric import JobMetricRepository
            from datetime import datetime, timezone, timedelta

            # User requested Asia/Jakarta (UTC+7)
            jakarta_tz = timezone(timedelta(hours=7))

            if db is not None:
                # Reuse existing session — caller manages commit/close
                try:
                    repo = JobMetricRepository(db)
                    repo.upsert_metric(key, datetime.now(jakarta_tz))
                    db.commit()
                except Exception as e:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    logger.error(f"Failed to record job metric for {key}: {e}")
            else:
                # Open a dedicated session (standalone invocation)
                from app.core.database import db_manager
                session = db_manager.session_factory()
                try:
                    repo = JobMetricRepository(session)
                    repo.upsert_metric(key, datetime.now(jakarta_tz))
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to record job metric for {key}: {e}")
                finally:
                    session.close()
        except Exception as e:
            logger.error(f"Error in metric recording wrapper for {key}: {e}")

    def _run_wal_monitor(self) -> None:
        """
        Synchronous wrapper for async WAL monitor task.
        Uses shared event loop to avoid creating/destroying loops per invocation.
        """
        try:
            self._run_async(self.wal_monitor.monitor_all_sources())
            self._record_job_metric("wal_monitor")
        except Exception as e:
            logger.error("Error running WAL monitor task", extra={"error": str(e)})

    def _run_replication_monitor(self) -> None:
        """
        Synchronous wrapper for replication monitor task.
        Uses shared event loop.
        """
        try:
            if self.replication_monitor:
                self._run_async(self.replication_monitor.monitor_all_sources())
                self._record_job_metric("replication_monitor")
        except Exception as e:
            logger.error(
                "Error running replication monitor task", extra={"error": str(e)}
            )

    def _run_schema_monitor(self) -> None:
        """
        Synchronous wrapper for schema monitor task.
        Uses shared event loop.
        """
        try:
            if self.schema_monitor:
                self._run_async(self.schema_monitor.monitor_all_sources())
                self._record_job_metric("schema_monitor")
        except Exception as e:
            logger.error("Error running schema monitor task", extra={"error": str(e)})

    def _run_credit_monitor(self) -> None:
        """
        Synchronous wrapper for credit monitor task.
        """
        try:
            if self.credit_monitor:
                self.credit_monitor.monitor_all_destinations()
                self._record_job_metric("credit_monitor")
        except Exception as e:
            logger.error("Error running credit monitor task", extra={"error": str(e)})

    def _run_table_list_refresh(self) -> None:
        """
        Synchronous wrapper for table list refresh task.
        Uses single DB session for both work and job metric recording.
        """
        try:
            from app.core.database import db_manager
            from app.domain.services.source import SourceService

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                service = SourceService(db)
                sources = service.list_sources(limit=1000)
                for source in sources:
                    try:
                        service.refresh_available_tables(source.id)
                    except Exception as e:
                        logger.error(
                            f"Failed to auto-refresh tables for source {source.id}: {e}"
                        )
                self._record_job_metric("table_list_refresh", db=db)
            finally:
                db.close()

        except Exception as e:
            logger.error(
                "Error running table list refresh task", extra={"error": str(e)}
            )

    def _run_destination_table_list_refresh(self) -> None:
        """
        Synchronous wrapper for destination table list refresh task.
        Dispatches a Celery task per destination to the worker.
        Uses single DB session for both work and job metric recording.
        """
        try:
            from app.core.database import db_manager
            from app.domain.services.destination import DestinationService

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                service = DestinationService(db)
                service.refresh_table_list_all()
                self._record_job_metric("destination_table_list_refresh", db=db)
            finally:
                db.close()

        except Exception as e:
            logger.error(
                "Error running destination table list refresh task",
                extra={"error": str(e)},
            )

    def _run_system_metric_collection(self) -> None:
        """
        Synchronous wrapper for system metric collection task.
        Uses single DB session for both metric collection and job metric recording.
        """
        try:
            from app.core.database import db_manager
            from app.domain.services.system_metric import SystemMetricService

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                service = SystemMetricService(db)
                service.collect_and_save_metrics()
                # Batch: record job metric in same session
                self._record_job_metric("system_metric_collection", db=db)
            finally:
                db.close()
        except Exception as e:
            logger.error(
                "Error running system metric collection task", extra={"error": str(e)}
            )

    def _run_notification_sender(self) -> None:
        """
        Synchronous wrapper for notification sender task.
        Uses single DB session for both work and job metric recording.
        """
        try:
            from app.core.database import db_manager
            from app.domain.services.notification_service import NotificationService

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                service = NotificationService(db)
                service.process_pending_notifications()
                self._record_job_metric("notification_sender", db=db)
            finally:
                db.close()
        except Exception as e:
            logger.error(
                "Error running notification sender task", extra={"error": str(e)}
            )

    def _run_worker_health_check(self) -> None:
        """
        Check Celery worker health via HTTP and save to database.
        Runs every 10 seconds to keep status fresh.
        Uses persistent httpx client to avoid TCP connection churn.
        """
        try:
            from app.core.database import db_manager
            from app.domain.repositories.worker_health_repo import (
                WorkerHealthRepository,
            )

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                # Check if worker is enabled
                if not self.settings.worker_enabled:
                    return

                # Check worker health via persistent HTTP client
                url = f"{self.settings.worker_health_url}/health"
                client = self._get_httpx_client()
                response = client.get(url)

                repo = WorkerHealthRepository(db)

                if response.status_code == 200:
                    data = response.json()
                    repo.upsert_status(
                        healthy=data.get("healthy", False),
                        active_workers=data.get("active_workers", 0),
                        active_tasks=data.get("active_tasks", 0),
                        reserved_tasks=data.get("reserved_tasks", 0),
                        error_message=data.get("error"),
                        extra_data=data,
                    )
                else:
                    repo.upsert_status(
                        healthy=False,
                        error_message=f"HTTP {response.status_code}",
                    )

                # Batch metric in same session
                self._record_job_metric("worker_health_check", db=db)
            finally:
                db.close()
        except Exception as e:
            # If HTTP call fails, save unhealthy status (quietly - don't spam logs)
            try:
                from app.core.database import db_manager
                from app.domain.repositories.worker_health_repo import (
                    WorkerHealthRepository,
                )

                db2 = db_manager.session_factory()
                try:
                    repo = WorkerHealthRepository(db2)
                    repo.upsert_status(
                        healthy=False,
                        error_message=str(e),
                    )
                finally:
                    db2.close()
            except Exception:
                pass
            # Use debug level to avoid spamming logs when worker is down
            logger.debug(
                "Worker health check failed (worker may be offline)",
                extra={"error": str(e)},
            )

    def _run_pipeline_refresh_check(self) -> None:
        """
        Synchronous wrapper for pipeline refresh check task.
        Uses single DB session for both work and job metric recording.
        """
        try:
            from app.core.database import db_manager
            from app.domain.models.pipeline import Pipeline, PipelineStatus
            from datetime import datetime, timezone

            session_factory = db_manager.session_factory
            db = session_factory()
            try:
                # Find pipelines ready for refresh
                pipelines = (
                    db.query(Pipeline).filter(Pipeline.ready_refresh == True).all()
                )

                for pipeline in pipelines:
                    try:
                        pipeline.status = PipelineStatus.REFRESH.value
                        pipeline.ready_refresh = False
                        pipeline.last_refresh_at = datetime.now(timezone.utc)
                        logger.info(
                            f"Auto-refreshing pipeline {pipeline.id} ({pipeline.name})"
                        )
                    except Exception as e:
                        logger.error(f"Failed to refresh pipeline {pipeline.id}: {e}")

                if pipelines:
                    db.commit()

                self._record_job_metric("pipeline_refresh_check", db=db)
            finally:
                db.close()
        except Exception as e:
            logger.error(
                "Error running pipeline refresh check task", extra={"error": str(e)}
            )

    def start(self) -> None:
        """
        Start the background scheduler.

        Initializes and starts all scheduled tasks.
        """
        if not self.settings.background_task_enabled:
            logger.info("Background tasks are disabled in configuration")
            return

        logger.info("Starting background task scheduler")

        # Initialize scheduler
        self.scheduler = APSBackgroundScheduler(
            timezone=self.settings.scheduler_timezone
        )

        # Initialize WAL monitor
        if self.settings.wal_monitor_enabled:
            self.wal_monitor = WALMonitorService()

            # Schedule WAL monitoring task
            self.scheduler.add_job(
                self._run_wal_monitor,  # Use synchronous wrapper
                trigger=IntervalTrigger(
                    seconds=self.settings.wal_monitor_interval_seconds
                ),
                id="wal_monitor",
                name="PostgreSQL WAL Monitor",
                replace_existing=True,
                max_instances=1,  # Prevent concurrent executions
                coalesce=True,  # Combine missed executions
            )

            logger.info(
                "WAL monitoring scheduled",
                extra={"interval_seconds": self.settings.wal_monitor_interval_seconds},
            )

        # Initialize Replication monitor
        self.replication_monitor = ReplicationMonitorService()

        # Schedule Replication monitoring task
        self.scheduler.add_job(
            self._run_replication_monitor,
            trigger=IntervalTrigger(seconds=60),  # Default to 60s
            id="replication_monitor",
            name="Replication Monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Initialize Schema monitor
        self.schema_monitor = SchemaMonitorService()

        # Schedule Schema monitoring task
        self.scheduler.add_job(
            self._run_schema_monitor,
            trigger=IntervalTrigger(seconds=60),  # Default to 60s
            id="schema_monitor",
            name="Schema Monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Initialize Credit monitor
        self.credit_monitor = CreditMonitorService()

        # Schedule Credit monitoring task (every 1 hour)
        self.scheduler.add_job(
            self._run_credit_monitor,
            trigger=IntervalTrigger(hours=1),
            id="credit_monitor",
            name="Credit Monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Schedule Table List Refresh (every 10 minutes)
        self.scheduler.add_job(
            self._run_table_list_refresh,
            trigger=IntervalTrigger(minutes=5),
            id="table_list_refresh",
            name="Table List Refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Schedule Destination Table List Refresh (every 30 minutes, via worker)
        self.scheduler.add_job(
            self._run_destination_table_list_refresh,
            trigger=IntervalTrigger(minutes=30),
            id="destination_table_list_refresh",
            name="Destination Table List Refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Schedule System Metric Collection (every 15 seconds)
        # 5s was too aggressive — produces ~17k rows/day with minimal value
        self.scheduler.add_job(
            self._run_system_metric_collection,
            trigger=IntervalTrigger(seconds=15),
            id="system_metric_collection",
            name="System Metric Collection",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Schedule Notification Sender (every 30 seconds)
        self.scheduler.add_job(
            self._run_notification_sender,
            trigger=IntervalTrigger(seconds=30),
            id="notification_sender",
            name="Notification Sender",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Schedule Worker Health Check (every 10 seconds)
        if self.settings.worker_enabled:
            self.scheduler.add_job(
                self._run_worker_health_check,
                trigger=IntervalTrigger(seconds=10),
                id="worker_health_check",
                name="Worker Health Check",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
            logger.info("Worker health check scheduled (every 10 seconds)")

        logger.info(
            "Replication, Credit, Table Refresh, and System Metrics monitoring scheduled"
        )

        # Schedule Pipeline Refresh Check (every 10 seconds)
        self.scheduler.add_job(
            self._run_pipeline_refresh_check,
            trigger=IntervalTrigger(seconds=10),
            id="pipeline_refresh_check",
            name="Pipeline Refresh Check",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Start scheduler
        self.scheduler.start()
        logger.info("Background task scheduler started successfully")

        # Load user-defined schedules from DB and register as CronTrigger jobs
        # This must run AFTER self.scheduler.start() so the scheduler is live
        try:
            from app.infrastructure.tasks.dynamic_scheduler import (
                dynamic_scheduler_service,
            )
            from app.core.database import db_manager

            dynamic_scheduler_service.set_scheduler(self.scheduler)
            dynamic_scheduler_service.load_all_from_db(db_manager.session_factory)
            logger.info("Dynamic user schedules loaded and registered")
        except Exception as exc:
            logger.error(f"Failed to load dynamic user schedules: {exc}")

    def stop(self) -> None:
        """
        Stop the background scheduler.

        Gracefully shuts down all scheduled tasks.
        """
        logger.info("Stopping background task scheduler")

        if self.scheduler:
            self.scheduler.shutdown(wait=True)
            self.scheduler = None

        if self.wal_monitor:
            self.wal_monitor.stop()
            self.wal_monitor = None

        if self.replication_monitor:
            self.replication_monitor.stop()
            self.replication_monitor = None

        if self.schema_monitor:
            self.schema_monitor.stop()
            self.schema_monitor = None

        if self.credit_monitor:
            self.credit_monitor = (
                None  # No stop method needed strictly but consistent cleanup
            )

        logger.info("Background task scheduler stopped")

    def get_job_status(self) -> dict:
        """
        Get status of all scheduled jobs.

        Returns:
            Dictionary with job status information
        """
        if not self.scheduler:
            return {"status": "not_running", "jobs": []}

        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": (
                        str(job.next_run_time) if job.next_run_time else None
                    ),
                }
            )

        return {
            "status": "running" if self.scheduler.running else "stopped",
            "jobs": jobs,
        }
