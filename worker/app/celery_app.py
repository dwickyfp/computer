"""
Celery application factory and configuration.

Configures Celery with Redis broker and PostgreSQL result backend.
"""

from celery import Celery
from celery.signals import worker_init

from app.config.settings import get_settings
from app.core.logging import setup_logging

# Initialize logging
setup_logging()

settings = get_settings()

# Create Celery app
celery_app = Celery(
    "rosetta_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

# Celery Configuration
celery_app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Time limits
    task_soft_time_limit=settings.task_soft_time_limit,
    task_time_limit=settings.task_hard_time_limit,
    # Result settings
    result_expires=600,  # Results expire after 10 minutes (reduces Redis memory)
    result_extended=True,  # Store task args, name, etc.
    # Task routing
    task_routes={
        "worker.preview.execute": {"queue": "preview"},
        "worker.flow_task.preview": {"queue": "preview"},
        "worker.linked_task.execute": {"queue": "orchestration"},
    },
    # Default queue
    task_default_queue="default",
    # Worker settings - HIGH PERFORMANCE
    worker_concurrency=settings.worker_concurrency,
    worker_prefetch_multiplier=1,  # Keep low for CPU/memory-intensive DuckDB tasks
    # Note: worker_max_tasks_per_child has no effect with --pool=threads
    # Task behavior
    task_acks_late=True,  # Ack after task completes (crash safety)
    task_reject_on_worker_lost=True,  # Re-queue on worker crash (overridden per-task for flow/linked)
    task_track_started=True,  # Track STARTED state
    # Broker settings - HIGH PERFORMANCE
    broker_pool_limit=10,  # Connection pool to Redis (1 per worker thread + headroom)
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": 3600,  # 1 hour task visibility
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
    },
    # Result backend settings
    result_backend_transport_options={
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
    },
    # Timezone
    timezone="UTC",
    enable_utc=True,
)

# Auto-discover tasks from the tasks package
celery_app.autodiscover_tasks([
    "app.tasks.preview",
    "app.tasks.lineage",
    "app.tasks.flow_task",
    "app.tasks.destination_table_list",
    "app.tasks.linked_task",
])


# ─── Pre-install DuckDB extensions once at worker startup ─────────────────────

@worker_init.connect
def _preinstall_duckdb_extensions(**kwargs):
    """
    Install DuckDB extensions once when the worker process starts,
    so individual tasks only need LOAD (fast) instead of INSTALL+LOAD.
    """
    import structlog
    _logger = structlog.get_logger("celery.worker_init")

    try:
        import duckdb
        con = duckdb.connect(":memory:")
        # Limit memory during extension install to avoid transient spike
        con.execute("SET memory_limit='256MB';")
        for ext in ("postgres", "httpfs", "spatial"):
            try:
                con.execute(f"INSTALL {ext};")
                _logger.info("duckdb_extension_installed", ext=ext)
            except Exception as e:
                _logger.warning("duckdb_extension_install_skipped", ext=ext, err=str(e))
        for ext in ("snowflake",):
            try:
                con.execute(f"INSTALL {ext} FROM community;")
                _logger.info("duckdb_community_extension_installed", ext=ext)
            except Exception as e:
                _logger.warning("duckdb_community_extension_install_skipped", ext=ext, err=str(e))
        con.close()
    except Exception as e:
        _logger.error("duckdb_preinstall_failed", err=str(e))
