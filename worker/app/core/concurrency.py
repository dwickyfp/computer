"""
Concurrency controls for resource-intensive operations.

Limits concurrent DuckDB instances to prevent resource exhaustion.
With --pool=threads, each DuckDB connection uses duckdb_threads threads
and duckdb_memory_limit memory. Without a cap, 8 concurrent connections
could consume 32 threads and 16GB memory simultaneously.

IMPORTANT: Linked-task orchestration threads must NOT hold DuckDB slots
while waiting for child flow-task Celery results — doing so causes deadlock
when all slots are consumed by waiting parents and children can't acquire.
"""

import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Generator

import structlog

logger = structlog.get_logger(__name__)

_duckdb_semaphore: threading.Semaphore | None = None
_init_lock = threading.Lock()

# Track concurrency metrics for monitoring
_total_acquired = 0
_total_released = 0
_total_waited = 0
_total_memory_delays = 0
_metrics_lock = threading.Lock()

# Memory backpressure settings
_MEMORY_BACKPRESSURE_DELAY = 1.0  # seconds to wait when under memory pressure
_MEMORY_BACKPRESSURE_MAX_RETRIES = 5  # max retries before proceeding anyway


def _get_semaphore() -> threading.Semaphore:
    """Lazy-initialize the DuckDB concurrency semaphore."""
    global _duckdb_semaphore
    if _duckdb_semaphore is None:
        with _init_lock:
            if _duckdb_semaphore is None:
                from app.config.settings import get_settings

                settings = get_settings()
                max_concurrent = max(1, settings.duckdb_max_concurrent)
                _duckdb_semaphore = threading.Semaphore(max_concurrent)
                logger.info(
                    "DuckDB concurrency semaphore initialized",
                    max_concurrent=max_concurrent,
                    worker_concurrency=settings.worker_concurrency,
                )
    return _duckdb_semaphore


@contextmanager
def duckdb_slot() -> Generator[None, None, None]:
    """Context manager to acquire a DuckDB execution slot."""
    acquire_duckdb_slot()
    try:
        yield
    finally:
        release_duckdb_slot()


def acquire_duckdb_slot() -> None:
    """Acquire a DuckDB execution slot. Blocks if all slots are in use.

    Also applies memory backpressure: if current RSS exceeds the warning
    threshold, delays acquisition to allow in-flight tasks to complete
    and release memory before starting new ones.
    """
    global _total_acquired, _total_waited, _total_memory_delays

    # Memory backpressure: delay if under memory pressure
    for _ in range(_MEMORY_BACKPRESSURE_MAX_RETRIES):
        if not check_memory_pressure():
            break
        with _metrics_lock:
            _total_memory_delays += 1
        logger.warning(
            "Memory backpressure — delaying DuckDB slot acquisition",
            delay_seconds=_MEMORY_BACKPRESSURE_DELAY,
        )
        time.sleep(_MEMORY_BACKPRESSURE_DELAY)

    sem = _get_semaphore()
    if not sem.acquire(blocking=False):
        with _metrics_lock:
            _total_waited += 1
        logger.debug("DuckDB slot contention — waiting for available slot")
        sem.acquire()  # blocking wait
    with _metrics_lock:
        _total_acquired += 1


def release_duckdb_slot() -> None:
    """Release a previously acquired DuckDB execution slot."""
    global _total_released
    _get_semaphore().release()
    with _metrics_lock:
        _total_released += 1


def get_concurrency_metrics() -> dict:
    """Return concurrency metrics for health/monitoring endpoints."""
    with _metrics_lock:
        return {
            "total_acquired": _total_acquired,
            "total_released": _total_released,
            "total_waited": _total_waited,
            "total_memory_delays": _total_memory_delays,
            "slots_in_use": _total_acquired - _total_released,
            "current_rss_mb": round(get_memory_usage_mb(), 1),
        }


# ─── Memory watchdog ─────────────────────────────────────────────────────────

_MEMORY_WARNING_MB: int | None = None


def _get_memory_warning_mb() -> int:
    """Get memory warning threshold in MB (lazy-initialized)."""
    global _MEMORY_WARNING_MB
    if _MEMORY_WARNING_MB is None:
        from app.config.settings import get_settings
        settings = get_settings()
        _MEMORY_WARNING_MB = settings.memory_warning_mb
    return _MEMORY_WARNING_MB


def check_memory_pressure() -> bool:
    """Check if current process memory usage exceeds warning threshold.

    Returns True if memory is under pressure (above threshold).
    Logs a warning when threshold is crossed.

    Uses /proc/self/statm on Linux for accurate *current* RSS
    (not peak like ru_maxrss). Falls back to psutil or resource module.
    """
    try:
        current_mb = get_memory_usage_mb()
        if current_mb <= 0:
            return False

        threshold = _get_memory_warning_mb()
        if current_mb > threshold:
            logger.warning(
                "Memory pressure detected",
                rss_mb=round(current_mb, 1),
                threshold_mb=threshold,
            )
            return True
        return False
    except Exception:
        return False


def get_memory_usage_mb() -> float:
    """Return current RSS memory usage in MB (not peak).

    Priority:
    1. /proc/self/statm (Linux) — most accurate, no imports
    2. psutil (cross-platform) — if installed
    3. resource.getrusage — fallback (reports peak on macOS)
    """
    # Method 1: Linux /proc/self/statm (current RSS, not peak)
    try:
        if sys.platform == "linux":
            with open("/proc/self/statm", "r") as f:
                parts = f.read().split()
                # Second field is RSS in pages
                rss_pages = int(parts[1])
                page_size = os.sysconf("SC_PAGE_SIZE")
                return (rss_pages * page_size) / (1024 * 1024)
    except Exception:
        pass

    # Method 2: psutil (accurate current RSS, cross-platform)
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    except Exception:
        pass

    # Method 3: resource.getrusage fallback (peak RSS on macOS, current on Linux)
    try:
        import resource
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        if sys.platform == "darwin":
            # macOS reports bytes (peak RSS)
            return rusage.ru_maxrss / (1024 * 1024)
        # Linux reports KB
        return rusage.ru_maxrss / 1024
    except Exception:
        return 0.0
