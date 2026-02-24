"""
Health check endpoints.

Provides application health and status information.
"""

import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends

from app import __version__
from app.core.config import get_settings
from app.core.database import check_database_health
from app.domain.schemas.common import HealthResponse

import httpx
import asyncio
from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()

# No longer needed - worker health is read from DB (instant)
# Simple cache for overall health check results (3 second TTL)
_health_cache: Optional[HealthResponse] = None
_health_cache_time: float = 0
_health_cache_ttl: float = 2.0  # Reduced to 2s since checks are now fast


@router.get(
    "",
    response_model=HealthResponse,
    summary="Health check",
    description="Check application health and dependency status",
)
async def health_check() -> HealthResponse:
    """
    Check application health.

    Returns health status including database connectivity.
    All checks run in parallel with 1-second timeout for faster response.
    Uses 3-second cache to avoid excessive health checks.
    """
    global _health_cache, _health_cache_time
    
    # Return cached result if fresh
    now = time.time()
    if _health_cache and (now - _health_cache_time) < _health_cache_ttl:
        return _health_cache
    
    settings = get_settings()

    async def check_db() -> bool:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(check_database_health),
                timeout=1.0
            )
        except Exception as e:
            logger.warning(f"DB health check failed: {e}")
            return False

    async def check_redis() -> bool:
        try:
            from app.infrastructure.redis import RedisClient
            redis_client = RedisClient.get_instance()
            check = redis_client.ping()
            if asyncio.iscoroutinefunction(redis_client.ping):
                return await asyncio.wait_for(check, timeout=1.0)
            return check
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False

    async def check_compute() -> bool:
        try:
            url = f"{settings.compute_node_url}/health"
            async with httpx.AsyncClient(timeout=1.0) as client:
                response = await client.get(url)
                return response.status_code == 200 and response.json().get("status") == "healthy"
        except Exception as e:
            logger.debug(f"Compute health check failed: {e}")
            return False

    async def check_worker() -> tuple[bool, dict]:
        """Check worker health from database (populated by scheduler every 10s)."""
        if not settings.worker_enabled:
            return False, {}
        try:
            from app.domain.repositories.worker_health_repo import WorkerHealthRepository
            from app.core.database import db_manager
            
            def _read_worker_health():
                db = db_manager.session_factory()
                try:
                    repo = WorkerHealthRepository(db)
                    latest = repo.get_latest()
                    if not latest:
                        return False, {"error": "No worker health data yet"}
                    
                    # Check staleness (>30s = stale)
                    from datetime import datetime, timezone, timedelta
                    now = datetime.now(timezone(timedelta(hours=7)))
                    last_check = latest.last_check_at
                    # Ensure both are timezone-aware for comparison
                    if last_check.tzinfo is None:
                        last_check = last_check.replace(tzinfo=timezone(timedelta(hours=7)))
                    age = (now - last_check).total_seconds()
                    
                    if age > 30:
                        return False, {"error": f"Stale ({age:.0f}s ago)"}
                    
                    return latest.healthy, {}
                finally:
                    db.close()
            
            return await asyncio.to_thread(_read_worker_health)
        except Exception as e:
            logger.debug(f"Worker health DB read failed: {e}")
            return False, {"error": str(e)}

    # Run all checks in parallel
    db_healthy, redis_healthy, compute_healthy, (worker_healthy, worker_stats) = await asyncio.gather(
        check_db(),
        check_redis(),
        check_compute(),
        check_worker(),
        return_exceptions=False
    )

    # Determine overall status
    # We consider healthy if DB and Redis are up. Compute is optional for API but tracked.
    overall_status = "healthy" if db_healthy and redis_healthy else "unhealthy"

    result = HealthResponse(
        status=overall_status,
        version=__version__,
        timestamp=datetime.now(timezone(timedelta(hours=7))),
        checks={
            "database": db_healthy, 
            "redis": redis_healthy,
            "wal_monitor": settings.wal_monitor_enabled,
            "compute": compute_healthy,
            "worker": worker_healthy,
        },
    )
    
    # Cache the result
    _health_cache = result
    _health_cache_time = now
    return result


@router.get(
    "/worker",
    summary="Worker status",
    description="Get detailed Celery worker health and statistics",
)
async def worker_status() -> Dict[str, Any]:
    """
    Get detailed worker status including active workers,
    running tasks, and queue info.

    Returns disabled status if WORKER_ENABLED is false.
    """
    settings = get_settings()

    if not settings.worker_enabled:
        return {
            "enabled": False,
            "healthy": False,
            "active_workers": 0,
            "active_tasks": 0,
            "reserved_tasks": 0,
        }

    try:
        from app.domain.repositories.worker_health_repo import WorkerHealthRepository
        from app.core.database import db_manager
        
        def _read_worker_status():
            db = db_manager.session_factory()
            try:
                repo = WorkerHealthRepository(db)
                latest = repo.get_latest()
                
                if not latest:
                    return {
                        "enabled": True,
                        "healthy": False,
                        "active_workers": 0,
                        "active_tasks": 0,
                        "reserved_tasks": 0,
                        "error": "No health data yet. Scheduler will populate shortly.",
                    }
                
                # Check staleness
                from datetime import datetime, timezone, timedelta
                now = datetime.now(timezone(timedelta(hours=7)))
                last_check = latest.last_check_at
                # Ensure both are timezone-aware for comparison
                if last_check.tzinfo is None:
                    last_check = last_check.replace(tzinfo=timezone(timedelta(hours=7)))
                age = (now - last_check).total_seconds()
                
                result = {
                    "enabled": True,
                    "healthy": latest.healthy if age <= 30 else False,
                    "active_workers": latest.active_workers,
                    "active_tasks": latest.active_tasks,
                    "reserved_tasks": latest.reserved_tasks,
                    "last_check": latest.last_check_at.isoformat(),
                    "age_seconds": round(age, 1),
                }
                if latest.error_message:
                    result["error"] = latest.error_message
                if age > 30:
                    result["error"] = f"Data stale ({age:.0f}s old)"
                return result
            finally:
                db.close()
        
        return await asyncio.to_thread(_read_worker_status)
    except Exception as e:
        logger.warning(f"Worker status check failed: {e}")
        return {
            "enabled": True,
            "healthy": False,
            "active_workers": 0,
            "active_tasks": 0,
            "reserved_tasks": 0,
            "error": str(e),
        }
