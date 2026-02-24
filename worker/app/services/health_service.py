"""
Health check service for worker.

Provides health and readiness endpoints for monitoring.
"""

from typing import Any

from app.celery_app import celery_app
from app.core.database import get_db_session
from app.core.redis_client import get_redis

import structlog

logger = structlog.get_logger(__name__)


def get_health_status() -> dict[str, Any]:
    """
    Get worker health status.

    Checks:
    - Celery broker connectivity
    - Database connectivity
    - Redis connectivity

    Returns:
        Health status dict with component statuses
    """
    status = {
        "status": "healthy",
        "components": {},
    }

    # Check Celery broker
    try:
        inspector = celery_app.control.inspect(timeout=2.0)
        active = inspector.active()
        status["components"]["celery"] = {
            "status": "up",
            "active_workers": len(active) if active else 0,
        }
    except Exception as e:
        status["components"]["celery"] = {"status": "down", "error": str(e)}
        status["status"] = "degraded"

    # Check database
    try:
        with get_db_session() as session:
            from sqlalchemy import text
            session.execute(text("SELECT 1"))
        status["components"]["database"] = {"status": "up"}
    except Exception as e:
        status["components"]["database"] = {"status": "down", "error": str(e)}
        status["status"] = "unhealthy"

    # Check Redis
    try:
        redis_client = get_redis()
        if redis_client:
            redis_client.ping()
            status["components"]["redis"] = {"status": "up"}
        else:
            status["components"]["redis"] = {"status": "down", "error": "No connection"}
            status["status"] = "degraded"
    except Exception as e:
        status["components"]["redis"] = {"status": "down", "error": str(e)}
        status["status"] = "degraded"

    return status


def get_worker_stats() -> dict[str, Any]:
    """Get worker statistics from Celery inspect."""
    try:
        inspector = celery_app.control.inspect(timeout=2.0)
        return {
            "active": inspector.active() or {},
            "reserved": inspector.reserved() or {},
            "stats": inspector.stats() or {},
        }
    except Exception as e:
        logger.error("Failed to get worker stats", error=str(e))
        return {"error": str(e)}
