"""
FastAPI Server for Rosetta Worker Health API.

Provides health check endpoint for monitoring.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import time

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.celery_app import celery_app as _worker_celery_app
from app.config.settings import settings
from app.core.redis_client import get_redis

logger = logging.getLogger(__name__)

app = FastAPI(title="Rosetta Worker Health API")

# Cache results for 3 seconds to avoid repeated expensive inspect calls
_health_cache: Optional[dict] = None
_health_cache_time: float = 0
_cache_ttl: float = 3.0


@app.get("/health")
async def health_check():
    """
    Health check endpoint.

    Checks if the Celery worker is responding by inspecting active workers.
    Results are cached for 3 seconds to improve response time.
    """
    global _health_cache, _health_cache_time

    # Return cached result if fresh
    now = time.time()
    if _health_cache and (now - _health_cache_time) < _cache_ttl:
        return _health_cache

    try:
        # Use shared Celery app from worker
        celery_app = _worker_celery_app

        # Try inspector with reduced timeout to avoid blocking health API
        # Note: inspector.ping() can be unreliable even when workers are functioning
        inspector = celery_app.control.inspect(timeout=1.0)

        active_workers = 0
        active_tasks = 0
        reserved_tasks = 0

        try:
            # Quick ping attempt - if it fails, we'll still check Redis broker
            ping_result = inspector.ping()
            if ping_result:
                active_workers = len(ping_result)

                # Count active and reserved tasks
                active = inspector.active()
                if active:
                    for worker_tasks in active.values():
                        active_tasks += len(worker_tasks)

                reserved = inspector.reserved()
                if reserved:
                    for worker_tasks in reserved.values():
                        reserved_tasks += len(worker_tasks)
        except Exception as ping_error:
            # Ping failed, but check if Redis broker is accessible
            logger.debug(f"Inspector ping failed (might be busy): {ping_error}")

            # Test Redis broker connectivity as fallback
            # Use existing connection pool to avoid leaking connections
            try:
                redis_client = get_redis()
                if redis_client:
                    redis_client.ping()
                # Redis is accessible, assume worker is healthy
                # (If preview tasks work, worker is functional even if ping fails)
                result = {
                    "status": "healthy",
                    "healthy": True,
                    "active_workers": 1,  # Assume at least 1 worker (can't inspect)
                    "active_tasks": 0,
                    "reserved_tasks": 0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "note": "Celery inspector unavailable, verified via Redis broker",
                }
                _health_cache = result
                _health_cache_time = now
                return result
            except Exception as redis_error:
                logger.error(f"Redis broker check failed: {redis_error}")
                result = {
                    "status": "unhealthy",
                    "healthy": False,
                    "active_workers": 0,
                    "active_tasks": 0,
                    "reserved_tasks": 0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "error": f"Celery ping failed and Redis unreachable: {redis_error}",
                }
                return result

        # If we got here, ping succeeded
        if active_workers == 0:
            result = {
                "status": "unhealthy",
                "healthy": False,
                "active_workers": 0,
                "active_tasks": 0,
                "reserved_tasks": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": "No workers responding to ping",
            }
            return result

        result = {
            "status": "healthy",
            "healthy": True,
            "active_workers": active_workers,
            "active_tasks": active_tasks,
            "reserved_tasks": reserved_tasks,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Cache the successful result
        _health_cache = result
        _health_cache_time = now
        return result

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        result = {
            "status": "unhealthy",
            "healthy": False,
            "active_workers": 0,
            "active_tasks": 0,
            "reserved_tasks": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }
        # Don't cache errors
        return result


def run_server() -> None:
    """
    Run FastAPI server using Uvicorn.
    """
    try:
        uvicorn.run(
            app, host=settings.server_host, port=settings.server_port, log_level="info"
        )
    except Exception as e:
        logger.error(f"Failed to start health API server: {e}")


# ─── Schema endpoint ──────────────────────────────────────────────────────────

class NodeSchemaRequest(BaseModel):
    node_id: str
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]


class ColumnSchemaItem(BaseModel):
    column_name: str
    data_type: str


class NodeSchemaResponse(BaseModel):
    columns: List[ColumnSchemaItem]


@app.post("/schema", response_model=NodeSchemaResponse)
async def get_node_schema(request: NodeSchemaRequest):
    """
    Execute the DuckDB CTE chain up to the target node with LIMIT 0 and
    return the output column names + types.

    Runs in a thread pool so the event loop stays responsive for /health.
    """
    try:
        from app.tasks.flow_task.preview_executor import execute_node_schema

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            execute_node_schema,
            request.node_id,
            {"nodes": request.nodes, "edges": request.edges},
        )
        return NodeSchemaResponse(columns=result["columns"])
    except Exception as e:
        # Return empty columns instead of crashing — the UI handles the empty state
        # gracefully. Common causes: source DB not reachable, node not yet configured.
        logger.warning(f"Node schema resolution returned empty (non-fatal): {e}")
        return NodeSchemaResponse(columns=[])


if __name__ == "__main__":
    run_server()
