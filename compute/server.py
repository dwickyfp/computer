"""
FastAPI server for Rosetta Compute Engine.
"""

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from core.runtime_health import overall_status
from core.runtime_metrics import snapshot as metrics_snapshot

logger = logging.getLogger(__name__)

app = FastAPI(title="Rosetta Compute Engine")


@app.get("/health")
async def health_check():
    """Health check endpoint backed by the runtime worker registry."""
    healthy, workers = overall_status()
    payload = {
        "status": "healthy" if healthy else "unhealthy",
        "workers": workers,
        "metrics": metrics_snapshot(),
    }
    if healthy:
        return payload
    return JSONResponse(status_code=503, content=payload)


@app.get("/health/pool")
async def pool_health():
    """Connection pool health endpoint."""
    from core.database import get_pool_health

    return get_pool_health()


def run_server(host: str, port: int) -> None:
    """Run FastAPI server using Uvicorn."""
    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    except Exception as exc:
        logger.error("Failed to start API server: %s", exc)
        raise
