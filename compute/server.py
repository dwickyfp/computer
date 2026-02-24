"""
FastAPI Server for Rosetta Compute Engine.

Provides health check and other API endpoints.
"""

import logging
import uvicorn
from fastapi import FastAPI
from config.config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(title="Rosetta Compute Engine")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/health/pool")
async def pool_health():
    """Connection pool health endpoint (C3)."""
    from core.database import get_pool_health
    return get_pool_health()


def run_server(host: str, port: int) -> None:
    """
    Run FastAPI server using Uvicorn.

    Args:
        host: Host to bind to
        port: Port to bind to
    """
    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    except Exception as e:
        logger.error(f"Failed to start API server: {e}")
