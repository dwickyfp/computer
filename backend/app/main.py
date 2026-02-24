"""
Rosetta ETL Platform - FastAPI Application.

A production-ready FastAPI application for managing ETL pipeline configurations
with PostgreSQL WAL monitoring capabilities.
"""

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
from datetime import datetime


from app import __version__
from app.api.v1 import api_router
from app.core.config import get_settings
from app.core.database import check_database_health, db_manager
from app.core.exceptions import RosettaException
from app.core.logging import get_logger, setup_logging
from app.infrastructure.tasks.scheduler import BackgroundScheduler

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Get settings
settings = get_settings()

# Initialize background scheduler
background_scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events for the application.
    """
    # Startup
    logger.info(
        "Starting Rosetta ETL Platform",
        extra={"version": __version__, "environment": settings.app_env},
    )

    try:
        # Initialize database (sync operation)
        db_manager.initialize()
        logger.info("Database initialized successfully")

        # Start background scheduler
        background_scheduler.start()
        logger.info("Background scheduler started successfully")

        # Check database health in background to not block startup
        async def _startup_health_check():
            try:
                logger.info("Performing startup database health check...")
                db_healthy = await asyncio.to_thread(check_database_health)
                if not db_healthy:
                    logger.warning("Database health check failed during startup")
                else:
                    logger.info("Database health check passed")
            except Exception as e:
                logger.error("Error during startup health check", extra={"error": str(e)})

        asyncio.create_task(_startup_health_check())

        logger.info("Application startup completed successfully")

    except Exception as e:
        logger.error("Failed to start application", extra={"error": str(e)})
        raise

    yield

    # Shutdown
    logger.info("Shutting down Rosetta ETL Platform")

    try:
        # Stop background scheduler
        background_scheduler.stop()
        logger.info("Background scheduler stopped")

        # Close database connections
        db_manager.close()
        logger.info("Database connections closed")

        logger.info("Application shutdown completed successfully")

    except Exception as e:
        logger.error("Error during application shutdown", extra={"error": str(e)})


# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=__version__,
    description=(
        "A production-ready FastAPI application for managing ETL pipeline "
        "configurations with PostgreSQL WAL monitoring capabilities."
    ),
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    openapi_url="/openapi.json" if settings.debug else None,
    lifespan=lifespan,
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_origin_regex=r"https?://localhost:\d+",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(RosettaException)
async def rosetta_exception_handler(
    request: Request, exc: RosettaException
) -> JSONResponse:
    """
    Handle custom Rosetta exceptions.

    Returns standardized error response with appropriate HTTP status code.
    """
    logger.warning(
        f"Application exception: {exc.__class__.__name__}",
        extra={
            "error": exc.message,
            "path": request.url.path,
            "status_code": exc.status_code,
        },
    )

    return JSONResponse(status_code=exc.status_code, content=exc.to_dict())


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """
    Handle Pydantic validation errors.

    Returns detailed validation error information.
    """
    logger.warning(
        "Validation error", extra={"errors": exc.errors(), "path": request.url.path}
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "details": exc.errors(),
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle unexpected exceptions.

    Returns generic error response and logs exception details.
    """
    logger.error(
        "Unexpected exception",
        extra={"error": str(exc), "type": type(exc).__name__, "path": request.url.path},
        exc_info=True,
    )

    # Don't expose internal errors in production
    error_message = str(exc) if settings.debug else "Internal server error"

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": error_message,
            "details": {},
        },
    )


# Include API routers
app.include_router(api_router, prefix=settings.api_v1_prefix)


# Root endpoint
@app.get("/", tags=["root"], summary="Root endpoint", description="Get API information")
async def root():
    """
    Root endpoint.

    Returns basic API information.
    """
    return {
        "name": settings.app_name,
        "version": __version__,
        "environment": settings.app_env,
        "docs_url": f"{settings.api_v1_prefix}/docs" if settings.debug else None,
    }


async def check_compute_health() -> bool:
    """
    Check compute node health.
    """
    url = f"{settings.compute_node_url}/health"
    try:
        # TIMEOUT INCREASED to 5.0s to avoid flakiness
        async with httpx.AsyncClient(timeout=5.0) as client:
            logger.info(f"Checking compute health at: {url}")
            response = await client.get(url)
            is_healthy = response.status_code == 200 and response.json().get("status") == "healthy"
            if not is_healthy:
                logger.warning(f"Compute health check returned {response.status_code}: {response.text}")
            return is_healthy
    except Exception as e:
        logger.error(f"Compute health check failed for {url}: {e.__class__.__name__}: {e}")
        return False


# Health check endpoint (outside versioned API for monitoring)
@app.get(
    "/health",
    tags=["health"],
    summary="Health check",
    description="Check application health",
)
async def health_check():
    """
    Health check endpoint.

    Used by load balancers and monitoring systems.
    """
    db_healthy = await asyncio.to_thread(check_database_health)
    compute_healthy = await check_compute_health()
    
    # Overall is healthy if DB is healthy. Compute can be down without affecting API.
    # But usually we want to know if everything is up.
    overall_status = "healthy" if db_healthy else "unhealthy"

    return {
        "status": overall_status,
        "version": __version__,
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {
            "database": db_healthy,
            "compute": compute_healthy,
        },
    }


# ─── D5: WebSocket Real-Time Pipeline Metrics Stream ──────────────────────────

class MetricsConnectionManager:
    """Manages WebSocket connections for real-time metrics streaming."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(
            f"WebSocket client connected. Active: {len(self.active_connections)}"
        )

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(
            f"WebSocket client disconnected. Active: {len(self.active_connections)}"
        )

    async def broadcast(self, data: dict):
        """Send data to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception:
                disconnected.append(connection)
        for conn in disconnected:
            self.disconnect(conn)


metrics_manager = MetricsConnectionManager()


def _collect_pipeline_metrics() -> dict:
    """Collect current pipeline metrics from database (runs in thread)."""
    from app.core.database import get_db
    from sqlalchemy import text

    try:
        db = next(get_db())
        try:
            # Pipeline status counts
            status_rows = db.execute(
                text(
                    "SELECT status, COUNT(*) as count FROM pipeline_metadata "
                    "GROUP BY status"
                )
            ).fetchall()
            status_counts = {row.status: row.count for row in status_rows}

            # System metrics (latest)
            sys_row = db.execute(
                text(
                    "SELECT cpu_percent, memory_percent, created_at "
                    "FROM system_metrics ORDER BY created_at DESC LIMIT 1"
                )
            ).fetchone()

            system_metrics = {}
            if sys_row:
                system_metrics = {
                    "cpu_percent": sys_row.cpu_percent,
                    "memory_percent": sys_row.memory_percent,
                    "collected_at": sys_row.created_at.isoformat() if sys_row.created_at else None,
                }

            # WAL size
            wal_row = db.execute(
                text(
                    "SELECT config_value FROM rosetta_setting_configuration "
                    "WHERE config_key = 'CURRENT_WAL_SIZE'"
                )
            ).fetchone()
            wal_size = wal_row.config_value if wal_row else None

            # Error count (last hour)
            error_row = db.execute(
                text(
                    "SELECT COUNT(*) as cnt FROM notification_log "
                    "WHERE type = 'ERROR' AND created_at > NOW() - INTERVAL '1 hour'"
                )
            ).fetchone()
            recent_errors = error_row.cnt if error_row else 0

            return {
                "type": "pipeline_metrics",
                "timestamp": datetime.utcnow().isoformat(),
                "pipeline_status": status_counts,
                "system": system_metrics,
                "wal_size_mb": wal_size,
                "recent_errors": recent_errors,
            }
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"Failed to collect pipeline metrics: {e}")
        return {
            "type": "pipeline_metrics",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
        }


@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """
    WebSocket endpoint for real-time pipeline metrics (D5).

    Streams pipeline status, system metrics, and alerts every 5 seconds.
    Clients can send JSON messages to control the stream:
      {"action": "set_interval", "interval": 10}  — change push interval (seconds)
    """
    await metrics_manager.connect(websocket)
    push_interval = 5  # default 5 seconds

    try:
        while True:
            # Collect and send metrics
            metrics = await asyncio.to_thread(_collect_pipeline_metrics)
            await websocket.send_json(metrics)

            # Wait for interval, but also listen for client messages
            try:
                msg = await asyncio.wait_for(
                    websocket.receive_text(), timeout=push_interval
                )
                try:
                    parsed = json.loads(msg)
                    if parsed.get("action") == "set_interval":
                        new_interval = int(parsed.get("interval", 5))
                        push_interval = max(1, min(60, new_interval))
                        await websocket.send_json({
                            "type": "config_ack",
                            "interval": push_interval,
                        })
                except (json.JSONDecodeError, ValueError):
                    pass
            except asyncio.TimeoutError:
                pass  # Just continue to next push cycle

    except WebSocketDisconnect:
        metrics_manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
        metrics_manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
