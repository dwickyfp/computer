"""
Application middleware for cross-cutting concerns.

Provides:
- Request tracing (X-Request-ID) for distributed request tracking (#13)
- Rate limiting via SlowAPI (#5)
"""

import contextvars
import uuid
from typing import Optional

import structlog
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# #13  Request Tracing Middleware
# ──────────────────────────────────────────────────────────────────────────────

# Context variable accessible from any async/sync code during a request
request_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)


def get_request_id() -> Optional[str]:
    """Get the current request ID from context (usable from any layer)."""
    return request_id_ctx.get()


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """
    Adds a unique X-Request-ID to every request/response.

    - Reads X-Request-ID from incoming headers (forwarded by reverse proxy)
    - Falls back to a generated UUID4 if not present
    - Stores it in a ContextVar for access from service/repository layers
    - Adds it to structlog context for automatic inclusion in all logs
    - Returns it in the response header
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Extract or generate request ID
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

        # Store in context var
        token = request_id_ctx.set(req_id)

        # Bind to structlog so all logs within this request include it
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=req_id)

        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            request_id_ctx.reset(token)
            structlog.contextvars.clear_contextvars()


# ──────────────────────────────────────────────────────────────────────────────
# #5  Rate Limiting
# ──────────────────────────────────────────────────────────────────────────────

def _get_client_ip(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For from reverse proxies."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def setup_rate_limiting(app: FastAPI) -> None:
    """
    Wire up SlowAPI rate limiting if enabled in settings.

    Uses the existing `rate_limit_per_minute` config field.
    Falls back gracefully if slowapi is not installed.
    """
    settings = get_settings()

    if not settings.rate_limit_enabled:
        logger.info("Rate limiting is disabled in configuration")
        return

    try:
        from slowapi import Limiter, _rate_limit_exceeded_handler
        from slowapi.errors import RateLimitExceeded

        limiter = Limiter(
            key_func=_get_client_ip,
            default_limits=[f"{settings.rate_limit_per_minute}/minute"],
            storage_uri=settings.redis_url,
        )

        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

        logger.info(
            "Rate limiting enabled",
            extra={"limit": f"{settings.rate_limit_per_minute}/minute"},
        )
    except ImportError:
        logger.warning(
            "slowapi not installed — rate limiting disabled. "
            "Install with: pip install slowapi"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Setup helper — called from main.py
# ──────────────────────────────────────────────────────────────────────────────

def setup_middleware(app: FastAPI) -> None:
    """Register all custom middleware on the FastAPI app."""
    # Request tracing (must be added first so it wraps everything)
    app.add_middleware(RequestTracingMiddleware)
    logger.info("Request tracing middleware (X-Request-ID) registered")

    # Rate limiting
    setup_rate_limiting(app)
