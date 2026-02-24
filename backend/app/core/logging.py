"""
Structured logging configuration.

Provides JSON-formatted logging with context enrichment for
production-ready observability.
"""

import logging
import sys
from typing import Any, Dict

import structlog
from pythonjsonlogger import jsonlogger

from app.core.config import get_settings


def setup_logging() -> None:
    """
    Configure structured logging for the application.

    Sets up:
    - JSON formatted logs for production
    - Console and file handlers
    - Log rotation
    - Structured context enrichment
    """
    settings = get_settings()


    # Configure logging level
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Create formatters
    if settings.log_format == "json":
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Silence noisy loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a logger instance with structured logging support.

    Args:
        name: Logger name (usually __name__ of the module)

    Returns:
        Configured logger instance with structured logging

    Usage:
        logger = get_logger(__name__)
        logger.info("User logged in", user_id=123, ip_address="192.168.1.1")
    """
    return structlog.get_logger(name)


class LoggerAdapter:
    """
    Logger adapter for adding request context to logs.

    Useful for middleware to add request_id, user_id, etc.
    """

    def __init__(self, logger: structlog.stdlib.BoundLogger, context: Dict[str, Any]):
        """
        Initialize logger adapter.

        Args:
            logger: Base logger instance
            context: Context dictionary to add to all log entries
        """
        self.logger = logger
        self.context = context

    def _add_context(self, **kwargs) -> Dict[str, Any]:
        """Add context to log kwargs."""
        return {**self.context, **kwargs}

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(message, **self._add_context(**kwargs))

    def info(self, message: str, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(message, **self._add_context(**kwargs))

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(message, **self._add_context(**kwargs))

    def error(self, message: str, **kwargs) -> None:
        """Log error message with context."""
        self.logger.error(message, **self._add_context(**kwargs))

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message with context."""
        self.logger.critical(message, **self._add_context(**kwargs))
