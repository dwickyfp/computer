"""
Database utility functions for retry logic and connection health.

Provides decorators and helpers for handling transient database errors.
"""

import logging
import time
from functools import wraps
from typing import Callable, TypeVar, Any

import psycopg2

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_on_connection_error(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (psycopg2.OperationalError, psycopg2.InterfaceError),
) -> Callable:
    """
    Decorator to retry database operations on connection errors.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch and retry

    Usage:
        @retry_on_connection_error(max_retries=3, delay=1.0)
        def get_pipeline(pipeline_id: int):
            return PipelineRepository.get_by_id(pipeline_id)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt < max_retries:
                        logger.warning(
                            f"Database operation '{func.__name__}' failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Database operation '{func.__name__}' failed after {max_retries + 1} attempts: {e}"
                        )

            # Re-raise the last exception if all retries failed
            raise last_exception

        return wrapper

    return decorator


def is_connection_error(exception: Exception) -> bool:
    """
    Check if an exception is a database connection error.

    Args:
        exception: The exception to check

    Returns:
        True if it's a connection-related error
    """
    if isinstance(exception, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True

    error_msg = str(exception).lower()
    connection_error_keywords = [
        "connection",
        "closed",
        "terminated",
        "timeout",
        "reset",
        "broken pipe",
        "no message from the libpq",
    ]

    return any(keyword in error_msg for keyword in connection_error_keywords)


def validate_connection(conn: psycopg2.extensions.connection) -> bool:
    """
    Validate that a connection is alive and usable.

    Args:
        conn: PostgreSQL connection to validate

    Returns:
        True if connection is valid, False otherwise
    """
    try:
        # Try to access connection properties
        _ = conn.isolation_level

        # Execute a simple query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

        return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        return False
