"""
Core initialization module.

Exports core utilities and configuration.
"""

from app.core.config import get_settings, settings
from app.core.database import get_db, get_db_session
from app.core.exceptions import (
    DatabaseConnectionError,
    DatabaseError,
    EntityNotFoundError,
    RosettaException,
    ValidationError,
)
from app.core.logging import get_logger, setup_logging

__all__ = [
    # Configuration
    "settings",
    "get_settings",
    # Database
    "get_db",
    "get_db_session",
    # Exceptions
    "RosettaException",
    "DatabaseError",
    "DatabaseConnectionError",
    "EntityNotFoundError",
    "ValidationError",
    # Logging
    "setup_logging",
    "get_logger",
]
