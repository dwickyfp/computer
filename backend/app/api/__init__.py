"""
API initialization module.

Exports API dependencies and utilities.
"""

from app.api.deps import get_db

__all__ = [
    "get_db",
]
