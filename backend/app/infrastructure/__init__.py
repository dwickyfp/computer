"""
Infrastructure layer initialization.

Exports infrastructure components like background tasks.
"""

from app.infrastructure.tasks.scheduler import BackgroundScheduler

__all__ = [
    "BackgroundScheduler",
]
