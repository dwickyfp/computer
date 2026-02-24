"""
Worker Health Status Model.

Stores the current health status of Celery workers,
updated periodically by background task.
"""

from datetime import datetime
from sqlalchemy import Boolean, Column, DateTime, Integer, String, JSON
from app.domain.models.base import Base


class WorkerHealthStatus(Base):
    """Worker health status table."""

    __tablename__ = "worker_health_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    healthy = Column(Boolean, nullable=False, default=False)
    active_workers = Column(Integer, nullable=False, default=0)
    active_tasks = Column(Integer, nullable=False, default=0)
    reserved_tasks = Column(Integer, nullable=False, default=0)
    error_message = Column(String, nullable=True)
    extra_data = Column(JSON, nullable=True)
    last_check_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
