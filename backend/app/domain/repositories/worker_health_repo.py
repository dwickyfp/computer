"""
Worker Health Status Repository.

Handles database operations for worker health status.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from app.domain.models.worker_health import WorkerHealthStatus


class WorkerHealthRepository:
    """Repository for worker health status operations."""

    def __init__(self, db: Session):
        """Initialize repository."""
        self.db = db

    def get_latest(self) -> Optional[WorkerHealthStatus]:
        """Get the latest worker health status."""
        return (
            self.db.query(WorkerHealthStatus)
            .order_by(WorkerHealthStatus.last_check_at.desc())
            .first()
        )

    def upsert_status(
        self,
        healthy: bool,
        active_workers: int = 0,
        active_tasks: int = 0,
        reserved_tasks: int = 0,
        error_message: Optional[str] = None,
        extra_data: Optional[dict] = None,
    ) -> WorkerHealthStatus:
        """
        Upsert worker health status.
        
        Always creates a new record for audit trail, but keeps only recent records.
        """
        now = datetime.now(timezone(timedelta(hours=7)))
        
        status = WorkerHealthStatus(
            healthy=healthy,
            active_workers=active_workers,
            active_tasks=active_tasks,
            reserved_tasks=reserved_tasks,
            error_message=error_message,
            extra_data=extra_data,
            last_check_at=now,
            created_at=now,
            updated_at=now,
        )
        
        self.db.add(status)
        self.db.commit()
        self.db.refresh(status)
        
        # Cleanup old records (keep only last 100)
        self._cleanup_old_records()
        
        return status

    def _cleanup_old_records(self) -> None:
        """Keep only the most recent 100 records."""
        try:
            # Get IDs of records to keep
            keep_ids = (
                self.db.query(WorkerHealthStatus.id)
                .order_by(WorkerHealthStatus.last_check_at.desc())
                .limit(100)
            )
            
            # Delete old records - use scalar_subquery() to avoid warning
            self.db.query(WorkerHealthStatus).filter(
                WorkerHealthStatus.id.notin_(keep_ids.scalar_subquery())
            ).delete(synchronize_session=False)
            
            self.db.commit()
        except Exception:
            # Don't fail the main operation if cleanup fails
            self.db.rollback()
