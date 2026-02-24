"""
Backfill repository - Data access for queue_backfill_data.

Handles CRUD operations for backfill jobs.
"""

from typing import List, Optional, Dict

from sqlalchemy import select, update, func
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.queue_backfill import BackfillStatus, QueueBackfillData
from app.domain.repositories.base import BaseRepository

logger = get_logger(__name__)


class BackfillRepository(BaseRepository[QueueBackfillData]):
    """Repository for backfill job operations."""

    def __init__(self, db: Session):
        """Initialize repository with QueueBackfillData model."""
        super().__init__(QueueBackfillData, db)

    def get_by_pipeline_id(
        self, pipeline_id: int, skip: int = 0, limit: int = 100
    ) -> List[QueueBackfillData]:
        """
        Get backfill jobs for a specific pipeline.

        Args:
            pipeline_id: Pipeline ID to filter by
            skip: Number of records to skip (pagination)
            limit: Maximum number of records to return

        Returns:
            List of backfill jobs
        """
        try:
            stmt = (
                select(QueueBackfillData)
                .where(QueueBackfillData.pipeline_id == pipeline_id)
                .order_by(QueueBackfillData.created_at.desc())
                .offset(skip)
                .limit(limit)
            )
            result = self.db.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(
                f"Error fetching backfill jobs for pipeline {pipeline_id}: {e}"
            )
            return []

    def count_by_pipeline_id(self, pipeline_id: int) -> int:
        """
        Count backfill jobs for a pipeline.

        Args:
            pipeline_id: Pipeline ID to count

        Returns:
            Number of backfill jobs
        """
        try:
            stmt = select(QueueBackfillData).where(
                QueueBackfillData.pipeline_id == pipeline_id
            )
            result = self.db.execute(stmt)
            return len(list(result.scalars().all()))
        except Exception as e:
            logger.error(
                f"Error counting backfill jobs for pipeline {pipeline_id}: {e}"
            )
            return 0

    def get_pending_jobs(self, limit: int = 10) -> List[QueueBackfillData]:
        """
        Get pending backfill jobs ready for execution.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of pending backfill jobs
        """
        try:
            stmt = (
                select(QueueBackfillData)
                .where(QueueBackfillData.status == BackfillStatus.PENDING.value)
                .order_by(QueueBackfillData.created_at.asc())
                .limit(limit)
            )
            result = self.db.execute(stmt)
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error fetching pending backfill jobs: {e}")
            return []

    def update_status(
        self,
        job_id: int,
        status: str,
        count_record: Optional[int] = None,
    ) -> Optional[QueueBackfillData]:
        """
        Update backfill job status.

        Args:
            job_id: Job ID to update
            status: New status value
            count_record: Optional record count to update

        Returns:
            Updated job or None if not found
        """
        try:
            job = self.get_by_id(job_id)
            if not job:
                logger.warning(f"Backfill job {job_id} not found")
                return None

            job.status = status
            if count_record is not None:
                job.count_record = count_record

            self.db.flush()
            self.db.refresh(job)
            logger.info(f"Updated backfill job {job_id} to status {status}")
            return job
        except Exception as e:
            logger.error(f"Error updating backfill job {job_id}: {e}")
            return None

    def cancel_job(self, job_id: int) -> bool:
        """
        Cancel a backfill job.

        Args:
            job_id: Job ID to cancel

        Returns:
            True if cancelled, False otherwise
        """
        try:
            job = self.get_by_id(job_id)
            if not job:
                logger.warning(f"Backfill job {job_id} not found")
                return False

            if job.status not in [
                BackfillStatus.PENDING.value,
                BackfillStatus.EXECUTING.value,
            ]:
                logger.warning(f"Cannot cancel job {job_id} with status {job.status}")
                return False

            job.status = BackfillStatus.CANCELLED.value
            self.db.flush()
            logger.info(f"Cancelled backfill job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling backfill job {job_id}: {e}")
            return False

    def increment_count(self, job_id: int, increment: int) -> bool:
        """
        Increment record count for a job.

        Args:
            job_id: Job ID to update
            increment: Number to add to count

        Returns:
            True if updated, False otherwise
        """
        try:
            stmt = (
                update(QueueBackfillData)
                .where(QueueBackfillData.id == job_id)
                .values(count_record=QueueBackfillData.count_record + increment)
            )
            self.db.execute(stmt)
            self.db.flush()
            return True
        except Exception as e:
            logger.error(f"Error incrementing count for job {job_id}: {e}")
            return False

    def get_global_status_summary(self) -> Dict[str, int]:
        """
        Get count of backfill jobs by status globally.

        Returns:
            Dictionary of status counts
        """
        try:
            stmt = select(
                QueueBackfillData.status,
                func.count(QueueBackfillData.id)
            ).group_by(QueueBackfillData.status)

            result = self.db.execute(stmt)

            summary = {
                "PENDING": 0,
                "EXECUTING": 0,
                "COMPLETED": 0,
                "FAILED": 0,
                "CANCELLED": 0,
            }

            for row in result:
                status = row[0]
                count = row[1]
                if status in summary:
                    summary[status] = count
                else:
                    summary[status] = count

            # Calculate total
            summary["total"] = sum(summary.values())

            return summary
        except Exception as e:
            logger.error(f"Error fetching global backfill status summary: {e}")
            return {
                "PENDING": 0,
                "EXECUTING": 0,
                "COMPLETED": 0,
                "FAILED": 0,
                "CANCELLED": 0,
                "total": 0,
            }
