"""
Backfill service containing business logic.

Implements business rules and orchestrates backfill operations.
"""

from typing import List, Optional

from sqlalchemy.orm import Session

from app.core.exceptions import EntityNotFoundError, ValidationError
from app.core.logging import get_logger
from app.domain.models.queue_backfill import BackfillStatus, QueueBackfillData
from app.domain.repositories.backfill import BackfillRepository
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
from app.domain.schemas.backfill import (
    BackfillJobCreate,
    BackfillJobResponse,
    BackfillJobListResponse,
)

logger = get_logger(__name__)


class BackfillService:
    """
    Service layer for Backfill operations.

    Implements business logic for managing backfill jobs.
    """

    def __init__(self, db: Session):
        """Initialize backfill service."""
        self.db = db
        self.repository = BackfillRepository(db)
        self.pipeline_repo = PipelineRepository(db)
        self.table_metadata_repo = TableMetadataRepository(db)

    def create_backfill_job(
        self, pipeline_id: int, job_data: BackfillJobCreate
    ) -> BackfillJobResponse:
        """
        Create a new backfill job.

        Args:
            pipeline_id: Pipeline ID for the backfill
            job_data: Backfill job creation data

        Returns:
            Created backfill job

        Raises:
            EntityNotFoundError: If pipeline doesn't exist
            ValidationError: If table doesn't exist in source or validation fails
        """
        logger.info(
            f"Creating backfill job for pipeline {pipeline_id}, table {job_data.table_name}"
        )

        # Validate pipeline exists
        pipeline = self.pipeline_repo.get_by_id(pipeline_id)
        if not pipeline:
            raise EntityNotFoundError(
                entity_type="Pipeline",
                entity_id=pipeline_id,
            )

        # Validate pipeline is not already running a backfill for this table
        existing_jobs = self.repository.get_by_pipeline_id(pipeline_id)
        active_statuses = [BackfillStatus.PENDING.value, BackfillStatus.EXECUTING.value]

        # Validate table exists in source
        table_metadata = self.table_metadata_repo.get_by_source_and_name(
            source_id=pipeline.source_id,
            table_name=job_data.table_name,
        )

        if not table_metadata:
            raise ValidationError(
                message=f"Table '{job_data.table_name}' does not exist in source",
                details={"field": "table_name"},
            )

        # Convert filters to SQL format
        filter_sql = job_data.get_filter_sql()

        # Create backfill job
        job = self.repository.create(
            pipeline_id=pipeline_id,
            source_id=pipeline.source_id,
            table_name=job_data.table_name,
            filter_sql=filter_sql,
            status=BackfillStatus.PENDING.value,
            count_record=0,
        )

        self.db.commit()

        logger.info(f"Backfill job created successfully: {job.id}")
        return BackfillJobResponse.from_orm(job)

    def get_pipeline_backfill_jobs(
        self, pipeline_id: int, skip: int = 0, limit: int = 10
    ) -> BackfillJobListResponse:
        """
        Get all backfill jobs for a pipeline.

        Args:
            pipeline_id: Pipeline ID to filter by
            skip: Number of records to skip
            limit: Maximum number of records

        Returns:
            List of backfill jobs with total count
        """
        logger.debug(f"Fetching backfill jobs for pipeline {pipeline_id}")

        jobs = self.repository.get_by_pipeline_id(pipeline_id, skip=skip, limit=limit)
        total = self.repository.count_by_pipeline_id(pipeline_id)

        return BackfillJobListResponse(
            total=total,
            items=[BackfillJobResponse.from_orm(job) for job in jobs],
        )

    def get_backfill_job(self, job_id: int) -> BackfillJobResponse:
        """
        Get a specific backfill job.

        Args:
            job_id: Backfill job ID

        Returns:
            Backfill job details

        Raises:
            EntityNotFoundError: If job doesn't exist
        """
        job = self.repository.get_by_id(job_id)
        if not job:
            raise EntityNotFoundError(
                entity_type="BackfillJob",
                entity_id=job_id,
            )

        return BackfillJobResponse.from_orm(job)

    def cancel_backfill_job(self, job_id: int) -> BackfillJobResponse:
        """
        Cancel a backfill job.

        Args:
            job_id: Backfill job ID to cancel

        Returns:
            Updated backfill job

        Raises:
            EntityNotFoundError: If job doesn't exist
            ValidationError: If job cannot be cancelled
        """
        logger.info(f"Cancelling backfill job {job_id}")

        job = self.repository.get_by_id(job_id)
        if not job:
            raise EntityNotFoundError(
                entity_type="BackfillJob",
                entity_id=job_id,
            )

        # Check if job can be cancelled
        if job.status not in [
            BackfillStatus.PENDING.value,
            BackfillStatus.EXECUTING.value,
        ]:
            raise ValidationError(
                message=f"Cannot cancel job with status {job.status}",
                details={"field": "status"},
            )

        # Cancel the job
        success = self.repository.cancel_job(job_id)
        if not success:
            raise ValidationError(
                message="Failed to cancel backfill job",
                details={"field": "status"},
            )

        self.db.commit()

        # Fetch updated job
        updated_job = self.repository.get_by_id(job_id)
        logger.info(f"Backfill job {job_id} cancelled successfully")

        return BackfillJobResponse.from_orm(updated_job)

    def delete_backfill_job(self, job_id: int) -> bool:
        """
        Delete a backfill job.

        Args:
            job_id: Backfill job ID to delete

        Returns:
            True if deleted successfully

        Raises:
            EntityNotFoundError: If job doesn't exist
            ValidationError: If job is currently executing
        """
        logger.info(f"Deleting backfill job {job_id}")

        job = self.repository.get_by_id(job_id)
        if not job:
            raise EntityNotFoundError(
                entity_type="BackfillJob",
                entity_id=job_id,
            )

        # Don't allow deleting executing jobs
        if job.status == BackfillStatus.EXECUTING.value:
            raise ValidationError(
                message="Cannot delete job that is currently executing. Cancel it first.",
                details={"field": "status"},
            )

        self.repository.delete(job_id)
        self.db.commit()

        logger.info(f"Backfill job {job_id} deleted successfully")
        return True
