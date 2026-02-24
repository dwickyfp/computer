"""
Backfill endpoints.

Provides REST API for managing backfill jobs.
"""

from fastapi import APIRouter, Depends, Path, Query, status

from app.api.deps import get_backfill_service
from app.domain.schemas.backfill import (
    BackfillJobCreate,
    BackfillJobListResponse,
    BackfillJobResponse,
)
from app.domain.services.backfill import BackfillService

router = APIRouter()


@router.post(
    "/pipelines/{pipeline_id}/backfill",
    response_model=BackfillJobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create backfill job",
    description="Create a new backfill job for a pipeline to backfill historical data",
)
def create_backfill_job(
    pipeline_id: int = Path(..., description="Pipeline ID", gt=0),
    job_data: BackfillJobCreate = ...,
    service: BackfillService = Depends(get_backfill_service),
) -> BackfillJobResponse:
    """
    Create a new backfill job.

    Args:
        pipeline_id: Pipeline ID to backfill
        job_data: Backfill job configuration
        service: Backfill service instance

    Returns:
        Created backfill job
    """
    return service.create_backfill_job(pipeline_id, job_data)


@router.get(
    "/pipelines/{pipeline_id}/backfill",
    response_model=BackfillJobListResponse,
    status_code=status.HTTP_200_OK,
    summary="List backfill jobs",
    description="Get all backfill jobs for a specific pipeline",
)
def list_backfill_jobs(
    pipeline_id: int = Path(..., description="Pipeline ID", gt=0),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    service: BackfillService = Depends(get_backfill_service),
) -> BackfillJobListResponse:
    """
    List all backfill jobs for a pipeline.

    Args:
        pipeline_id: Pipeline ID to filter by
        skip: Number of records to skip (pagination)
        limit: Maximum records to return
        service: Backfill service instance

    Returns:
        List of backfill jobs with total count
    """
    return service.get_pipeline_backfill_jobs(pipeline_id, skip=skip, limit=limit)


@router.get(
    "/backfill/{job_id}",
    response_model=BackfillJobResponse,
    status_code=status.HTTP_200_OK,
    summary="Get backfill job",
    description="Get details of a specific backfill job",
)
def get_backfill_job(
    job_id: int = Path(..., description="Backfill job ID", gt=0),
    service: BackfillService = Depends(get_backfill_service),
) -> BackfillJobResponse:
    """
    Get a specific backfill job.

    Args:
        job_id: Backfill job ID
        service: Backfill service instance

    Returns:
        Backfill job details
    """
    return service.get_backfill_job(job_id)


@router.post(
    "/backfill/{job_id}/cancel",
    response_model=BackfillJobResponse,
    status_code=status.HTTP_200_OK,
    summary="Cancel backfill job",
    description="Cancel a pending or executing backfill job",
)
def cancel_backfill_job(
    job_id: int = Path(..., description="Backfill job ID", gt=0),
    service: BackfillService = Depends(get_backfill_service),
) -> BackfillJobResponse:
    """
    Cancel a backfill job.

    Args:
        job_id: Backfill job ID to cancel
        service: Backfill service instance

    Returns:
        Updated backfill job
    """
    return service.cancel_backfill_job(job_id)


@router.delete(
    "/backfill/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete backfill job",
    description="Delete a completed, failed, or cancelled backfill job",
)
def delete_backfill_job(
    job_id: int = Path(..., description="Backfill job ID", gt=0),
    service: BackfillService = Depends(get_backfill_service),
) -> None:
    """
    Delete a backfill job.

    Args:
        job_id: Backfill job ID to delete
        service: Backfill service instance
    """
    service.delete_backfill_job(job_id)
