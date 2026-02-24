"""
Pipeline endpoints.

Provides REST API for managing ETL pipelines.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_pipeline_service, get_pipeline_service_readonly
from app.core.config import get_settings
from app.domain.schemas.pipeline import (
    PipelineCreate,
    PipelineResponse,
    PipelineUpdate,
)
from app.domain.schemas.pipeline_preview import PipelinePreviewRequest
from app.domain.services.pipeline import PipelineService

router = APIRouter()


@router.post(
    "",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create pipeline",
    description="Create a new ETL pipeline connecting a source to a destination",
)
def create_pipeline(
    pipeline_data: PipelineCreate,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineResponse:
    """
    Create a new pipeline.

    Args:
        pipeline_data: Pipeline configuration data
        service: Pipeline service instance

    Returns:
        Created pipeline with source and destination details
    """
    pipeline = service.create_pipeline(pipeline_data)
    return PipelineResponse.from_orm(pipeline)


@router.post(
    "/{pipeline_id}/destinations",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add destination to pipeline",
    description="Add a destination to an existing pipeline",
)
def add_pipeline_destination(
    pipeline_id: int,
    destination_id: int = Query(..., description="Destination ID to add"),
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineResponse:
    """
    Add a destination to a pipeline.

    Args:
        pipeline_id: Pipeline identifier
        destination_id: Destination identifier
        service: Pipeline service instance

    Returns:
        Updated pipeline with new destination
    """
    pipeline = service.add_pipeline_destination(pipeline_id, destination_id)
    return PipelineResponse.from_orm(pipeline)


@router.delete(
    "/{pipeline_id}/destinations/{destination_id}",
    response_model=PipelineResponse,
    status_code=status.HTTP_200_OK,
    summary="Remove destination from pipeline",
    description="Remove a destination from an existing pipeline",
)
def remove_pipeline_destination(
    pipeline_id: int,
    destination_id: int,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineResponse:
    """
    Remove a destination from a pipeline.

    Args:
        pipeline_id: Pipeline identifier
        destination_id: Destination identifier
        service: Pipeline service instance

    Returns:
        Updated pipeline without the removed destination
    """
    pipeline = service.remove_pipeline_destination(pipeline_id, destination_id)
    return PipelineResponse.from_orm(pipeline)


@router.get(
    "",
    response_model=List[PipelineResponse],
    summary="List pipelines",
    description="Get a list of all configured pipelines",
)
def list_pipelines(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of items to return"
    ),
    service: PipelineService = Depends(get_pipeline_service_readonly),
) -> List[PipelineResponse]:
    """
    List all pipelines with pagination.

    Args:
        skip: Number of pipelines to skip
        limit: Maximum number of pipelines to return
        service: Pipeline service instance

    Returns:
        List of pipelines with source, destination, and metadata
    """
    pipelines = service.list_pipelines(skip=skip, limit=limit)
    return [PipelineResponse.from_orm(p) for p in pipelines]


@router.get(
    "/{pipeline_id}",
    response_model=PipelineResponse,
    summary="Get pipeline",
    description="Get a specific pipeline by ID with full details",
)
def get_pipeline(
    pipeline_id: int, service: PipelineService = Depends(get_pipeline_service_readonly)
) -> PipelineResponse:
    """
    Get pipeline by ID.

    Args:
        pipeline_id: Pipeline identifier
        service: Pipeline service instance

    Returns:
        Pipeline details with source, destination, and metadata
    """
    pipeline = service.get_pipeline(pipeline_id)
    return PipelineResponse.from_orm(pipeline)


@router.put(
    "/{pipeline_id}",
    response_model=PipelineResponse,
    summary="Update pipeline",
    description="Update an existing pipeline configuration",
)
def update_pipeline(
    pipeline_id: int,
    pipeline_data: PipelineUpdate,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineResponse:
    """
    Update pipeline.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_data: Pipeline update data
        service: Pipeline service instance

    Returns:
        Updated pipeline
    """
    pipeline = service.update_pipeline(pipeline_id, pipeline_data)
    return PipelineResponse.from_orm(pipeline)


@router.delete(
    "/{pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete pipeline",
    description="Delete a pipeline configuration",
)
def delete_pipeline(
    pipeline_id: int, service: PipelineService = Depends(get_pipeline_service)
) -> None:
    """
    Delete pipeline.

    Args:
        pipeline_id: Pipeline identifier
        service: Pipeline service instance
    """
    service.delete_pipeline(pipeline_id)


@router.post(
    "/{pipeline_id}/start",
    response_model=PipelineResponse,
    summary="Start pipeline",
    description="Start a paused pipeline",
)
def start_pipeline(
    pipeline_id: int, service: PipelineService = Depends(get_pipeline_service)
) -> PipelineResponse:
    """
    Start a pipeline.

    Args:
        pipeline_id: Pipeline identifier
        service: Pipeline service instance

    Returns:
        Updated pipeline
    """
    pipeline = service.start_pipeline(pipeline_id)
    return PipelineResponse.from_orm(pipeline)


@router.post(
    "/{pipeline_id}/pause",
    response_model=PipelineResponse,
    summary="Pause pipeline",
    description="Pause a running pipeline",
)
def pause_pipeline(
    pipeline_id: int, service: PipelineService = Depends(get_pipeline_service)
) -> PipelineResponse:
    """
    Pause a pipeline.

    Args:
        pipeline_id: Pipeline identifier
        service: Pipeline service instance

    Returns:
        Updated pipeline
    """
    pipeline = service.pause_pipeline(pipeline_id)
    return PipelineResponse.from_orm(pipeline)


@router.post(
    "/{pipeline_id}/refresh",
    response_model=PipelineResponse,
    summary="Refresh pipeline",
    description="Trigger a pipeline refresh",
)
def refresh_pipeline(
    pipeline_id: int, service: PipelineService = Depends(get_pipeline_service)
) -> PipelineResponse:
    """
    Refresh a pipeline.

    Args:
        pipeline_id: Pipeline identifier
        service: Pipeline service instance

    Returns:
        Updated pipeline
    """
    pipeline = service.refresh_pipeline(pipeline_id)
    return PipelineResponse.from_orm(pipeline)


@router.get(
    "/{pipeline_id}/stats",
    response_model=List[dict],
    summary="Get pipeline data flow stats",
    description="Get data flow statistics for a pipeline, including daily counts and recent activity",
)
def get_pipeline_stats(
    pipeline_id: int,
    days: int = Query(7, ge=1, le=30, description="Number of days to look back"),
    service: PipelineService = Depends(get_pipeline_service_readonly),
) -> List[dict]:
    """
    Get pipeline data flow statistics.

    Args:
        pipeline_id: Pipeline identifier
        days: Number of days to look back
        service: Pipeline service instance

    Returns:
        List of statistics per table
    """
    return service.get_pipeline_data_flow_stats(pipeline_id, days)


@router.post(
    "/{pipeline_id}/preview",
    summary="Preview custom SQL",
    description="Preview result of custom SQL query with live data. "
    "When worker is enabled, returns a task_id for polling.",
)
def preview_pipeline_sql(
    pipeline_id: int,
    request: PipelinePreviewRequest,
    service: PipelineService = Depends(get_pipeline_service),
):
    """
    Preview custom SQL.

    If WORKER_ENABLED=true, dispatches to Celery worker and returns
    { task_id, state: "PENDING" } for async polling.
    Otherwise, executes synchronously and returns preview data.

    Args:
        pipeline_id: Pipeline identifier
        request: Preview request
        service: Pipeline service instance

    Returns:
        PipelinePreviewResponse (sync) or { task_id, state } (async)
    """
    settings = get_settings()

    if settings.worker_enabled:
        from fastapi import HTTPException
        from app.infrastructure.worker_client import get_worker_client

        try:
            client = get_worker_client()
            task_id = client.submit_preview_task(
                sql=request.sql,
                source_id=request.source_id,
                destination_id=request.destination_id,
                table_name=request.table_name,
                filter_sql=request.filter_sql,
            )
            return {"task_id": task_id, "state": "PENDING", "status": "queued"}
        except ConnectionError as e:
            # Worker unavailable - return error, don't fall back to sync
            raise HTTPException(
                status_code=503,
                detail="Worker service unavailable. Please ensure the worker is running.",
            )

    # Sync mode only when WORKER_ENABLED=false
    return service.preview_custom_sql(request)


@router.get(
    "/{pipeline_id}/preview/{task_id}",
    summary="Poll preview task status",
    description="Check the status of an async preview task.",
)
def poll_preview_task(
    pipeline_id: int,
    task_id: str,
):
    """
    Poll the status of a preview task submitted to the worker.

    States:
    - PENDING: Task is queued
    - STARTED/PROGRESS: Task is running
    - SUCCESS: Task completed, result included
    - FAILURE: Task failed, error included
    - REVOKED: Task was cancelled

    Args:
        pipeline_id: Pipeline identifier
        task_id: Celery task ID

    Returns:
        Task status dict with state and optional result/error
    """
    from app.infrastructure.worker_client import get_worker_client

    client = get_worker_client()
    return client.get_task_status(task_id)


@router.delete(
    "/{pipeline_id}/preview/{task_id}",
    summary="Cancel preview task",
    description="Cancel a running or pending preview task.",
)
def cancel_preview_task(
    pipeline_id: int,
    task_id: str,
):
    """
    Cancel a preview task.

    Args:
        pipeline_id: Pipeline identifier
        task_id: Celery task ID

    Returns:
        Success status
    """
    from app.infrastructure.worker_client import get_worker_client

    client = get_worker_client()
    cancelled = client.cancel_task(task_id)
    return {"task_id": task_id, "cancelled": cancelled}
