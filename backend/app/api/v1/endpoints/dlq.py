"""
DLQ endpoints.

Provides queue summaries, non-destructive message previews,
and destructive discard actions for the DLQ Manager UI.
"""

from fastapi import APIRouter, Depends, Path, Query, status

from app.api.deps import get_dlq_service, get_dlq_service_readonly
from app.domain.schemas.dlq import (
    DLQDiscardMessagesRequest,
    DLQDiscardResponse,
    DLQMessagesResponse,
    DLQPipelineDiscardResponse,
    DLQQueueIdentifier,
    DLQQueueListResponse,
)
from app.domain.services.dlq import DLQService

router = APIRouter()


@router.get(
    "/queues",
    response_model=DLQQueueListResponse,
    status_code=status.HTTP_200_OK,
    summary="List DLQ queues",
    description="List DLQ queues grouped by source table and destination",
)
def list_dlq_queues(
    pipeline_id: int | None = Query(
        default=None,
        gt=0,
        description="Optional pipeline filter",
    ),
    destination_id: int | None = Query(
        default=None,
        gt=0,
        description="Optional destination filter",
    ),
    search: str | None = Query(
        default=None,
        description="Optional case-insensitive search across queue labels",
    ),
    include_empty: bool = Query(
        default=False,
        description="Include empty streams that still exist in Redis",
    ),
    service: DLQService = Depends(get_dlq_service_readonly),
) -> DLQQueueListResponse:
    return service.list_queues(
        pipeline_id=pipeline_id,
        destination_id=destination_id,
        search=search,
        include_empty=include_empty,
    )


@router.get(
    "/messages",
    response_model=DLQMessagesResponse,
    status_code=status.HTTP_200_OK,
    summary="List DLQ messages",
    description="Preview queue messages newest-first without consuming them",
)
def list_dlq_messages(
    source_id: int = Query(..., gt=0, description="Source identifier"),
    destination_id: int = Query(..., gt=0, description="Destination identifier"),
    table_name: str = Query(..., min_length=1, description="Source table name"),
    before_id: str | None = Query(
        default=None,
        description="Cursor returned by the previous page to fetch older items",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="Maximum number of items to return",
    ),
    service: DLQService = Depends(get_dlq_service_readonly),
) -> DLQMessagesResponse:
    return service.list_messages(
        source_id=source_id,
        destination_id=destination_id,
        table_name=table_name,
        before_id=before_id,
        limit=limit,
    )


@router.post(
    "/messages/discard",
    response_model=DLQDiscardResponse,
    status_code=status.HTTP_200_OK,
    summary="Discard selected DLQ messages",
    description="Permanently discard selected DLQ rows from a queue",
)
def discard_dlq_messages(
    request: DLQDiscardMessagesRequest,
    service: DLQService = Depends(get_dlq_service),
) -> DLQDiscardResponse:
    return service.discard_messages(request)


@router.post(
    "/queues/discard",
    response_model=DLQDiscardResponse,
    status_code=status.HTTP_200_OK,
    summary="Discard an entire DLQ queue",
    description="Permanently discard all rows in a selected DLQ queue",
)
def discard_dlq_queue(
    request: DLQQueueIdentifier,
    service: DLQService = Depends(get_dlq_service),
) -> DLQDiscardResponse:
    return service.discard_queue(request)


@router.post(
    "/pipelines/{pipeline_id}/discard",
    response_model=DLQPipelineDiscardResponse,
    status_code=status.HTTP_200_OK,
    summary="Discard all DLQ queues for a pipeline",
    description="Permanently discard all DLQ rows tied to a pipeline source",
)
def discard_pipeline_dlq(
    pipeline_id: int = Path(..., gt=0, description="Pipeline identifier"),
    service: DLQService = Depends(get_dlq_service),
) -> DLQPipelineDiscardResponse:
    return service.discard_pipeline(pipeline_id)
