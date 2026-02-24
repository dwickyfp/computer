"""
Flow Task API endpoints.

Provides REST API for managing visual ETL flow tasks, graphs,
run triggers, node previews, and execution history.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.deps import get_flow_task_service
from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.schemas.flow_task import (
    ColumnInfo,
    FlowTaskCreate,
    FlowTaskGraphResponse,
    FlowTaskGraphSave,
    FlowTaskGraphSaveWithSummary,
    FlowTaskGraphVersionListResponse,
    FlowTaskGraphVersionResponse,
    FlowTaskListResponse,
    FlowTaskResponse,
    FlowTaskRunHistoryListResponse,
    FlowTaskRunHistoryResponse,
    FlowTaskTriggerResponse,
    FlowTaskUpdate,
    FlowTaskWatermarkConfig,
    FlowTaskWatermarkResponse,
    NodeColumnsResponse,
    NodePreviewRequest,
    NodePreviewTaskResponse,
    TaskStatusResponse,
)
from app.domain.services.flow_task import FlowTaskService

logger = get_logger(__name__)
router = APIRouter()


# ─── Flow Task CRUD ────────────────────────────────────────────────────────────

@router.post(
    "",
    response_model=FlowTaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create flow task",
)
def create_flow_task(
    data: FlowTaskCreate,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskResponse:
    """Create a new visual ETL flow task."""
    try:
        task = service.create_flow_task(data)
        return FlowTaskResponse.from_orm(task)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create flow task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create flow task",
        )


@router.get(
    "",
    response_model=FlowTaskListResponse,
    summary="List flow tasks",
)
def list_flow_tasks(
    page: int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=1000, description="Items per page"),
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskListResponse:
    """List all flow tasks with pagination."""
    skip = (page - 1) * page_size
    items, total = service.list_flow_tasks(skip=skip, limit=page_size)
    return FlowTaskListResponse(
        items=[FlowTaskResponse.from_orm(t) for t in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get(
    "/{flow_task_id}",
    response_model=FlowTaskResponse,
    summary="Get flow task",
)
def get_flow_task(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskResponse:
    """Get a flow task by ID."""
    try:
        task = service.get_flow_task(flow_task_id)
        return FlowTaskResponse.from_orm(task)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.put(
    "/{flow_task_id}",
    response_model=FlowTaskResponse,
    summary="Update flow task",
)
def update_flow_task(
    flow_task_id: int,
    data: FlowTaskUpdate,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskResponse:
    """Update flow task name, description, or trigger type."""
    try:
        task = service.update_flow_task(flow_task_id, data)
        return FlowTaskResponse.from_orm(task)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.delete(
    "/{flow_task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete flow task",
)
def delete_flow_task(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> None:
    """Delete a flow task and all associated data."""
    try:
        service.delete_flow_task(flow_task_id)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post(
    "/{flow_task_id}/duplicate",
    response_model=FlowTaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Duplicate flow task",
)
def duplicate_flow_task(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskResponse:
    """Duplicate a flow task and its graph."""
    try:
        task = service.duplicate_flow_task(flow_task_id)
        return FlowTaskResponse.from_orm(task)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to duplicate flow task {flow_task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to duplicate flow task",
        )


# ─── Graph ─────────────────────────────────────────────────────────────────────

@router.get(
    "/{flow_task_id}/graph",
    response_model=FlowTaskGraphResponse,
    summary="Load flow graph",
)
def get_graph(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskGraphResponse:
    """
    Load the saved node/edge graph for a flow task.

    Returns 404 if the graph has never been saved.
    """
    try:
        graph = service.get_graph(flow_task_id)
        if not graph:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No graph found for flow task {flow_task_id}. Save the graph first.",
            )
        return FlowTaskGraphResponse.from_orm(graph)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post(
    "/{flow_task_id}/graph",
    response_model=FlowTaskGraphResponse,
    summary="Save flow graph",
)
def save_graph(
    flow_task_id: int,
    data: FlowTaskGraphSave,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskGraphResponse:
    """
    Save (upsert) the node/edge graph for a flow task.

    Node positions are persisted so the canvas restores correctly on reload.
    """
    try:
        graph = service.save_graph(flow_task_id, data)
        # Invalidate schema cache so stale column info from the old graph
        # is not served after the topology/node config changes.
        try:
            from app.core.config import get_settings
            from app.infrastructure.schema_cache import invalidate_schema_cache
            invalidate_schema_cache(
                flow_task_id=flow_task_id,
                redis_url=get_settings().redis_url,
            )
        except Exception as cache_err:
            logger.warning("Schema cache invalidation failed (non-fatal): %s", cache_err)
        return FlowTaskGraphResponse.from_orm(graph)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to save graph for flow_task {flow_task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save graph",
        )


# ─── Run ───────────────────────────────────────────────────────────────────────

@router.post(
    "/{flow_task_id}/run",
    response_model=FlowTaskTriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger flow task run",
)
def trigger_run(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskTriggerResponse:
    """
    Trigger a manual execution of the flow task.

    Dispatches a Celery task and returns the run_id and celery_task_id
    for polling.
    """
    try:
        result = service.trigger_run(flow_task_id, trigger_type="MANUAL")
        return FlowTaskTriggerResponse(**result)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        )


# ─── Cancel ───────────────────────────────────────────────────────────────────

@router.post(
    "/{flow_task_id}/cancel",
    summary="Cancel a running flow task",
)
def cancel_run(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> dict:
    """Cancel the currently running execution of a flow task."""
    try:
        return service.cancel_run(flow_task_id)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# ─── Preview ───────────────────────────────────────────────────────────────────

@router.post(
    "/{flow_task_id}/preview",
    response_model=NodePreviewTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Preview node data",
)
def preview_node(
    flow_task_id: int,
    request: NodePreviewRequest,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> NodePreviewTaskResponse:
    """
    Submit a node preview task.

    Accepts the current (possibly unsaved) graph snapshot and the target
    node ID. Returns a task_id to poll for the 500-row preview result.
    """
    try:
        result = service.preview_node(flow_task_id, request)
        return NodePreviewTaskResponse(**result)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
        )


@router.get(
    "/task-status/{celery_task_id}",
    response_model=TaskStatusResponse,
    summary="Poll Celery task status",
)
def get_task_status(
    celery_task_id: str,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> TaskStatusResponse:
    """
    Poll the status of a Celery task (run or preview).

    Terminal states: SUCCESS, FAILURE.
    In-progress states: PENDING, STARTED, PROGRESS.
    """
    status_data = service.get_task_status(celery_task_id)
    return TaskStatusResponse(**status_data)


# ─── Node schema ───────────────────────────────────────────────────────────────

@router.post(
    "/{flow_task_id}/node-schema",
    response_model=NodeColumnsResponse,
    summary="Resolve node output schema via DuckDB",
)
def get_node_schema(
    flow_task_id: int,
    request: NodePreviewRequest,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> NodeColumnsResponse:
    """
    Execute the CTE chain up to the target node with LIMIT 0 in the worker's
    DuckDB instance and return the schema (column names + types) of the output.

    This correctly reflects transformed schemas — e.g. after Aggregate the
    columns are the group-by fields + aggregation aliases, not the source table
    columns. No actual data rows are fetched.
    """
    from app.core.config import get_settings
    from app.infrastructure.worker_client import get_node_schema_from_worker
    from app.infrastructure.schema_cache import get_or_fetch_schema

    settings = get_settings()
    worker_url = getattr(settings, "worker_health_url", "http://0.0.0.0:8002")

    nodes_raw = [n.dict() for n in request.nodes]
    edges_raw = [e.dict(by_alias=True) for e in request.edges]

    def _fetch():
        return get_node_schema_from_worker(
            node_id=request.node_id,
            nodes=nodes_raw,
            edges=edges_raw,
            worker_base_url=worker_url,
        )

    cols = get_or_fetch_schema(
        flow_task_id=flow_task_id,
        node_id=request.node_id,
        nodes=nodes_raw,
        edges=edges_raw,
        redis_url=settings.redis_url,
        fetcher=_fetch,
    )

    return NodeColumnsResponse(columns=[ColumnInfo(**c) for c in cols])


# ─── Run History ───────────────────────────────────────────────────────────────

@router.get(
    "/{flow_task_id}/runs",
    response_model=FlowTaskRunHistoryListResponse,
    summary="List run history",
)
def list_run_history(
    flow_task_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=100),
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskRunHistoryListResponse:
    """List execution history for a flow task with pagination."""
    try:
        skip = (page - 1) * page_size
        items, total = service.get_run_history(
            flow_task_id=flow_task_id, skip=skip, limit=page_size
        )
        return FlowTaskRunHistoryListResponse(
            items=[FlowTaskRunHistoryResponse.from_orm(r) for r in items],
            total=total,
            page=page,
            page_size=page_size,
        )
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.get(
    "/runs/{run_id}",
    response_model=FlowTaskRunHistoryResponse,
    summary="Get run detail",
)
def get_run_detail(
    run_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskRunHistoryResponse:
    """Get a single run history record with per-node logs."""
    try:
        run = service.get_run_detail(run_id)
        return FlowTaskRunHistoryResponse.from_orm(run)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


# ─── D4: Graph Versioning ─────────────────────────────────────────────────────

@router.get(
    "/{flow_task_id}/versions",
    response_model=FlowTaskGraphVersionListResponse,
    summary="List graph versions",
)
def list_graph_versions(
    flow_task_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskGraphVersionListResponse:
    """List version history for a flow task graph."""
    try:
        skip = (page - 1) * page_size
        items, total = service.list_graph_versions(
            flow_task_id=flow_task_id, skip=skip, limit=page_size
        )
        return FlowTaskGraphVersionListResponse(
            items=[FlowTaskGraphVersionResponse.from_orm(v) for v in items],
            total=total,
            page=page,
            page_size=page_size,
        )
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.get(
    "/{flow_task_id}/versions/{version}",
    response_model=FlowTaskGraphVersionResponse,
    summary="Get specific graph version",
)
def get_graph_version(
    flow_task_id: int,
    version: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskGraphVersionResponse:
    """Get a specific version snapshot of the graph."""
    try:
        v = service.get_graph_version(flow_task_id, version)
        return FlowTaskGraphVersionResponse.from_orm(v)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post(
    "/{flow_task_id}/rollback/{version}",
    response_model=FlowTaskGraphResponse,
    summary="Rollback graph to version",
)
def rollback_graph(
    flow_task_id: int,
    version: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskGraphResponse:
    """Rollback the graph to a previous version."""
    try:
        graph = service.rollback_graph(flow_task_id, version)
        return FlowTaskGraphResponse.from_orm(graph)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# ─── D8: Watermark Management ─────────────────────────────────────────────────

@router.get(
    "/{flow_task_id}/watermarks",
    response_model=list[FlowTaskWatermarkResponse],
    summary="List watermarks",
)
def list_watermarks(
    flow_task_id: int,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> list[FlowTaskWatermarkResponse]:
    """Get all watermarks for a flow task."""
    try:
        wms = service.get_watermarks(flow_task_id)
        return [FlowTaskWatermarkResponse.from_orm(w) for w in wms]
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post(
    "/{flow_task_id}/watermarks",
    response_model=FlowTaskWatermarkResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Set watermark",
)
def set_watermark(
    flow_task_id: int,
    config: FlowTaskWatermarkConfig,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> FlowTaskWatermarkResponse:
    """Configure a watermark for an input node."""
    try:
        wm = service.set_watermark(
            flow_task_id=flow_task_id,
            node_id=config.node_id,
            watermark_column=config.watermark_column,
            watermark_type=config.watermark_type,
        )
        return FlowTaskWatermarkResponse.from_orm(wm)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.delete(
    "/{flow_task_id}/watermarks/{node_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Reset watermark",
)
def reset_watermark(
    flow_task_id: int,
    node_id: str,
    service: FlowTaskService = Depends(get_flow_task_service),
) -> None:
    """Reset (delete) the watermark for an input node."""
    try:
        service.reset_watermark(flow_task_id, node_id)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
