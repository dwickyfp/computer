"""
Linked Task API endpoints.

REST API for managing linked task orchestration DAGs,
graph save/load, run triggering, and history.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.deps import get_linked_task_service
from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.schemas.linked_task import (
    LinkedTaskCreate,
    LinkedTaskDetailResponse,
    LinkedTaskEdgeResponse,
    LinkedTaskGraphSave,
    LinkedTaskListResponse,
    LinkedTaskResponse,
    LinkedTaskRunHistoryListResponse,
    LinkedTaskRunHistoryResponse,
    LinkedTaskStepResponse,
    LinkedTaskTriggerResponse,
    LinkedTaskUpdate,
)
from app.domain.services.linked_task import LinkedTaskService

logger = get_logger(__name__)
router = APIRouter()


# ─── CRUD ─────────────────────────────────────────────────────────────────────

@router.get(
    "",
    response_model=LinkedTaskListResponse,
    summary="List linked tasks",
)
def list_linked_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskListResponse:
    items, total = service.list_linked_tasks(page, page_size)
    return LinkedTaskListResponse(
        items=[LinkedTaskResponse.from_orm(t) for t in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post(
    "",
    response_model=LinkedTaskResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create linked task",
)
def create_linked_task(
    data: LinkedTaskCreate,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskResponse:
    try:
        task = service.create_linked_task(data)
        return LinkedTaskResponse.from_orm(task)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get(
    "/{linked_task_id}",
    response_model=LinkedTaskDetailResponse,
    summary="Get linked task",
)
def get_linked_task(
    linked_task_id: int,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskDetailResponse:
    try:
        task = service.get_linked_task(linked_task_id)
        steps, edges = service.get_graph(linked_task_id)
        resp = LinkedTaskDetailResponse.from_orm(task)
        resp.steps = [LinkedTaskStepResponse.from_orm(s) for s in steps]
        resp.edges = [LinkedTaskEdgeResponse.from_orm(e) for e in edges]
        return resp
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.put(
    "/{linked_task_id}",
    response_model=LinkedTaskResponse,
    summary="Update linked task",
)
def update_linked_task(
    linked_task_id: int,
    data: LinkedTaskUpdate,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskResponse:
    try:
        task = service.update_linked_task(linked_task_id, data)
        return LinkedTaskResponse.from_orm(task)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.delete(
    "/{linked_task_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete linked task",
)
def delete_linked_task(
    linked_task_id: int,
    service: LinkedTaskService = Depends(get_linked_task_service),
):
    try:
        service.delete_linked_task(linked_task_id)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# ─── Graph ─────────────────────────────────────────────────────────────────────

@router.post(
    "/{linked_task_id}/graph",
    response_model=LinkedTaskDetailResponse,
    summary="Save linked task graph",
)
def save_graph(
    linked_task_id: int,
    data: LinkedTaskGraphSave,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskDetailResponse:
    """Replace the full step/edge graph for a linked task."""
    try:
        steps, edges = service.save_graph(linked_task_id, data)
        task = service.get_linked_task(linked_task_id)
        resp = LinkedTaskDetailResponse.from_orm(task)
        resp.steps = [LinkedTaskStepResponse.from_orm(s) for s in steps]
        resp.edges = [LinkedTaskEdgeResponse.from_orm(e) for e in edges]
        return resp
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to save linked task graph: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save graph",
        )


# ─── Run ───────────────────────────────────────────────────────────────────────

@router.post(
    "/{linked_task_id}/run",
    response_model=LinkedTaskTriggerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger linked task run",
)
def trigger_run(
    linked_task_id: int,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskTriggerResponse:
    try:
        run = service.trigger_run(linked_task_id)
        return LinkedTaskTriggerResponse(
            message="Linked task dispatched",
            run_id=run.id,
            celery_task_id=run.celery_task_id or "",
            status="RUNNING",
        )
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to trigger linked task {linked_task_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to trigger run",
        )


# ─── Run History ───────────────────────────────────────────────────────────────

@router.get(
    "/{linked_task_id}/runs",
    response_model=LinkedTaskRunHistoryListResponse,
    summary="Get linked task run history",
)
def get_run_history(
    linked_task_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskRunHistoryListResponse:
    try:
        items, total = service.get_run_history(linked_task_id, page, page_size)
        return LinkedTaskRunHistoryListResponse(
            items=[LinkedTaskRunHistoryResponse.from_orm(r) for r in items],
            total=total,
            page=page,
            page_size=page_size,
        )
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


# ─── Cancel Run ────────────────────────────────────────────────────────────────

@router.post(
    "/{linked_task_id}/runs/{run_id}/cancel",
    response_model=LinkedTaskRunHistoryResponse,
    summary="Cancel a running linked task run",
)
def cancel_run(
    linked_task_id: int,
    run_id: int,
    service: LinkedTaskService = Depends(get_linked_task_service),
) -> LinkedTaskRunHistoryResponse:
    try:
        run = service.cancel_run(linked_task_id, run_id)
        return LinkedTaskRunHistoryResponse.from_orm(run)
    except EntityNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to cancel run {run_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel run",
        )
