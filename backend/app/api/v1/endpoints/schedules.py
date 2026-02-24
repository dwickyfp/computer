"""
Schedules API — CRUD + pause/resume + run history endpoints.

All mutations immediately sync APScheduler via ScheduleService,
which delegates to DynamicSchedulerService.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_db, get_schedule_service
from app.core.exceptions import EntityNotFoundError
from app.domain.schemas.schedule import (
    RunHistoryResponse,
    ScheduleCreate,
    ScheduleHistoryPageResponse,
    ScheduleListResponse,
    ScheduleResponse,
    ScheduleUpdate,
)
from app.domain.services.schedule import ScheduleService

router = APIRouter()


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


@router.get("", response_model=List[ScheduleResponse])
def list_schedules(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=500, description="Max records to return"),
    service: ScheduleService = Depends(get_schedule_service),
) -> List[ScheduleResponse]:
    """Return all schedules with recent run history (last 20)."""
    schedules = service.list_schedules(skip=skip, limit=limit)
    results = []
    for s in schedules:
        resp = ScheduleResponse.from_orm(s)
        # Limit run_history to 20 to keep list response small
        # Note: 'run_history' is lazy loaded, but access triggers it.
        # Ideally we'd optimize the query, but this suffices for now.
        resp.run_history = [
            RunHistoryResponse.from_orm(h)
            for h in (s.run_history[:20] if s.run_history else [])
        ]
        results.append(resp)
    return results


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


@router.post("", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
def create_schedule(
    data: ScheduleCreate,
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleResponse:
    """
    Create a new schedule.

    If status=ACTIVE the cron job is registered in APScheduler immediately.
    """
    try:
        schedule = service.create_schedule(data)
        return ScheduleResponse.from_orm(schedule)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A schedule named '{data.name}' already exists",
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        )


# ---------------------------------------------------------------------------
# Get by ID
# ---------------------------------------------------------------------------


@router.get("/{schedule_id}", response_model=ScheduleResponse)
def get_schedule(
    schedule_id: int,
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleResponse:
    """Return a single schedule including the last 20 run history entries."""
    try:
        schedule = service.get_schedule(schedule_id)
        response = ScheduleResponse.from_orm(schedule)
        # Limit run_history to 20 on the detail endpoint; /history for full pagination
        response.run_history = response.run_history[:20]
        return response
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


@router.put("/{schedule_id}", response_model=ScheduleResponse)
def update_schedule(
    schedule_id: int,
    data: ScheduleUpdate,
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleResponse:
    """
    Update schedule fields.

    The old APScheduler job is removed and a new one registered immediately
    with the updated cron expression / status.
    """
    try:
        schedule = service.update_schedule(schedule_id, data)
        return ScheduleResponse.from_orm(schedule)
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_schedule(
    schedule_id: int,
    service: ScheduleService = Depends(get_schedule_service),
) -> None:
    """
    Delete a schedule and all its run history.

    APScheduler job is removed first to prevent orphaned execution.
    """
    try:
        service.delete_schedule(schedule_id)
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )


# ---------------------------------------------------------------------------
# Pause
# ---------------------------------------------------------------------------


@router.post("/{schedule_id}/pause", response_model=ScheduleResponse)
def pause_schedule(
    schedule_id: int,
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleResponse:
    """
    Pause a schedule — sets status=PAUSED and removes APScheduler job.
    The schedule is preserved in DB and can be resumed.
    """
    try:
        schedule = service.pause_schedule(schedule_id)
        resp = ScheduleResponse.from_orm(schedule)
        resp.run_history = []
        return resp
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )


# ---------------------------------------------------------------------------
# Resume
# ---------------------------------------------------------------------------


@router.post("/{schedule_id}/resume", response_model=ScheduleResponse)
def resume_schedule(
    schedule_id: int,
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleResponse:
    """
    Resume a paused schedule — sets status=ACTIVE and re-registers job in APScheduler.
    """
    try:
        schedule = service.resume_schedule(schedule_id)
        resp = ScheduleResponse.from_orm(schedule)
        resp.run_history = []
        return resp
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )


# ---------------------------------------------------------------------------
# Paginated Run History
# ---------------------------------------------------------------------------


@router.get("/{schedule_id}/history", response_model=ScheduleHistoryPageResponse)
def get_run_history(
    schedule_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    service: ScheduleService = Depends(get_schedule_service),
) -> ScheduleHistoryPageResponse:
    """Return paginated execution history for a schedule."""
    try:
        items = service.get_run_history(schedule_id, skip=skip, limit=limit)
        total = service.count_run_history(schedule_id)
        return ScheduleHistoryPageResponse(
            items=[RunHistoryResponse.from_orm(r) for r in items],
            total=total,
            skip=skip,
            limit=limit,
        )
    except EntityNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule {schedule_id} not found",
        )
