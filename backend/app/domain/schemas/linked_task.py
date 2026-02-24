"""
Pydantic schemas for Linked Task API request/response serialization.
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel


# ─── Shared ──────────────────────────────────────────────────────────────────


class LinkedTaskStepBase(BaseModel):
    flow_task_id: int
    pos_x: float = 0.0
    pos_y: float = 0.0


class LinkedTaskEdgeBase(BaseModel):
    source_step_id: int
    target_step_id: int
    condition: str = "ON_SUCCESS"  # ON_SUCCESS | ALWAYS


# ─── Graph save ──────────────────────────────────────────────────────────────


class LinkedTaskStepSave(BaseModel):
    """Step as sent from the frontend during graph save."""
    id: Optional[Any] = None   # Temporary ID (UUID string or int) used for linking edges
    flow_task_id: int
    pos_x: float = 0.0
    pos_y: float = 0.0


class LinkedTaskEdgeSave(BaseModel):
    """Edge as sent from the frontend during graph save."""
    source_step_id: Any  # Refers to LinkedTaskStepSave.id (UUID string or int)
    target_step_id: Any
    condition: str = "ON_SUCCESS"


class LinkedTaskGraphSave(BaseModel):
    """Full graph payload sent from the canvas."""
    steps: List[LinkedTaskStepSave] = []
    edges: List[LinkedTaskEdgeSave] = []


# ─── CRUD ────────────────────────────────────────────────────────────────────


class LinkedTaskCreate(BaseModel):
    name: str
    description: Optional[str] = None


class LinkedTaskUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


# ─── Responses ───────────────────────────────────────────────────────────────


class FlowTaskRef(BaseModel):
    """Lightweight flow task reference embedded in step responses."""
    id: int
    name: str
    status: str

    class Config:
        orm_mode = True


class LinkedTaskStepResponse(BaseModel):
    id: int
    linked_task_id: int
    flow_task_id: int
    pos_x: float
    pos_y: float
    flow_task: Optional[FlowTaskRef] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class LinkedTaskEdgeResponse(BaseModel):
    id: int
    linked_task_id: int
    source_step_id: int
    target_step_id: int
    condition: str
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class LinkedTaskResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    status: str
    last_run_at: Optional[datetime]
    last_run_status: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class LinkedTaskDetailResponse(LinkedTaskResponse):
    """Full response including steps and edges."""
    steps: List[LinkedTaskStepResponse] = []
    edges: List[LinkedTaskEdgeResponse] = []

    class Config:
        orm_mode = True


class LinkedTaskListResponse(BaseModel):
    items: List[LinkedTaskResponse]
    total: int
    page: int
    page_size: int


# ─── Run History ─────────────────────────────────────────────────────────────


class LinkedTaskRunStepLogResponse(BaseModel):
    id: int
    run_history_id: int
    step_id: int
    flow_task_id: int
    flow_task_run_history_id: Optional[int]
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error_message: Optional[str]
    created_at: datetime

    class Config:
        orm_mode = True


class LinkedTaskRunHistoryResponse(BaseModel):
    id: int
    linked_task_id: int
    trigger_type: str
    status: str
    celery_task_id: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    error_message: Optional[str]
    step_logs: List[LinkedTaskRunStepLogResponse] = []
    created_at: datetime

    class Config:
        orm_mode = True


class LinkedTaskRunHistoryListResponse(BaseModel):
    items: List[LinkedTaskRunHistoryResponse]
    total: int
    page: int
    page_size: int


# ─── Trigger response ─────────────────────────────────────────────────────────


class LinkedTaskTriggerResponse(BaseModel):
    message: str
    run_id: int
    celery_task_id: str
    status: str
