"""
Pydantic schemas for Schedule API request/response validation.
"""

import re
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, validator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CRON_RE = re.compile(
    r"^(\*|([0-9]|[1-5][0-9])(/[0-9]+)?|(\*\/[0-9]+))\s+"  # minute
    r"(\*|([0-9]|1[0-9]|2[0-3])(/[0-9]+)?|(\*\/[0-9]+))\s+"  # hour
    r"(\*|([1-9]|[12][0-9]|3[01])(/[0-9]+)?|(\*\/[0-9]+))\s+"  # day-of-month
    r"(\*|([1-9]|1[0-2])(/[0-9]+)?|(\*\/[0-9]+))\s+"  # month
    r"(\*|[0-7](/[0-9]+)?|(\*\/[0-7]))$"  # day-of-week
)

VALID_TASK_TYPES = {"FLOW_TASK", "LINKED_TASK"}
VALID_STATUSES = {"ACTIVE", "PAUSED"}

# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------


class RunHistoryResponse(BaseModel):
    id: int
    schedule_id: int
    task_type: str
    task_id: int
    triggered_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    status: str
    message: Optional[str] = None

    class Config:
        orm_mode = True


# ---------------------------------------------------------------------------
# Schedule Request Schemas
# ---------------------------------------------------------------------------


class ScheduleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    task_type: str
    task_id: int = Field(..., gt=0)
    cron_expression: str
    status: str = "ACTIVE"

    @validator("name")
    def name_no_whitespace(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Name must not contain spaces")
        return v.strip()

    @validator("task_type")
    def task_type_valid(cls, v: str) -> str:
        if v not in VALID_TASK_TYPES:
            raise ValueError(f"task_type must be one of {VALID_TASK_TYPES}")
        return v

    @validator("status")
    def status_valid(cls, v: str) -> str:
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}")
        return v

    @validator("cron_expression")
    def cron_valid(cls, v: str) -> str:
        v = v.strip()
        parts = v.split()
        if len(parts) != 5:
            raise ValueError(
                "cron_expression must be a 5-part crontab string (minute hour day month weekday)"
            )
        return v


class ScheduleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    task_type: Optional[str] = None
    task_id: Optional[int] = Field(None, gt=0)
    cron_expression: Optional[str] = None
    status: Optional[str] = None

    @validator("name")
    def name_no_whitespace(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and " " in v:
            raise ValueError("Name must not contain spaces")
        return v.strip() if v else v

    @validator("task_type")
    def task_type_valid(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_TASK_TYPES:
            raise ValueError(f"task_type must be one of {VALID_TASK_TYPES}")
        return v

    @validator("status")
    def status_valid(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}")
        return v

    @validator("cron_expression")
    def cron_valid(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            parts = v.split()
            if len(parts) != 5:
                raise ValueError("cron_expression must be a 5-part crontab string")
        return v


# ---------------------------------------------------------------------------
# Schedule Response Schemas
# ---------------------------------------------------------------------------


class ScheduleListResponse(BaseModel):
    """Lightweight response for the list page (no run history)."""

    id: int
    name: str
    description: Optional[str] = None
    task_type: str
    task_id: int
    cron_expression: str
    status: str
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class ScheduleResponse(ScheduleListResponse):
    """Full response including the latest run history records."""

    run_history: List[RunHistoryResponse] = []

    class Config:
        orm_mode = True


class ScheduleHistoryPageResponse(BaseModel):
    """Paginated run history response."""

    items: List[RunHistoryResponse]
    total: int
    skip: int
    limit: int
