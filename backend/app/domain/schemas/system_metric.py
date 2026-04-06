from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class SystemMetricBase(BaseModel):
    cpu_usage: Optional[float] = None
    total_memory: Optional[int] = None
    used_memory: Optional[int] = None
    total_swap: Optional[int] = None
    used_swap: Optional[int] = None


class SystemMetricCreate(SystemMetricBase):
    pass


class SystemMetricResponse(SystemMetricBase):
    id: int
    recorded_at: datetime

    # Calculated fields
    memory_usage_percent: Optional[float] = None
    swap_usage_percent: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)
