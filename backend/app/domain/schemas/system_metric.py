from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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

    class Config:
        orm_mode = True
