from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class DataFlowRecordBase(BaseModel):
    pipeline_id: int
    source_id: int
    table_name: str
    record_count: int


class DataFlowRecordCreate(DataFlowRecordBase):
    pass


class DataFlowRecord(DataFlowRecordBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class DataFlowStats(BaseModel):
    table_name: str
    total_records: int
    daily_stats: list[dict] # To store daily aggregation
    
    model_config = ConfigDict(from_attributes=True)
