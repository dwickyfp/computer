"""
Source Detail schemas.

Separated from source.py to avoid circular imports with wal_monitor.py
"""

from typing import List, Optional

from pydantic import BaseModel, Field

from app.domain.schemas.source import SourceResponse
from app.domain.schemas.wal_monitor import WALMonitorResponse


class SourceTableInfo(BaseModel):
    """
    Schema for table information in source details.
    """
    id: int
    table_name: str
    version: int = Field(default=1, description="Table schema version")
    schema_definition: Optional[List[dict]] = Field(default=None, alias="schema_table")

    class Config:
        orm_mode = True


class TableSchemaDiff(BaseModel):
    """
    Schema for schema differences/evolution.
    """
    new_columns: List[str] = []
    dropped_columns: List[dict] = []
    type_changes: dict = {} # col_name -> {old_type: str, new_type: str}

class TableSchemaResponse(BaseModel):
    """
    Response schema for table schema with evolution info.
    """
    columns: List[dict]
    diff: Optional[TableSchemaDiff] = None

class SourceDetailResponse(BaseModel):
    """
    Schema for detailed source response.
    
    Includes source info, WAL monitor metrics, and table list.
    """
    source: SourceResponse
    wal_monitor: Optional[WALMonitorResponse] = None
    tables: List[SourceTableInfo] = []
    destinations: List[str] = []

    class Config:
        orm_mode = True
