from typing import Any, List, Dict, Optional
from pydantic import BaseModel

class PipelinePreviewRequest(BaseModel):
    """Request model for previewing table data or custom SQL."""
    sql: Optional[str] = None
    destination_id: int
    table_name: str
    source_id: int
    filter_sql: Optional[str] = None

class PipelinePreviewResponse(BaseModel):
    """Response model for previewing custom SQL."""
    columns: List[str]
    column_types: List[str]
    data: List[Dict[str, Any]]
    error: Optional[str] = None
