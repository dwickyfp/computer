"""
Flow Task Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving flow task data.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from app.domain.models.flow_task import (
    FlowTaskNodeStatus,
    FlowTaskRunStatus,
    FlowTaskStatus,
    FlowTaskTriggerType,
)
from app.domain.schemas.common import BaseSchema, TimestampSchema


# ─── Node / Edge graph primitives ─────────────────────────────────────────────

class NodePosition(BaseSchema):
    """ReactFlow node coordinates."""

    x: float = Field(..., description="X position on the canvas")
    y: float = Field(..., description="Y position on the canvas")


class FlowNode(BaseSchema):
    """
    A single ReactFlow node with its config data.

    The `data` dict is node-type specific:
    - input:     {source_type, source_id, table_name, schema_name, sample_limit}
    - clean:     {filters, renames, calculations, group_replace}
    - aggregate: {group_by, aggregations: [{column, func, alias}]}
    - join:      {join_type, left_key, right_key, output_columns}
    - union:     {stack_mode}   # UNION or UNION ALL
    - pivot:     {direction, pivot_column, value_column, group_columns, agg_func}
    - new_rows:  {generate_type, start, end, step, alias}
    - output:    {target_table, schema_name, write_mode, upsert_keys, destination_id}
    """

    id: str = Field(..., description="Unique node ID (ReactFlow generated)")
    type: str = Field(
        ...,
        description="Node type: input|clean|aggregate|join|union|pivot|new_rows|output",
    )
    position: NodePosition = Field(..., description="Canvas coordinates")
    data: Dict[str, Any] = Field(default_factory=dict, description="Node configuration")
    label: Optional[str] = Field(default=None, description="Optional display label")


class FlowEdge(BaseSchema):
    """A ReactFlow edge connecting two nodes."""

    id: str = Field(..., description="Unique edge ID")
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    source_handle: Optional[str] = Field(default=None, alias="sourceHandle")
    target_handle: Optional[str] = Field(default=None, alias="targetHandle")

    class Config:
        populate_by_name = True
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


# ─── Flow Task CRUD schemas ────────────────────────────────────────────────────

class FlowTaskCreate(BaseSchema):
    """Schema for creating a new flow task."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique flow task name",
        examples=["daily-customer-transform"],
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional description",
    )
    trigger_type: FlowTaskTriggerType = Field(
        default=FlowTaskTriggerType.MANUAL,
        description="Default trigger type",
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate name format."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("Name cannot be empty")
        return stripped


class FlowTaskUpdate(BaseSchema):
    """Schema for updating an existing flow task."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    trigger_type: Optional[FlowTaskTriggerType] = None

    @validator("name")
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return v.strip()
        return v


class FlowTaskResponse(TimestampSchema):
    """Full flow task response."""

    id: int
    name: str
    description: Optional[str]
    status: str
    trigger_type: str
    last_run_at: Optional[datetime]
    last_run_status: Optional[str]
    last_run_record_count: Optional[int]

    class Config:
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


class FlowTaskListResponse(BaseSchema):
    """Paginated list of flow tasks."""

    items: List[FlowTaskResponse]
    total: int
    page: int
    page_size: int


# ─── Graph schemas ─────────────────────────────────────────────────────────────

class FlowTaskGraphSave(BaseSchema):
    """Payload to save the current graph state."""

    nodes: List[FlowNode] = Field(default_factory=list)
    edges: List[FlowEdge] = Field(default_factory=list)


class FlowTaskGraphResponse(TimestampSchema):
    """Saved graph returned from the API."""

    id: int
    flow_task_id: int
    nodes_json: List[Dict[str, Any]]
    edges_json: List[Dict[str, Any]]
    version: int

    class Config:
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


# ─── Run History schemas ───────────────────────────────────────────────────────

class FlowTaskRunNodeLogResponse(TimestampSchema):
    """Per-node execution log entry."""

    id: int
    run_history_id: int
    flow_task_id: int
    node_id: str
    node_type: str
    node_label: Optional[str]
    row_count_in: Optional[int]
    row_count_out: Optional[int]
    duration_ms: Optional[int]
    status: str
    error_message: Optional[str]

    class Config:
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


class FlowTaskRunHistoryResponse(TimestampSchema):
    """Full run history record with nested node logs."""

    id: int
    flow_task_id: int
    trigger_type: str
    status: str
    celery_task_id: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    error_message: Optional[str]
    total_input_records: Optional[int]
    total_output_records: Optional[int]
    run_metadata: Optional[Dict[str, Any]]
    node_logs: List[FlowTaskRunNodeLogResponse] = Field(default_factory=list)

    class Config:
        orm_mode = True
        use_enum_values = True
        allow_population_by_field_name = True


class FlowTaskRunHistoryListResponse(BaseSchema):
    """Paginated run history."""

    items: List[FlowTaskRunHistoryResponse]
    total: int
    page: int
    page_size: int


# ─── Trigger / Preview schemas ─────────────────────────────────────────────────

class FlowTaskTriggerResponse(BaseSchema):
    """Response after triggering a run."""

    run_id: int
    celery_task_id: str
    status: str = "RUNNING"
    message: str = "Flow task execution started"


class NodePreviewRequest(BaseSchema):
    """Request to preview data at a specific node (before saving)."""

    node_id: str = Field(..., description="Target node ID to preview")
    nodes: List[FlowNode] = Field(
        ..., description="Current graph nodes (unsaved snapshot)"
    )
    edges: List[FlowEdge] = Field(
        ..., description="Current graph edges (unsaved snapshot)"
    )
    limit: int = Field(default=500, ge=1, le=2000, description="Row limit for preview")


class NodePreviewTaskResponse(BaseSchema):
    """Response after submitting a node preview task."""

    task_id: str
    status: str = "PENDING"
    message: str = "Preview task submitted"


class TaskStatusResponse(BaseSchema):
    """Generic Celery task status response."""

    task_id: str
    state: str
    status: str
    result: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ─── Node schema (DuckDB-derived) ─────────────────────────────────────────────

class ColumnInfo(BaseSchema):
    """A single column descriptor returned by the node-schema endpoint."""

    column_name: str
    data_type: str


class NodeColumnsResponse(BaseSchema):
    """Schema response from POST /{flow_task_id}/node-schema."""

    columns: List[ColumnInfo]


# ─── D4: Graph version schemas ────────────────────────────────────────────────

class FlowTaskGraphVersionResponse(BaseSchema):
    """Response for a single graph version snapshot."""

    id: int
    flow_task_id: int
    version: int
    nodes_json: List[Dict[str, Any]]
    edges_json: List[Dict[str, Any]]
    change_summary: Optional[str]
    created_at: datetime

    class Config:
        orm_mode = True


class FlowTaskGraphVersionListResponse(BaseSchema):
    """Paginated version list."""

    items: List[FlowTaskGraphVersionResponse]
    total: int
    page: int
    page_size: int


class FlowTaskGraphSaveWithSummary(FlowTaskGraphSave):
    """Graph save that also creates a version snapshot."""

    change_summary: Optional[str] = Field(
        default=None, description="Optional summary of what changed"
    )


# ─── D8: Watermark schemas ────────────────────────────────────────────────────

class FlowTaskWatermarkResponse(BaseSchema):
    """Flow task watermark entry."""

    id: int
    flow_task_id: int
    node_id: str
    watermark_column: str
    last_watermark_value: Optional[str]
    watermark_type: str
    last_run_at: Optional[datetime]
    record_count: int

    class Config:
        orm_mode = True


class FlowTaskWatermarkConfig(BaseSchema):
    """Config for setting a watermark on an input node."""

    node_id: str = Field(..., description="Input node ID")
    watermark_column: str = Field(..., description="Column to track")
    watermark_type: str = Field(default="TIMESTAMP", description="TIMESTAMP or INTEGER")
