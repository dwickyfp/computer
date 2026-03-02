"""
Flow Task Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving flow task data.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field, field_validator

from app.domain.models.flow_task import (
    FlowTaskNodeStatus,
    FlowTaskRunStatus,
    FlowTaskStatus,
    FlowTaskTriggerType,
)
from app.domain.schemas.common import BaseSchema, TimestampSchema


# --- Node / Edge graph primitives ---------------------------------------------


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

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# --- Flow Task CRUD schemas ---------------------------------------------------


class FlowTaskCreate(BaseSchema):
    """Payload for creating a new flow task."""

    name: str = Field(..., min_length=1, max_length=255, description="Unique flow task name")
    description: Optional[str] = Field(default=None, description="Optional description")
    trigger_type: FlowTaskTriggerType = Field(
        default=FlowTaskTriggerType.MANUAL,
        description="Default trigger type: MANUAL or SCHEDULED",
    )

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be blank")
        return v.strip()


class FlowTaskUpdate(BaseSchema):
    """Payload for updating flow task metadata."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None)
    trigger_type: Optional[FlowTaskTriggerType] = Field(default=None)

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("name must not be blank")
        return v.strip() if v else v


class FlowTaskResponse(TimestampSchema):
    """Full flow task representation returned by the API."""

    id: int
    name: str
    description: Optional[str]
    status: FlowTaskStatus
    trigger_type: FlowTaskTriggerType
    last_run_at: Optional[datetime]
    last_run_status: Optional[str]
    last_run_record_count: Optional[int]

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class FlowTaskListResponse(BaseSchema):
    """Paginated list of flow tasks."""

    items: List[FlowTaskResponse]
    total: int
    page: int
    page_size: int


# --- Graph save / load schemas ------------------------------------------------


class FlowTaskGraphSave(BaseSchema):
    """Payload for saving (upserting) a flow graph."""

    nodes: List[FlowNode] = Field(default_factory=list, description="ReactFlow nodes")
    edges: List[FlowEdge] = Field(default_factory=list, description="ReactFlow edges")


class FlowTaskGraphSaveWithSummary(FlowTaskGraphSave):
    """Graph save that also creates a version snapshot."""

    change_summary: Optional[str] = Field(
        default=None, description="Optional summary of what changed"
    )


class FlowTaskGraphResponse(TimestampSchema):
    """Persisted graph returned by the API."""

    id: int
    flow_task_id: int
    nodes_json: List[Any]
    edges_json: List[Any]
    version: int

    model_config = ConfigDict(from_attributes=True)


# --- Graph versioning schemas -------------------------------------------------


class FlowTaskGraphVersionResponse(BaseSchema):
    """A single versioned graph snapshot."""

    id: int
    flow_task_id: int
    version: int
    nodes_json: List[Any]
    edges_json: List[Any]
    change_summary: Optional[str]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FlowTaskGraphVersionListResponse(BaseSchema):
    """Paginated version list."""

    items: List[FlowTaskGraphVersionResponse]
    total: int
    page: int
    page_size: int


# --- Run history schemas ------------------------------------------------------


class FlowTaskRunNodeLogResponse(BaseSchema):
    """Per-node execution stats for a single flow run."""

    id: int
    run_history_id: int
    flow_task_id: int
    node_id: str
    node_type: str
    node_label: Optional[str]
    row_count_in: Optional[int]
    row_count_out: Optional[int]
    duration_ms: Optional[int]
    status: FlowTaskNodeStatus
    error_message: Optional[str]

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class FlowTaskRunHistoryResponse(BaseSchema):
    """Full run history record including per-node logs."""

    id: int
    flow_task_id: int
    trigger_type: FlowTaskTriggerType
    status: FlowTaskRunStatus
    celery_task_id: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    error_message: Optional[str]
    total_input_records: Optional[int]
    total_output_records: Optional[int]
    run_metadata: Optional[Dict[str, Any]]
    node_logs: List[FlowTaskRunNodeLogResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class FlowTaskRunHistoryListResponse(BaseSchema):
    """Paginated run history list."""

    items: List[FlowTaskRunHistoryResponse]
    total: int
    page: int
    page_size: int


# --- Trigger / status response schemas ----------------------------------------


class FlowTaskTriggerResponse(BaseSchema):
    """Response returned when a flow task run is triggered."""

    run_id: int
    celery_task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseSchema):
    """Celery task status for polling a run or preview task."""

    state: str = Field(
        ..., description="PENDING | STARTED | PROGRESS | SUCCESS | FAILURE | UNKNOWN"
    )
    result: Optional[Any] = Field(default=None, description="Task result payload on SUCCESS")
    error: Optional[str] = Field(default=None, description="Error message on FAILURE")
    progress: Optional[Dict[str, Any]] = Field(default=None, description="Progress metadata")


# --- D8: Watermark schemas ----------------------------------------------------


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

    model_config = ConfigDict(from_attributes=True)


class FlowTaskWatermarkConfig(BaseSchema):
    """Config for setting a watermark on an input node."""

    node_id: str = Field(..., description="Input node ID")
    watermark_column: str = Field(..., description="Column to track")
    watermark_type: str = Field(default="TIMESTAMP", description="TIMESTAMP or INTEGER")


# --- Node preview / schema schemas --------------------------------------------


class NodePreviewRequest(BaseSchema):
    """
    Payload for node preview and node-schema endpoints.

    Accepts the current (possibly unsaved) graph snapshot so preview
    works before the graph is persisted.
    """

    node_id: str = Field(..., description="Target node ID to preview/resolve schema for")
    nodes: List[FlowNode] = Field(..., description="Full node list from the canvas")
    edges: List[FlowEdge] = Field(..., description="Full edge list from the canvas")
    limit: int = Field(default=500, ge=1, le=5000, description="Max preview rows")


class NodePreviewTaskResponse(BaseSchema):
    """Response after submitting a preview task to Celery."""

    task_id: str
    status: str
    message: str


class ColumnInfo(BaseSchema):
    """Single column name + type descriptor."""

    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column data type (DuckDB type string)")


class NodeColumnsResponse(BaseSchema):
    """Resolved output schema for a node."""

    columns: List[ColumnInfo] = Field(default_factory=list)