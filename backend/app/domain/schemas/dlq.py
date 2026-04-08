"""
DLQ schemas for queue inspection and discard actions.

Backs the DLQ Manager page with queue summaries, message previews,
and destructive discard operations.
"""

from datetime import datetime
from typing import Any

from pydantic import Field

from app.domain.schemas.common import BaseSchema


class DLQQueueSummary(BaseSchema):
    """Flattened DLQ queue summary enriched with pipeline metadata."""

    pipeline_id: int | None = Field(default=None, description="Pipeline identifier")
    pipeline_name: str | None = Field(default=None, description="Pipeline name")
    source_id: int = Field(..., description="Source identifier")
    source_name: str | None = Field(default=None, description="Source name")
    destination_id: int = Field(..., description="Destination identifier")
    destination_name: str | None = Field(default=None, description="Destination name")
    table_name: str = Field(..., description="Source table name")
    table_name_target: str | None = Field(
        default=None,
        description="Target table name from the DLQ payload",
    )
    message_count: int = Field(..., ge=0, description="Current queue message count")
    oldest_failed_at: datetime | None = Field(
        default=None,
        description="Timestamp of the oldest message in the queue",
    )
    newest_failed_at: datetime | None = Field(
        default=None,
        description="Timestamp of the newest message in the queue",
    )


class DLQQueueListResponse(BaseSchema):
    """Response payload for DLQ queue discovery."""

    items: list[DLQQueueSummary] = Field(
        default_factory=list,
        description="Queue summaries matching the current filters",
    )
    total_messages: int = Field(..., ge=0, description="Total messages in result set")
    total_queues: int = Field(..., ge=0, description="Total queues in result set")
    total_pipelines: int = Field(
        ...,
        ge=0,
        description="Distinct pipeline count in result set",
    )
    total_destinations: int = Field(
        ...,
        ge=0,
        description="Distinct destination count in result set",
    )


class DLQMessageResponse(BaseSchema):
    """Single DLQ message preview item."""

    message_id: str = Field(..., description="Redis Stream entry id")
    operation: str | None = Field(default=None, description="CDC operation code")
    event_timestamp: datetime | None = Field(
        default=None,
        description="Source CDC event timestamp when available",
    )
    first_failed_at: datetime | None = Field(
        default=None,
        description="Timestamp of the first DLQ failure",
    )
    retry_count: int = Field(default=0, ge=0, description="DLQ retry count")
    table_name: str = Field(..., description="Source table name")
    table_name_target: str | None = Field(
        default=None,
        description="Target table name from table sync config",
    )
    key: dict[str, Any] | None = Field(default=None, description="CDC key payload")
    value: dict[str, Any] | None = Field(default=None, description="CDC value payload")
    schema_payload: dict[str, Any] | None = Field(
        default=None,
        alias="schema",
        serialization_alias="schema",
        description="CDC schema payload when present",
    )
    table_sync_config: dict[str, Any] | None = Field(
        default=None,
        description="Serialized table sync configuration stored in the message",
    )


class DLQMessagesResponse(BaseSchema):
    """Cursor-based DLQ message preview page."""

    items: list[DLQMessageResponse] = Field(
        default_factory=list,
        description="Messages for the selected queue, newest first",
    )
    next_before_id: str | None = Field(
        default=None,
        description="Cursor to request older messages",
    )
    total_count: int = Field(..., ge=0, description="Total messages in the queue")


class DLQQueueIdentifier(BaseSchema):
    """Queue identifier used by preview and discard APIs."""

    source_id: int = Field(..., gt=0, description="Source identifier")
    destination_id: int = Field(..., gt=0, description="Destination identifier")
    table_name: str = Field(..., min_length=1, description="Source table name")


class DLQDiscardMessagesRequest(DLQQueueIdentifier):
    """Request body for deleting selected DLQ messages."""

    message_ids: list[str] = Field(
        ...,
        min_length=1,
        description="Redis Stream entry ids to discard",
    )


class DLQDiscardResponse(BaseSchema):
    """Response payload for row or queue discard actions."""

    discarded_count: int = Field(..., ge=0, description="Number of discarded rows")


class DLQPipelineDiscardResponse(DLQDiscardResponse):
    """Response payload for pipeline-wide DLQ discard."""

    queues_cleared: int = Field(..., ge=0, description="Number of queue keys removed")
