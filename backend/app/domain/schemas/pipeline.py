"""
Pipeline Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving pipeline configurations.
"""

from datetime import datetime

from pydantic import Field, validator
from typing import List

from app.domain.models.pipeline import PipelineMetadataStatus, PipelineStatus
from app.domain.schemas.common import BaseSchema, TimestampSchema
from app.domain.schemas.destination import DestinationResponse
from app.domain.schemas.source import SourceResponse


class PipelineBase(BaseSchema):
    """Base pipeline schema with common fields."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique pipeline name",
        examples=["prod-to-snowflake", "analytics-sync"],
    )


class PipelineCreate(PipelineBase):
    """
    Schema for creating a new pipeline.
    """

    source_id: int = Field(
        ..., ge=1, description="ID of the source database", examples=[1, 42]
    )
    status: PipelineStatus = Field(
        default=PipelineStatus.START,
        description="Initial pipeline status",
        examples=["START", "PAUSE"],
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate pipeline name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Pipeline name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    class Config:
        schema_extra = {
            "example": {
                "name": "production-to-snowflake",
                "source_id": 1,
                "status": "START",
            }
        }


class PipelineUpdate(BaseSchema):
    """
    Schema for updating an existing pipeline.

    All fields are optional to support partial updates.
    """

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="Unique pipeline name"
    )
    source_id: int | None = Field(
        default=None, ge=1, description="ID of the source database"
    )
    status: PipelineStatus | None = Field(default=None, description="Pipeline status")

    @validator("name")
    def validate_name(cls, v: str | None) -> str | None:
        """Validate pipeline name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Pipeline name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class PipelineStatusUpdate(BaseSchema):
    """
    Schema for updating pipeline status.

    Used for start/pause/refresh operations.
    """

    status: PipelineStatus = Field(
        ..., description="New pipeline status", examples=["START", "PAUSE", "REFRESH"]
    )

    class Config:
        schema_extra = {"example": {"status": "START"}}


class PipelineMetadataResponse(BaseSchema):
    """
    Schema for pipeline metadata responses.

    Contains runtime status and error information.
    """

    id: int = Field(..., description="Unique metadata identifier")
    pipeline_id: int = Field(..., description="Associated pipeline ID")
    status: PipelineMetadataStatus = Field(..., description="Runtime status")
    last_error: str | None = Field(default=None, description="Last error message")
    last_error_at: datetime | None = Field(
        default=None, description="Timestamp of last error"
    )
    last_start_at: datetime | None = Field(
        default=None, description="Timestamp of last pipeline start"
    )
    created_at: datetime = Field(..., description="Metadata creation timestamp")
    updated_at: datetime = Field(..., description="Metadata last update timestamp")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "pipeline_id": 1,
                "status": "RUNNING",
                "last_error": None,
                "last_error_at": None,
                "last_start_at": "2024-01-01T00:00:00Z",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }


class PipelineProgressResponse(BaseSchema):
    """
    Schema for pipeline progress.
    """

    id: int = Field(..., description="Unique progress identifier")
    pipeline_id: int = Field(..., description="Associated pipeline ID")
    progress: int = Field(..., description="Progress percentage")
    step: str | None = Field(default=None, description="Current step")
    status: str = Field(..., description="Progress status")
    details: str | None = Field(default=None, description="Additional details")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class PipelineDestinationTableSyncResponse(BaseSchema):
    """
    Schema for pipeline destination table sync.
    """

    id: int = Field(..., description="Unique table sync identifier")
    pipeline_destination_id: int = Field(..., description="Pipeline destination ID")
    table_name: str = Field(..., description="Source table name")
    table_name_target: str = Field(..., description="Target table name")
    custom_sql: str | None = Field(default=None, description="Custom SQL")
    filter_sql: str | None = Field(default=None, description="Filter SQL")
    primary_key_column_target: str | None = Field(
        default=None, description="Custom primary key columns (semicolon-separated)"
    )

    # Snowflake Status Flags
    is_exists_table_landing: bool = Field(
        default=False, description="Landing table exists"
    )
    is_exists_stream: bool = Field(default=False, description="Stream exists")
    is_exists_task: bool = Field(default=False, description="Task exists")
    is_exists_table_destination: bool = Field(
        default=False, description="Target table exists"
    )

    is_error: bool = Field(default=False, description="Whether sync is in error state")
    error_message: str | None = Field(
        default=None, description="Error message if in error state"
    )

    # Lineage fields
    lineage_metadata: dict | None = Field(
        default=None, description="Column-level lineage data"
    )
    lineage_status: str | None = Field(
        default="PENDING",
        description="Lineage generation status: PENDING, GENERATING, COMPLETED, FAILED",
    )
    lineage_error: str | None = Field(
        default=None, description="Lineage generation error message"
    )
    lineage_generated_at: datetime | None = Field(
        default=None, description="Timestamp when lineage was last generated"
    )

    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class TableSyncCreateRequest(BaseSchema):
    """
    Schema for creating/updating a table sync configuration.
    """

    id: int | None = Field(default=None, description="Sync ID (optional, for updates)")
    table_name: str = Field(
        ..., min_length=1, max_length=255, description="Source table name"
    )
    table_name_target: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Target table name (defaults to table_name if not provided)",
    )
    custom_sql: str | None = Field(default=None, description="Custom SQL query")
    filter_sql: str | None = Field(
        default=None,
        description="Filter conditions in format: column:operator:value;column2:operator:value2",
    )
    primary_key_column_target: str | None = Field(
        default=None,
        description="Custom primary key columns for PostgreSQL merge (semicolon-separated: key1;key2)",
    )
    enabled: bool = Field(default=True, description="Whether sync is enabled")


class TableSyncBulkRequest(BaseSchema):
    """
    Schema for bulk table sync operations.
    """

    tables: List[TableSyncCreateRequest] = Field(
        ..., min_items=1, description="List of table sync configurations"
    )


class TableValidationRequest(BaseSchema):
    """
    Schema for table name validation request.
    """

    table_name: str = Field(..., description="Target table name to validate")


class TableValidationResponse(BaseSchema):
    """
    Schema for table name validation response.
    """

    valid: bool = Field(..., description="Whether the table name is valid")
    exists: bool = Field(
        ..., description="Whether the table already exists in destination"
    )
    message: str | None = Field(
        default=None, description="Validation message or error details"
    )


class ColumnSchemaResponse(BaseSchema):
    """
    Schema for table column information.
    """

    column_name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="PostgreSQL data type")
    real_data_type: str | None = Field(
        default=None, description="Detailed PostgreSQL data type"
    )
    is_nullable: bool = Field(default=True, description="Whether column is nullable")
    is_primary_key: bool = Field(
        default=False, description="Whether column is primary key"
    )
    has_default: bool = Field(
        default=False, description="Whether column has a default value"
    )
    default_value: str | None = Field(default=None, description="Default value")
    numeric_scale: int | None = Field(default=None, description="Numeric scale")
    numeric_precision: int | None = Field(default=None, description="Numeric precision")


class TableWithSyncInfoResponse(BaseSchema):
    """
    Schema for table with sync configuration info.
    """

    table_name: str = Field(..., description="Table name")
    columns: List[ColumnSchemaResponse] = Field(default=[], description="Column schema")
    sync_configs: List[PipelineDestinationTableSyncResponse] = Field(
        default=[], description="Current sync configurations (branches)"
    )
    # Snowflake status flags (might need to be per-sync/target in future, but keeping simple for now)
    # These flags originally tracked landing/stream/task existence.
    # With branching, landing/stream are shared (per source table), but Tasks/Target Tables are per branch.
    # We'll need to think about how these map. For now, let's keep them as indicative of *at least one* path or the landing setup.
    is_exists_table_landing: bool = Field(
        default=False, description="Landing table exists"
    )
    is_exists_stream: bool = Field(default=False, description="Stream exists")
    is_exists_task: bool = Field(
        default=False, description="Task exists (at least one)"
    )
    is_exists_table_destination: bool = Field(
        default=False, description="Target table exists (at least one)"
    )


class PipelineDestinationResponse(BaseSchema):
    """
    Schema for pipeline destination.
    """

    id: int = Field(..., description="Unique pipeline destination identifier")
    pipeline_id: int = Field(..., description="Pipeline ID")
    destination_id: int = Field(..., description="Destination ID")
    destination: DestinationResponse | None = Field(
        default=None, description="Destination details"
    )
    table_syncs: List[PipelineDestinationTableSyncResponse] = Field(
        default=[], description="Table sync settings"
    )
    # Error tracking
    is_error: bool = Field(
        default=False, description="Whether destination is in error state"
    )
    error_message: str | None = Field(
        default=None, description="Error message if in error state"
    )
    last_error_at: datetime | None = Field(
        default=None, description="Timestamp of last error"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class PipelineResponse(PipelineBase, TimestampSchema):
    """
    Schema for pipeline API responses.

    Includes full pipeline details with related source and destinations.
    """

    id: int = Field(..., description="Unique pipeline identifier", examples=[1, 42])
    source_id: int = Field(..., description="ID of the source database")
    status: PipelineStatus = Field(..., description="Pipeline operational status")
    ready_refresh: bool = Field(
        default=False, description="Flag indicating pipeline needs refresh"
    )
    last_refresh_at: datetime | None = Field(
        default=None, description="Timestamp of last pipeline refresh"
    )

    # Nested relationships
    source: SourceResponse | None = Field(
        default=None, description="Source configuration details"
    )
    destinations: List[PipelineDestinationResponse] = Field(
        default=[], description="List of destinations"
    )
    pipeline_metadata: PipelineMetadataResponse | None = Field(
        default=None, description="Pipeline runtime metadata"
    )
    pipeline_progress: PipelineProgressResponse | None = Field(
        default=None, description="Pipeline initialization progress"
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "name": "production-to-snowflake",
                "source_id": 1,
                "status": "START",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "source": {
                    "id": 1,
                    "name": "production-postgres",
                    "pg_host": "postgres.example.com",
                    "pg_port": 5432,
                    "pg_database": "myapp_production",
                    "pg_username": "replication_user",
                    "publication_name": "dbz_publication",
                    "replication_name": "dbz_replication_slot_1",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "destinations": [
                    {
                        "id": 1,
                        "pipeline_id": 1,
                        "destination_id": 1,
                        "destination": {
                            "id": 1,
                            "name": "snowflake-production",
                            "type": "SNOWFLAKE",
                            "config": {},
                            "created_at": "2024-01-01T00:00:00Z",
                            "updated_at": "2024-01-01T00:00:00Z",
                        },
                        "table_syncs": [],
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
                "pipeline_metadata": {
                    "id": 1,
                    "pipeline_id": 1,
                    "status": "RUNNING",
                    "last_error": None,
                    "last_error_at": None,
                    "last_start_at": "2024-01-01T00:00:00Z",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "pipeline_progress": {
                    "id": 1,
                    "pipeline_id": 1,
                    "progress": 50,
                    "step": "Creating Landing Table",
                    "status": "IN_PROGRESS",
                    "details": None,
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
            }
        }
