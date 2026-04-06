"""
Common schemas used across the application.

Provides reusable Pydantic models for API responses.
"""

from datetime import datetime
from typing import Any, Generic, TypeVar
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class BaseSchema(BaseModel):
    """
    Base schema with common configuration.

    All schemas should inherit from this.
    """

    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=True,
        populate_by_name=True,
    )


class TimestampSchema(BaseSchema):
    """Schema mixin for timestamp fields."""

    created_at: datetime = Field(..., description="Record creation timestamp")
    updated_at: datetime = Field(..., description="Record last update timestamp")


class PaginatedResponse(BaseSchema, Generic[T]):
    """
    Generic paginated response wrapper.

    Used for list endpoints to provide pagination metadata.
    """

    items: list[T] = Field(..., description="List of items in current page")
    total: int = Field(..., ge=0, description="Total number of items across all pages")
    page: int = Field(..., ge=1, description="Current page number (1-indexed)")
    page_size: int = Field(..., ge=1, le=100, description="Number of items per page")

    @property
    def total_pages(self) -> int:
        """Calculate total number of pages."""
        return (self.total + self.page_size - 1) // self.page_size

    @property
    def has_next(self) -> bool:
        """Check if there is a next page."""
        return self.page < self.total_pages

    @property
    def has_previous(self) -> bool:
        """Check if there is a previous page."""
        return self.page > 1


class HealthResponse(BaseSchema):
    """
    Health check response.

    Indicates application and dependency health status.
    """

    status: str = Field(
        ..., description="Overall health status: 'healthy' or 'unhealthy'"
    )
    version: str = Field(..., description="Application version")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        description="Health check timestamp",
    )
    checks: dict[str, bool] = Field(
        default_factory=dict, description="Individual component health checks"
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "timestamp": "2024-01-01T00:00:00Z",
                "checks": {"database": True, "wal_monitor": True},
            }
        },
    )


class ErrorResponse(BaseSchema):
    """
    Standard error response format.

    Used by exception handlers for consistent error responses.
    """

    error: str = Field(..., description="Error type/code")
    message: str = Field(..., description="Human-readable error message")
    details: dict[str, Any] = Field(
        default_factory=dict, description="Additional error context"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo('Asia/Jakarta')),
        description="Error timestamp",
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "error": "EntityNotFoundError",
                "message": "Source with id '123' not found",
                "details": {"entity_type": "Source", "entity_id": "123"},
                "timestamp": "2024-01-01T00:00:00Z",
            }
        },
    )


class TaskDispatchResponse(BaseSchema):
    """Asynchronous worker task dispatch acknowledgement."""

    message: str = Field(..., description="Dispatch status message")
    task_id: str | None = Field(
        default=None,
        description="Celery task identifier when the worker accepted the job",
    )
