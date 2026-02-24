"""
WAL Metric Pydantic schemas for request/response validation.

Defines schemas for querying and retrieving WAL metrics.
"""

from datetime import datetime

from pydantic import Field, validator

from app.domain.schemas.common import BaseSchema


class WALMetricResponse(BaseSchema):
    """
    Schema for WAL metric API responses.

    Contains WAL size measurement data.
    """

    id: int = Field(..., description="Unique metric identifier", examples=[1, 42])
    source_id: int = Field(
        ..., description="ID of the monitored source", examples=[1, 42]
    )
    size_bytes: int = Field(
        ..., ge=0, description="WAL size in bytes", examples=[1024000000, 5368709120]
    )
    size_mb: float = Field(
        ...,
        ge=0.0,
        description="WAL size in megabytes (calculated)",
        examples=[976.56, 5120.0],
    )
    size_gb: float = Field(
        ...,
        ge=0.0,
        description="WAL size in gigabytes (calculated)",
        examples=[0.95, 5.0],
    )
    recorded_at: datetime = Field(
        ...,
        description="When this metric was recorded",
        examples=["2024-01-01T00:00:00Z"],
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "source_id": 1,
                "size_bytes": 1073741824,
                "size_mb": 1024.0,
                "size_gb": 1.0,
                "recorded_at": "2024-01-01T00:00:00Z",
            }
        }


class WALMetricsQuery(BaseSchema):
    """
    Schema for querying WAL metrics.

    Supports filtering by source and time range.
    """

    source_id: int | None = Field(
        default=None,
        ge=1,
        description="Filter by source ID (optional)",
        examples=[1, 42],
    )
    start_date: datetime | None = Field(
        default=None,
        description="Start of time range (optional)",
        examples=["2024-01-01T00:00:00Z"],
    )
    end_date: datetime | None = Field(
        default=None,
        description="End of time range (optional)",
        examples=["2024-01-31T23:59:59Z"],
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of metrics to return",
        examples=[100, 500],
    )

    @validator("end_date")
    def validate_date_range(cls, v: datetime | None, values) -> datetime | None:
        """Validate that end_date is after start_date."""
        start_date = values.get("start_date")
        if v is not None and start_date is not None:
            if v <= start_date:
                raise ValueError("end_date must be after start_date")
        return v

    class Config:
        schema_extra = {
            "example": {
                "source_id": 1,
                "start_date": "2024-01-01T00:00:00Z",
                "end_date": "2024-01-31T23:59:59Z",
                "limit": 100,
            }
        }
