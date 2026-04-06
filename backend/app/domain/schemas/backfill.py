"""
Backfill data schemas - Pydantic validation models.

Request/response schemas for backfill operations.
"""

from datetime import datetime
import json
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.models.queue_backfill import BackfillStatus


class BackfillFilterCreate(BaseModel):
    """Single filter for backfill."""

    column: str = Field(..., description="Column name to filter")
    operator: str = Field(..., description="SQL operator (=, >, <, LIKE, etc.)")
    value: str = Field(..., description="Filter value")


class BackfillJobCreate(BaseModel):
    """Create backfill job request."""

    table_name: str = Field(
        ..., min_length=1, max_length=255, description="Table name to backfill"
    )
    filters: Optional[list[BackfillFilterCreate]] = Field(
        default=None, max_items=5, description="Filter conditions (max 5)"
    )

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, v):
        """Validate filters list."""
        if v and len(v) > 5:
            raise ValueError("Maximum 5 filters allowed")
        return v

    def get_filter_sql(self) -> Optional[str]:
        """Convert filters to JSON v2 format."""
        if not self.filters:
            return None

        conditions: list[dict[str, str]] = []
        for f in self.filters:
            clean_column = f.column.replace(";", "").replace("--", "")
            clean_value = f.value.replace(";", "").replace("--", "")
            conditions.append(
                {
                    "column": clean_column,
                    "operator": f.operator.upper(),
                    "value": clean_value,
                }
            )

        if not conditions:
            return None

        return json.dumps(
            {
                "version": 2,
                "groups": [{"conditions": conditions, "intraLogic": "AND"}],
                "interLogic": [],
            }
        )


class BackfillJobUpdate(BaseModel):
    """Update backfill job request."""

    status: Optional[str] = Field(None, description="Job status")
    count_record: Optional[int] = Field(None, ge=0, description="Record count")


class BackfillJobResponse(BaseModel):
    """Backfill job response."""

    id: int
    pipeline_id: int
    source_id: int
    table_name: str
    filter_sql: Optional[str] = None
    status: str
    count_record: int
    total_record: int
    is_error: bool
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class BackfillJobListResponse(BaseModel):
    """Paginated backfill job list response."""

    items: list[BackfillJobResponse] = Field(default_factory=list)
    total: int = Field(default=0)
