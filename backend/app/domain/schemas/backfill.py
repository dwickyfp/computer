"""
Backfill data schemas - Pydantic validation models.

Request/response schemas for backfill operations.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator

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

    @validator("filters")
    def validate_filters(cls, v):
        """Validate filters list."""
        if v and len(v) > 5:
            raise ValueError("Maximum 5 filters allowed")
        return v

    def get_filter_sql(self) -> Optional[str]:
        """Convert filters to SQL WHERE clause format, semicolon separated."""
        if not self.filters:
            return None

        clauses = []
        for f in self.filters:
            # Basic SQL injection prevention - in production add more validation
            clean_column = f.column.replace(";", "").replace("--", "")
            clean_value = f.value.replace(";", "").replace("--", "")

            if f.operator.upper() in ["LIKE", "ILIKE"]:
                clauses.append(f"{clean_column} {f.operator.upper()} '{clean_value}'")
            elif f.operator.upper() in ["IS NULL", "IS NOT NULL"]:
                clauses.append(f"{clean_column} {f.operator.upper()}")
            elif f.operator.upper() == "IN":
                # IN operator: values are comma-separated, wrap in parentheses
                values = [v.strip() for v in clean_value.split(",") if v.strip()]
                quoted_values = []
                for v in values:
                    try:
                        float(v)
                        quoted_values.append(v)
                    except ValueError:
                        quoted_values.append(f"'{v}'")
                clauses.append(
                    f"{clean_column} IN ({', '.join(quoted_values)})"
                )
            else:
                # For numeric comparisons, don't quote
                try:
                    float(clean_value)
                    clauses.append(f"{clean_column} {f.operator} {clean_value}")
                except ValueError:
                    # String value, quote it
                    clauses.append(f"{clean_column} {f.operator} '{clean_value}'")

        return ";".join(clauses) if clauses else None


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

    class Config:
        """Pydantic config."""

        orm_mode = True
        json_encoders = {datetime: lambda v: v.isoformat()}


class BackfillJobListResponse(BaseModel):
    """List of backfill jobs."""

    total: int
    items: list[BackfillJobResponse]


class BackfillJobCancelRequest(BaseModel):
    """Cancel backfill job request."""

    reason: Optional[str] = Field(
        None, max_length=500, description="Cancellation reason"
    )
