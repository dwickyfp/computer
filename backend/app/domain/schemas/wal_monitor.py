"""
WAL Monitor schemas for API requests and responses.

Pydantic models for validation and serialization.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from app.domain.schemas.source import SourceResponse


class WALMonitorBase(BaseModel):
    """Base schema for WAL monitor with common fields."""

    wal_lsn: Optional[str] = Field(None, description="Log Sequence Number")
    wal_position: Optional[int] = Field(
        None, description="WAL position as numeric value"
    )
    last_wal_received: Optional[datetime] = Field(
        None, description="Last WAL receive time"
    )
    last_transaction_time: Optional[datetime] = Field(
        None, description="Last transaction timestamp"
    )
    replication_slot_name: Optional[str] = Field(
        None, description="Replication slot name"
    )
    replication_lag_bytes: Optional[int] = Field(
        None, description="Replication lag in bytes"
    )
    total_wal_size: Optional[str] = Field(None, description="Total WAL size")
    wal_size_bytes: Optional[int] = Field(None, description="WAL size in bytes for threshold comparison")
    wal_threshold_status: Optional[str] = Field(None, description="Threshold status: OK, WARNING, ERROR")
    status: str = Field(default="ACTIVE", description="Status: ACTIVE, IDLE, ERROR")
    error_message: Optional[str] = Field(None, description="Error details if any")


class WALMonitorCreate(WALMonitorBase):
    """Schema for creating WAL monitor record."""

    source_id: int = Field(..., description="Source identifier")


class WALMonitorUpdate(WALMonitorBase):
    """Schema for updating WAL monitor record (all fields optional)."""

    pass


class WALMonitorStatusUpdate(BaseModel):
    """Schema for updating only status."""

    status: str = Field(..., description="Status: ACTIVE, IDLE, ERROR")
    error_message: Optional[str] = Field(
        None, description="Error message if status is ERROR"
    )


class WALMonitorResponse(WALMonitorBase):
    """Schema for WAL monitor response."""

    id: int = Field(..., description="Monitor record ID")
    source_id: int = Field(..., description="Source identifier")
    source: Optional[SourceResponse] = Field(None, description="Source details")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class WALMonitorListResponse(BaseModel):
    """Schema for list of WAL monitors."""

    monitors: List[WALMonitorResponse] = Field(
        ..., description="List of WAL monitor records"
    )
    total: int = Field(..., description="Total number of records")
