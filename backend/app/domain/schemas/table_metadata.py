"""
Table Metadata schemas.

Schemas for table metadata and history schema evolution.
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict


class HistorySchemaEvolutionBase(BaseModel):
    """Base schema for HistorySchemaEvolution."""
    schema_table_old: Optional[Any] = None
    schema_table_new: Optional[Any] = None
    changes_type: Optional[str] = None
    version_schema: Optional[int] = None


class HistorySchemaEvolutionResponse(HistorySchemaEvolutionBase):
    """Response schema for HistorySchemaEvolution."""
    id: int
    table_metadata_list_id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TableMetadataBase(BaseModel):
    """Base schema for TableMetadata."""
    table_name: str
    schema_table: Optional[Any] = None
    is_exists_table_landing: bool = False
    is_exists_stream: bool = False
    is_exists_task: bool = False
    is_exists_table_destination: bool = False
    is_changes_schema: bool = False


class TableMetadataCreate(TableMetadataBase):
    """Schema for creating TableMetadata."""
    source_id: int


class TableMetadataResponse(TableMetadataBase):
    """Response schema for TableMetadata."""
    id: int
    source_id: int
    created_at: datetime
    updated_at: datetime
    history_schema: List[HistorySchemaEvolutionResponse] = []

    model_config = ConfigDict(from_attributes=True)
