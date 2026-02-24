"""
Preset Pydantic schemas.
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from app.domain.schemas.common import BaseSchema, TimestampSchema

class PresetBase(BaseSchema):
    """Base Preset schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Preset name")
    table_names: List[str] = Field(..., description="List of table names")

class PresetCreate(PresetBase):
    """Schema for creating a preset."""
    pass

class PresetUpdate(BaseSchema):
    """Schema for updating a preset."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    table_names: Optional[List[str]] = None

class PresetResponse(PresetBase, TimestampSchema):
    """Schema for preset response."""
    id: int = Field(..., description="Preset ID")
    source_id: int = Field(..., description="Source ID")

    class Config:
        orm_mode = True
