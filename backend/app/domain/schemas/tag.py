"""
Tag Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving tags.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import Field, validator

from app.domain.schemas.common import BaseSchema, TimestampSchema


class TagBase(BaseSchema):
    """Base tag schema with common fields."""

    tag: str = Field(
        ...,
        min_length=1,
        max_length=150,
        description="Tag name",
        examples=["high-priority", "customer-data", "analytics"],
    )


class TagCreate(TagBase):
    """
    Schema for creating a new tag.
    """

    @validator("tag")
    def validate_tag(cls, v: str) -> str:
        """Validate and normalize tag name."""
        # Strip whitespace but preserve case
        v = v.strip()
        
        # Validate format (letters, numbers, hyphens, underscores, spaces)
        if not all(c.isalnum() or c in ["-", "_", " "] for c in v):
            raise ValueError(
                "Tag must contain only alphanumeric characters, "
                "hyphens, underscores, and spaces"
            )
        
        return v

    class Config:
        schema_extra = {
            "example": {
                "tag": "high-priority",
            }
        }


class TagResponse(TimestampSchema):
    """
    Schema for tag responses.
    """

    id: int = Field(..., description="Tag unique identifier")
    tag: str = Field(
        ...,
        min_length=1,
        max_length=150,
        description="Tag name",
        examples=["high-priority", "customer-data", "analytics"],
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "tag": "high-priority",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }


class TagListResponse(BaseSchema):
    """
    Schema for list of tags.
    """

    tags: List[TagResponse] = Field(
        default_factory=list, description="List of tags"
    )
    total: int = Field(..., description="Total number of tags")

    class Config:
        schema_extra = {
            "example": {
                "tags": [
                    {
                        "id": 1,
                        "tag": "high-priority",
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
                "total": 1,
            }
        }


class TableSyncTagAssociationCreate(BaseSchema):
    """
    Schema for creating tag association with table sync.
    """

    tag: str = Field(
        ...,
        min_length=1,
        max_length=150,
        description="Tag name (will be created if doesn't exist)",
        examples=["high-priority"],
    )

    @validator("tag")
    def validate_tag(cls, v: str) -> str:
        """Validate and normalize tag name."""
        v = v.strip()
        
        if not all(c.isalnum() or c in ["-", "_", " "] for c in v):
            raise ValueError(
                "Tag must contain only alphanumeric characters, "
                "hyphens, underscores, and spaces"
            )
        
        return v

    class Config:
        schema_extra = {
            "example": {
                "tag": "high-priority",
            }
        }


class TableSyncTagAssociationResponse(TimestampSchema):
    """
    Schema for tag association response.
    """

    id: int = Field(..., description="Association unique identifier")
    pipelines_destination_table_sync_id: int = Field(
        ..., description="Pipeline destination table sync ID"
    )
    tag_id: int = Field(..., description="Tag ID")
    tag_item: TagResponse = Field(..., description="Tag details")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "pipelines_destination_table_sync_id": 1,
                "tag_id": 1,
                "tag_item": {
                    "id": 1,
                    "tag": "high-priority",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }


class TableSyncTagsResponse(BaseSchema):
    """
    Schema for list of tags associated with a table sync.
    """

    table_sync_id: int = Field(..., description="Pipeline destination table sync ID")
    tags: List[TagResponse] = Field(
        default_factory=list, description="List of associated tags"
    )
    total: int = Field(..., description="Total number of tags")

    class Config:
        schema_extra = {
            "example": {
                "table_sync_id": 1,
                "tags": [
                    {
                        "id": 1,
                        "tag": "high-priority",
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
                "total": 1,
            }
        }


class TagSuggestionResponse(BaseSchema):
    """
    Schema for tag suggestions (autocomplete).
    """

    suggestions: List[TagResponse] = Field(
        default_factory=list, description="List of suggested tags"
    )

    class Config:
        schema_extra = {
            "example": {
                "suggestions": [
                    {
                        "id": 1,
                        "tag": "high-priority",
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
            }
        }


class TagWithUsageCount(TagResponse):
    """
    Schema for tag with usage count.
    """

    usage_count: int = Field(
        ..., description="Number of times this tag is used", ge=0
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "tag": "high-priority",
                "usage_count": 5,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }


class AlphabetGroupedTags(BaseSchema):
    """
    Schema for tags grouped by alphabet letter.
    """

    letter: str = Field(..., description="Alphabet letter", max_length=1)
    tags: List[TagWithUsageCount] = Field(
        default_factory=list, description="Tags starting with this letter"
    )
    count: int = Field(..., description="Number of tags in this group", ge=0)

    class Config:
        schema_extra = {
            "example": {
                "letter": "A",
                "count": 3,
                "tags": [
                    {
                        "id": 1,
                        "tag": "analytics",
                        "usage_count": 5,
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z",
                    }
                ],
            }
        }


class SmartTagsResponse(BaseSchema):
    """
    Schema for smart tags page response.
    """

    groups: List[AlphabetGroupedTags] = Field(
        default_factory=list, description="Tags grouped by alphabet"
    )
    total_tags: int = Field(..., description="Total number of unique tags", ge=0)

    class Config:
        schema_extra = {
            "example": {
                "groups": [
                    {
                        "letter": "A",
                        "count": 3,
                        "tags": [
                            {
                                "id": 1,
                                "tag": "analytics",
                                "usage_count": 5,
                                "created_at": "2024-01-01T00:00:00Z",
                                "updated_at": "2024-01-01T00:00:00Z",
                            }
                        ],
                    }
                ],
                "total_tags": 10,
            }
        }


class DestinationUsage(BaseSchema):
    """Schema for destination usage details."""
    
    destination_id: int
    destination_name: str
    tables: List[str]


class PipelineUsage(BaseSchema):
    """Schema for pipeline usage details."""
    
    pipeline_id: int
    pipeline_name: str
    destinations: List[DestinationUsage]


class TagUsageResponse(BaseSchema):
    """Schema for detailed tag usage response."""
    
    tag: str
    usage: List[PipelineUsage]


class TagRelationNode(BaseSchema):
    """Schema for a tag node in the relations graph."""
    
    id: int
    tag: str
    usage_count: int


class TagRelationEdge(BaseSchema):
    """Schema for an edge between two tags."""
    
    source: int
    target: int
    shared_tables: int


class TagRelationsResponse(BaseSchema):
    """Schema for tag relations graph response."""
    
    nodes: List[TagRelationNode]
    edges: List[TagRelationEdge]
