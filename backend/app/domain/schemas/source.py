"""
Source Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving source configurations.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, validator

from app.domain.schemas.common import BaseSchema, TimestampSchema






class SourceBase(BaseSchema):
    """Base source schema with common fields."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique source name",
        examples=["production-db", "analytics-db"],
    )
    pg_host: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL host address",
        examples=["localhost", "postgres.example.com", "10.0.1.5"],
    )
    pg_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostgreSQL port number",
        examples=[5432, 5433],
    )
    pg_database: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL database name",
        examples=["myapp", "production"],
    )
    pg_username: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL username",
        examples=["replication_user", "postgres"],
    )
    publication_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL publication name for CDC",
        examples=["dbz_publication", "cdc_pub"],
    )
    replication_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Replication slot name",
        examples=["dbz_replication_slot"],
    )


class SourceCreate(SourceBase):
    """
    Schema for creating a new source.

    Requires all connection details and credentials.
    """

    pg_password: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="PostgreSQL password (will be encrypted)",
        examples=["SecurePassword123!"],
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate source name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Source name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    @validator("publication_name")
    def validate_publication_name(cls, v: str) -> str:
        """Validate publication name format."""
        if not v.replace("_", "").isalnum():
            raise ValueError(
                "Publication name must contain only alphanumeric characters "
                "and underscores"
            )
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "production-postgres",
                "pg_host": "postgres.example.com",
                "pg_port": 5432,
                "pg_database": "myapp_production",
                "pg_username": "replication_user",
                "pg_password": "SecurePassword123!",
                "publication_name": "dbz_publication",
                "replication_name": "dbz_replication_slot",
            }
        }


class SourceConnectionTest(BaseSchema):
    """
    Schema for testing database connection.
    """
    pg_host: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL host address",
    )
    pg_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostgreSQL port number",
    )
    pg_database: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL database name",
    )
    pg_username: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL username",
    )
    pg_password: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="PostgreSQL password",
    )


class SourceUpdate(BaseSchema):
    """
    Schema for updating an existing source.

    All fields are optional to support partial updates.
    """

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="Unique source name"
    )
    pg_host: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="PostgreSQL host address",
    )
    pg_port: int | None = Field(
        default=None, ge=1, le=65535, description="PostgreSQL port number"
    )
    pg_database: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="PostgreSQL database name",
    )
    pg_username: str | None = Field(
        default=None, min_length=1, max_length=255, description="PostgreSQL username"
    )
    pg_password: str | None = Field(
        default=None,
        description="PostgreSQL password (will be encrypted)",
    )
    publication_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="PostgreSQL publication name for CDC",
    )
    replication_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Replication slot name",
    )

    @validator("name")
    def validate_name(cls, v: str | None) -> str | None:
        """Validate source name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Source name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class SourceResponse(SourceBase, TimestampSchema):
    """
    Schema for source API responses.

    Includes all source details (except sensitive password).
    """

    id: int = Field(..., description="Unique source identifier", examples=[1, 42])
    is_publication_enabled: bool = Field(default=False, description="Whether publication is enabled")
    is_replication_enabled: bool = Field(default=False, description="Whether replication is enabled")
    last_check_replication_publication: Optional[datetime] = Field(default=None, description="Last timestamp of replication/publication check")
    total_tables: int = Field(default=0, description="Total tables in publication")
    
    # Explicitly exclude password to ensure it's never returned
    pg_password: Optional[str] = Field(default=None, exclude=True)


    class Config:
        orm_mode = True
        fields = {
            'pg_password': {'exclude': True}
        }
        schema_extra = {
            "example": {
                "id": 1,
                "name": "production-postgres",
                "pg_host": "postgres.example.com",
                "pg_port": 5432,
                "pg_database": "myapp_production",
                "pg_username": "replication_user",
                "publication_name": "dbz_publication",
                "replication_name": "dbz_replication_slot",
                "is_publication_enabled": True,
                "is_replication_enabled": True,
                "total_tables": 10,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }

class PublicationCreateRequest(BaseModel):
    tables: List[str] = Field(..., min_items=1, description="List of tables to include in publication")
