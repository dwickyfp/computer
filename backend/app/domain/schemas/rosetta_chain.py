"""
Rosetta Chain Pydantic schemas for request/response validation.

Defines schemas for chain configuration, client management, and table discovery.
"""

import json
from datetime import datetime
from typing import Any, Optional

from pydantic import Field, validator

from app.domain.schemas.common import BaseSchema


# ─── Chain Key Schemas ──────────────────────────────────────────────────────────


class ChainKeyResponse(BaseSchema):
    """Response schema for chain key display."""

    chain_key_masked: str = Field(
        ..., description="Masked chain key (only last 8 chars visible)"
    )
    is_active: bool = Field(..., description="Whether chain ingestion is active")
    created_at: Optional[datetime] = Field(
        default=None, description="When the key was generated"
    )

    class Config:
        orm_mode = True


class ChainKeyGenerate(BaseSchema):
    """Request schema for generating a new chain key."""

    pass  # No fields needed — key is auto-generated


class ChainToggleActiveRequest(BaseSchema):
    """Request body for toggling chain active state."""

    is_active: bool = Field(..., description="Whether to accept incoming connections")


# ─── Chain Client Schemas ───────────────────────────────────────────────────────


class ChainClientBase(BaseSchema):
    """Base chain client schema."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique name for this remote instance",
        examples=["rosetta-production", "rosetta-staging"],
    )
    url: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="URL/hostname of the remote Rosetta compute service",
        examples=["192.168.1.100", "rosetta-prod.internal"],
    )
    port: int = Field(
        default=8001,
        ge=1,
        le=65535,
        description="Port of the remote Rosetta compute service",
        examples=[8001],
    )
    source_chain_id: Optional[str] = Field(
        default=None,
        max_length=255,
        description=(
            "The X-Chain-ID value the sender stamps on each ingest request "
            "(equals the sender's destination ID, visible in their pipeline destination list). "
            "Leave blank to let the receiver auto-detect it on first ingest (works when "
            "only one client is registered)."
        ),
        examples=["3", "12"],
    )


class ChainClientCreate(ChainClientBase):
    """Schema for registering a new remote Rosetta instance."""

    chain_key: str = Field(
        ...,
        min_length=16,
        description="Chain credential key of the remote instance (from the remote Rosetta's Chain Key page)",
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate client name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Client name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    @validator("chain_key")
    def validate_chain_key(cls, v: str) -> str:
        """Strip whitespace and reject obviously wrong values such as ISO dates or IP addresses."""
        v = v.strip()
        import re

        if re.match(r"^\d{4}-\d{2}-\d{2}", v):
            raise ValueError(
                "chain_key looks like a date — paste the key from the remote "
                "Rosetta's Chain Key page, not a timestamp."
            )
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", v):
            raise ValueError(
                "chain_key looks like an IP address — paste the key from the "
                "remote Rosetta's Chain Key page."
            )
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "rosetta-production",
                "url": "192.168.1.100",
                "port": 8001,
                "chain_key": "your-chain-key-here",
            }
        }


class ChainClientUpdate(BaseSchema):
    """Schema for updating a remote Rosetta instance."""

    name: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Unique name for this remote instance",
    )
    url: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=500,
        description="URL/hostname of the remote Rosetta compute service",
    )
    port: Optional[int] = Field(
        default=None,
        ge=1,
        le=65535,
        description="Port of the remote Rosetta compute service",
    )
    source_chain_id: Optional[str] = Field(
        default=None,
        max_length=255,
        description="The X-Chain-ID the sender stamps (= sender's destination ID)",
    )
    chain_key: Optional[str] = Field(
        default=None,
        min_length=16,
        description="Chain credential key of the remote instance (from the remote Rosetta's Chain Key page)",
    )

    @validator("chain_key")
    def validate_chain_key(cls, v: Optional[str]) -> Optional[str]:
        """Strip whitespace and reject obviously wrong values such as ISO dates or IP addresses."""
        if v is None:
            return v
        v = v.strip()
        if not v:
            return None
        import re

        if re.match(r"^\d{4}-\d{2}-\d{2}", v):
            raise ValueError(
                "chain_key looks like a date — paste the key from the remote "
                "Rosetta's Chain Key page, not a timestamp."
            )
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", v):
            raise ValueError(
                "chain_key looks like an IP address — paste the key from the "
                "remote Rosetta's Chain Key page."
            )
        return v

    is_active: Optional[bool] = Field(
        default=None,
        description="Whether this client connection is active",
    )

    @validator("name")
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        """Validate client name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Client name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class ChainTableResponse(BaseSchema):
    """Response schema for a chain table."""

    id: int = Field(..., description="Unique table identifier")
    chain_client_id: Optional[int] = Field(
        default=None,
        description="Owning chain client ID (None for cross-instance rows)",
    )
    table_name: str = Field(..., description="Table name")
    table_schema: dict = Field(default_factory=dict, description="Table schema")

    @validator("table_schema", pre=True, always=True)
    def coerce_table_schema(cls, v: Any) -> dict:
        """Parse table_schema when psycopg2 returns it as a JSON string."""
        if v is None:
            return {}
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                return parsed if isinstance(parsed, dict) else {}
            except (json.JSONDecodeError, TypeError):
                return {}
        if isinstance(v, dict):
            return v
        return {}

    source_chain_id: Optional[str] = Field(
        default=None, description="Source chain identifier"
    )
    record_count: int = Field(default=0, description="Approximate record count")
    last_synced_at: Optional[datetime] = Field(
        default=None, description="Last data sync timestamp"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class RosettaChainDatabaseResponse(BaseSchema):
    """Response schema for a chain database."""

    id: int = Field(..., description="Unique database identifier")
    chain_client_id: int = Field(..., description="Owning chain client ID")
    name: str = Field(..., description="Database name")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class ChainClientResponse(ChainClientBase):
    """Response schema for a chain client with tables."""

    id: int = Field(..., description="Unique client identifier")
    is_active: bool = Field(..., description="Whether connection is active")
    source_chain_id: Optional[str] = Field(
        default=None,
        description="The X-Chain-ID the sender stamps (= sender's destination ID)",
    )
    last_connected_at: Optional[datetime] = Field(
        default=None, description="Last successful connection timestamp"
    )
    tables: list[ChainTableResponse] = Field(
        default_factory=list, description="Tables available on this client"
    )
    databases: list[RosettaChainDatabaseResponse] = Field(
        default_factory=list, description="Databases available on this client"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class ChainClientTestResponse(BaseSchema):
    """Response schema for connection test."""

    success: bool = Field(..., description="Whether connection test succeeded")
    message: str = Field(..., description="Test result message")
    latency_ms: Optional[float] = Field(
        default=None, description="Connection latency in milliseconds"
    )
