"""
Pydantic schemas for Rosetta Catalog architecture.
"""

from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, ConfigDict, Field


class CatalogDatabaseBase(BaseModel):
    name: str = Field(..., description="Name of the database")
    description: Optional[str] = Field(None, description="Optional description")


class CatalogDatabaseCreate(CatalogDatabaseBase):
    pass


class CatalogDatabaseUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, description="New database name")
    description: Optional[str] = Field(None, description="Updated description")


class CatalogDatabaseResponse(CatalogDatabaseBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CatalogTableBase(BaseModel):
    table_name: str = Field(..., description="Name of the table")
    schema_definition: Dict[str, Any] = Field(
        ..., alias="schema_json", description="Table schema definition"
    )
    source_chain_id: Optional[str] = Field(
        None, description="Source chain ID if remote"
    )

    class Config:
        populate_by_name = True


class CatalogTableCreate(CatalogTableBase):
    pass


class CatalogTableResponse(CatalogTableBase):
    id: int
    database_id: int
    stream_name: str
    status: str
    last_health_check_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class SchemaRegistrationRequest(BaseModel):
    """
    Payload sent by Rosetta A to register a table in Rosetta B.
    """

    table_name: str = Field(..., description="Name of the table to register")
    database_name: str = Field(..., description="Target database name in Catalog")
    schema_definition: Dict[str, Any] = Field(
        ..., alias="schema_json", description="Full schema definition"
    )
    source_chain_id: Optional[str] = Field(None, description="Identifier of Rosetta A")

    class Config:
        populate_by_name = True
