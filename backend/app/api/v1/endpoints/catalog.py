"""
API Endpoints for Rosetta Catalog architecture.
"""

from typing import List
from fastapi import APIRouter, Depends

from sqlalchemy.orm import Session
from app.api.deps import get_db, get_db_readonly
from app.domain.services.catalog import CatalogService
from app.domain.schemas.catalog import (
    CatalogDatabaseCreate,
    CatalogDatabaseResponse,
    CatalogDatabaseUpdate,
    CatalogTableResponse,
    SchemaRegistrationRequest,
)

router = APIRouter()


def get_catalog_service(db: Session = Depends(get_db)) -> CatalogService:
    return CatalogService(db)


def get_catalog_service_readonly(db: Session = Depends(get_db_readonly)) -> CatalogService:
    return CatalogService(db)

# ─── Data Explorer ────────────────────────────────────────────────────────────

@router.get("/databases", response_model=List[CatalogDatabaseResponse])
def get_databases(service: CatalogService = Depends(get_catalog_service_readonly)):
    """List all logical databases in the catalog."""
    return service.list_databases()


@router.get("/databases/{db_id}", response_model=CatalogDatabaseResponse)
def get_database(db_id: int, service: CatalogService = Depends(get_catalog_service_readonly)):
    """Get a specific database by ID."""
    return service.get_database(db_id)


@router.post("/databases", response_model=CatalogDatabaseResponse, status_code=201)
def create_database(
    data: CatalogDatabaseCreate, service: CatalogService = Depends(get_catalog_service)
):
    """Create a new logical database container."""
    return service.create_database(data)


@router.delete("/databases/{db_id}", status_code=204)
def delete_database(db_id: int, service: CatalogService = Depends(get_catalog_service)):
    """Delete a logical database container and all its tables."""
    service.delete_database(db_id)


@router.put("/databases/{db_id}", response_model=CatalogDatabaseResponse)
def update_database(
    db_id: int,
    data: CatalogDatabaseUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    """Rename or update description of a logical database container."""
    return service.update_database(db_id, name=data.name, description=data.description)
    service.delete_database(db_id)


@router.get("/databases/{db_id}/tables", response_model=List[CatalogTableResponse])
def get_database_tables(
    db_id: int, service: CatalogService = Depends(get_catalog_service_readonly)
):
    """List all tables within a specific database."""
    return service.list_tables(db_id)


@router.get("/tables/{table_id}", response_model=CatalogTableResponse)
def get_table(table_id: int, service: CatalogService = Depends(get_catalog_service_readonly)):
    """Get a specific table's definition and metadata by ID."""
    return service.get_table(table_id)


# ─── Schema Registration (Handshake) ──────────────────────────────────────────

@router.post("/register", response_model=CatalogTableResponse, status_code=200)
def register_schema(
    req: SchemaRegistrationRequest, service: CatalogService = Depends(get_catalog_service)
):
    """
    Schema Registration Endpoint (Rosetta A -> B Handshake).
    Validates and registers the table scheme into the local catalog.
    """
    return service.register_schema(req)
