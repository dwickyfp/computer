"""
Source endpoints.

Provides REST API for managing data sources.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.deps import get_source_service, get_source_service_readonly, get_preset_service
from app.domain.schemas.source import (
    PublicationCreateRequest,
    SourceCreate,
    SourceResponse,
    SourceUpdate,
    SourceConnectionTest,
)
from app.domain.schemas.source_detail import SourceDetailResponse, TableSchemaResponse
from app.domain.services.source import SourceService
from app.domain.services.preset import PresetService
from app.domain.schemas.preset import PresetCreate, PresetResponse

router = APIRouter()


@router.post(
    "",
    response_model=SourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create source",
    description="Create a new PostgreSQL data source configuration",
)
def create_source(
    source_data: SourceCreate, service: SourceService = Depends(get_source_service)
) -> SourceResponse:
    """
    Create a new source.

    Args:
        source_data: Source configuration data
        service: Source service instance

    Returns:
        Created source
    """
    source = service.create_source(source_data)
    return SourceResponse.from_orm(source)


@router.get(
    "",
    response_model=List[SourceResponse],
    summary="List sources",
    description="Get a list of all configured data sources",
)
def list_sources(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of items to return"
    ),
    service: SourceService = Depends(get_source_service_readonly),
) -> List[SourceResponse]:
    """
    List all sources with pagination.

    Args:
        skip: Number of sources to skip
        limit: Maximum number of sources to return
        service: Source service instance

    Returns:
        List of sources
    """
    sources = service.list_sources(skip=skip, limit=limit)
    return [SourceResponse.from_orm(s) for s in sources]


@router.get(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Get source",
    description="Get a specific source by ID",
)
def get_source(
    source_id: int, service: SourceService = Depends(get_source_service_readonly)
) -> SourceResponse:
    """
    Get source by ID.

    Args:
        source_id: Source identifier
        service: Source service instance

    Returns:
        Source details
    """
    source = service.get_source(source_id)
    return SourceResponse.from_orm(source)


@router.get(
    "/{source_id}/details",
    response_model=SourceDetailResponse,
    summary="Get source details",
    description="Get detailed source information including WAL monitor metrics and table metadata",
)
def get_source_details(
    source_id: int,
    force_refresh: bool = Query(
        False, description="Force refresh from source database (bypasses cache, slower)"
    ),
    service: SourceService = Depends(get_source_service),
) -> SourceDetailResponse:
    """
    Get source details.

    Args:
        source_id: Source identifier
        force_refresh: If True, bypass cache and refresh from source database
        service: Source service instance

    Returns:
        Source details
    """
    return service.get_source_details(source_id, force_refresh=force_refresh)


@router.put(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Update source",
    description="Update an existing source configuration",
)
def update_source(
    source_id: int,
    source_data: SourceUpdate,
    service: SourceService = Depends(get_source_service),
) -> SourceResponse:
    """
    Update source.

    Args:
        source_id: Source identifier
        source_data: Source update data
        service: Source service instance

    Returns:
        Updated source
    """
    source = service.update_source(source_id, source_data)
    return SourceResponse.from_orm(source)


@router.delete(
    "/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete source",
    description="Delete a source configuration",
)
def delete_source(
    source_id: int, service: SourceService = Depends(get_source_service)
) -> None:
    """
    Delete source.

    Args:
        source_id: Source identifier
        service: Source service instance
    """
    service.delete_source(source_id)


@router.post(
    "/test_connection",
    response_model=bool,
    summary="Test connection",
    description="Test connection with provided configuration",
)
def test_connection(
    config: SourceConnectionTest,
    service: SourceService = Depends(get_source_service),
) -> bool:
    """
    Test connection with provided configuration.

    Args:
        config: Connection configuration
        service: Source service instance

    Returns:
        True if connection is successful, False otherwise
    """
    return service.test_connection_config(config)


@router.get(
    "/tables/{table_id}/schema",
    response_model=TableSchemaResponse,
    summary="Get table schema by version",
    description="Get schema columns for a specific table version with evolution info",
)
def get_table_schema(
    table_id: int,
    version: int = Query(..., ge=1, description="Schema version"),
    service: SourceService = Depends(get_source_service),
) -> TableSchemaResponse:
    """
    Get table schema for a specific version.

    Args:
        table_id: Table identifier
        version: Schema version
        service: Source service instance

    Returns:
        TableSchemaResponse
    """
    return service.get_table_schema_by_version(table_id, version)


class TableRegisterRequest(BaseModel):
    table_name: str


@router.post(
    "/{source_id}/tables/register",
    status_code=status.HTTP_200_OK,
    summary="Register table to publication",
    description="Add a table to the source's publication",
)
def register_table(
    source_id: int,
    request: TableRegisterRequest,
    service: SourceService = Depends(get_source_service),
) -> None:
    """
    Register table to publication.

    Args:
        source_id: Source identifier
        request: Registration request
        service: Source service instance
    """
    service.register_table_to_publication(source_id, request.table_name)


@router.delete(
    "/{source_id}/tables/{table_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Unregister/Drop table from publication",
    description="Remove a table from the source's publication",
)
def unregister_table(
    source_id: int,
    table_name: str,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.unregister_table_from_publication(source_id, table_name)


@router.post(
    "/{source_id}/refresh",
    status_code=status.HTTP_200_OK,
    summary="Refresh source metadata",
    description="Manually checks and updates table list and status",
)
def refresh_source(
    source_id: int,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.refresh_source_metadata(source_id)


@router.post(
    "/{source_id}/publication",
    status_code=status.HTTP_201_CREATED,
    summary="Create Publication",
)
def create_publication(
    source_id: int,
    request: PublicationCreateRequest,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.create_publication(source_id, request.tables)


@router.delete(
    "/{source_id}/publication",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Drop Publication",
)
def drop_publication(
    source_id: int,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.drop_publication(source_id)


@router.post(
    "/{source_id}/replication",
    status_code=status.HTTP_201_CREATED,
    summary="Create Replication Slot",
)
def create_replication_slot(
    source_id: int,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.create_replication_slot(source_id)


@router.delete(
    "/{source_id}/replication",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Drop Replication Slot",
)
def drop_replication_slot(
    source_id: int,
    service: SourceService = Depends(get_source_service),
) -> None:
    service.drop_replication_slot(source_id)


@router.get(
    "/{source_id}/available_tables",
    response_model=List[str],
    summary="Fetch all available tables from source",
)
def fetch_available_tables(
    source_id: int,
    refresh: bool = Query(False, description="Force refresh from source database"),
    service: SourceService = Depends(get_source_service),
) -> List[str]:
    """
    Fetch all available tables from source database.

    If refresh is True, ignores cache and fetches directly from DB, updating cache.
    Otherwise, returns cached result or fetches if cache miss.
    """
    if refresh:
        return service.refresh_available_tables(source_id)
    return service.fetch_available_tables(source_id)


# --- Presets ---


@router.post(
    "/{source_id}/presets",
    response_model=PresetResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new preset",
)
def create_preset(
    source_id: int,
    preset_data: PresetCreate,
    service: PresetService = Depends(get_preset_service),
) -> PresetResponse:
    """Create a new preset."""
    preset = service.create_preset(source_id, preset_data)
    return PresetResponse.from_orm(preset)


@router.get(
    "/{source_id}/presets",
    response_model=List[PresetResponse],
    summary="Get all presets for a source",
)
def get_presets(
    source_id: int,
    service: PresetService = Depends(get_preset_service),
) -> List[PresetResponse]:
    """Get all presets for a source."""
    presets = service.get_presets(source_id)
    return [PresetResponse.from_orm(p) for p in presets]


@router.delete(
    "/presets/{preset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a preset",
)
def delete_preset(
    preset_id: int,
    service: PresetService = Depends(get_preset_service),
) -> None:
    """Delete a preset."""
    service.delete_preset(preset_id)


@router.put(
    "/presets/{preset_id}",
    response_model=PresetResponse,
    summary="Update a preset",
)
def update_preset(
    preset_id: int,
    preset_data: PresetCreate,
    service: PresetService = Depends(get_preset_service),
) -> PresetResponse:
    """Update a preset."""
    preset = service.update_preset(preset_id, preset_data)
    return PresetResponse.from_orm(preset)


@router.post(
    "/{source_id}/duplicate",
    response_model=SourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Duplicate source",
    description="Duplicate an existing source configuration",
)
def duplicate_source(
    source_id: int,
    service: SourceService = Depends(get_source_service),
) -> SourceResponse:
    """
    Duplicate source.

    Args:
        source_id: Source identifier to duplicate
        service: Source service instance

    Returns:
        New source
    """
    source = service.duplicate_source(source_id)
    return SourceResponse.from_orm(source)


@router.get(
    "/{source_id}/schema",
    response_model=dict[str, List[str]],
    summary="Get source schema",
    description="Get tables and columns from the source database",
)
def get_source_schema(
    source_id: int,
    table: str | None = Query(None, description="Optional table name to filter"),
    scope: str = Query("all", description="Scope of schema fetch (all, tables)"),
    service: SourceService = Depends(get_source_service),
) -> dict[str, List[str]]:
    """
    Get source schema (tables and columns).

    Args:
        source_id: Source identifier
        table: Optional table name to filter
        scope: Scope of fetch ('all' = tables+columns, 'tables' = tables only)
        service: Source service instance

    Returns:
        Dictionary mapping table names to column lists
    """
    only_tables = scope == "tables"
    return service.fetch_schema(source_id, table_name=table, only_tables=only_tables)
