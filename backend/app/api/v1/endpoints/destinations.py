"""
Destination endpoints.

Provides REST API for managing data destinations.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_destination_service, get_destination_service_readonly
from app.domain.schemas.destination import (
    DestinationCreate,
    DestinationResponse,
    DestinationUpdate,
)
from app.domain.services.destination import DestinationService

router = APIRouter()


@router.post(
    "",
    response_model=DestinationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create destination",
    description="Create a new Snowflake data destination configuration",
)
def create_destination(
    destination_data: DestinationCreate,
    service: DestinationService = Depends(get_destination_service),
) -> DestinationResponse:
    """
    Create a new destination.

    Args:
        destination_data: Destination configuration data
        service: Destination service instance

    Returns:
        Created destination
    """
    destination = service.create_destination(destination_data)
    return DestinationResponse.from_orm(destination)


@router.get(
    "",
    response_model=List[DestinationResponse],
    summary="List destinations",
    description="Get a list of all configured data destinations",
)
def list_destinations(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of items to return"
    ),
    service: DestinationService = Depends(get_destination_service_readonly),
) -> List[DestinationResponse]:
    """
    List all destinations with pagination.

    Args:
        skip: Number of destinations to skip
        limit: Maximum number of destinations to return
        service: Destination service instance

    Returns:
        List of destinations
    """
    destinations = service.list_destinations(skip=skip, limit=limit)
    return [DestinationResponse.from_orm(d) for d in destinations]


@router.get(
    "/{destination_id}",
    response_model=DestinationResponse,
    summary="Get destination",
    description="Get a specific destination by ID",
)
def get_destination(
    destination_id: int, service: DestinationService = Depends(get_destination_service_readonly)
) -> DestinationResponse:
    """
    Get destination by ID.

    Args:
        destination_id: Destination identifier
        service: Destination service instance

    Returns:
        Destination details
    """
    destination = service.get_destination(destination_id)
    return DestinationResponse.from_orm(destination)


@router.put(
    "/{destination_id}",
    response_model=DestinationResponse,
    summary="Update destination",
    description="Update an existing destination configuration",
)
def update_destination(
    destination_id: int,
    destination_data: DestinationUpdate,
    service: DestinationService = Depends(get_destination_service),
) -> DestinationResponse:
    """
    Update destination.

    Args:
        destination_id: Destination identifier
        destination_data: Destination update data
        service: Destination service instance

    Returns:
        Updated destination
    """
    destination = service.update_destination(destination_id, destination_data)
    return DestinationResponse.from_orm(destination)


@router.delete(
    "/{destination_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete destination",
    description="Delete a destination configuration",
)
def delete_destination(
    destination_id: int, service: DestinationService = Depends(get_destination_service)
) -> None:
    """
    Delete destination.

    Args:
        destination_id: Destination identifier
        service: Destination service instance
    """
    service.delete_destination(destination_id)


@router.post(
    "/test-connection",
    status_code=status.HTTP_200_OK,
    summary="Test destination connection",
    description="Test connection using provided configuration",
)
def test_connection(
    destination_data: DestinationCreate,
    service: DestinationService = Depends(get_destination_service),
) -> dict:
    """
    Test Snowflake connection.

    Args:
        destination_data: Destination configuration to test
        service: Destination service instance

    Returns:
        Connection status message
    """
    try:
        service.test_connection(destination_data)
        return {"message": "Connection successful"}
    except Exception as e:
        # Return error message to client
        return {"message": f"Connection failed: {str(e)}", "error": True}


@router.post(
    "/{destination_id}/duplicate",
    response_model=DestinationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Duplicate destination",
    description="Duplicate an existing destination configuration",
)
def duplicate_destination(
    destination_id: int,
    service: DestinationService = Depends(get_destination_service),
) -> DestinationResponse:
    """
    Duplicate destination.

    Args:
        destination_id: Destination identifier
        service: Destination service instance

    Returns:
        New destination
    """
    destination = service.duplicate_destination(destination_id)
    return DestinationResponse.from_orm(destination)


@router.get(
    "/{destination_id}/schema",
    response_model=dict[str, List[str]],
    summary="Get destination schema",
    description="Get tables and columns from the active destination database",
)
def get_destination_schema(
    destination_id: int,
    table: str | None = Query(None, description="Optional table name to filter"),
    scope: str = Query("all", description="Scope of schema fetch (all, tables)"),
    service: DestinationService = Depends(get_destination_service),
) -> dict[str, List[str]]:
    """
    Get destination schema (tables and columns).

    Args:
        destination_id: Destination identifier
        table: Optional table name to filter
        scope: Scope of fetch ('all' = tables+columns, 'tables' = tables only)
        service: Destination service instance

    Returns:
        Dictionary mapping table names to column lists
    """
    only_tables = scope == "tables"
    return service.fetch_schema(destination_id, table_name=table, only_tables=only_tables)


@router.get(
    "/{destination_id}/tables",
    response_model=dict,
    summary="Get cached destination table list",
    description="Return the persisted table list (list_tables) for a destination",
)
def get_destination_table_list(
    destination_id: int,
    service: DestinationService = Depends(get_destination_service),
) -> dict:
    """
    Get the cached table list for a destination.

    Returns the list_tables, total_tables, and last_table_check_at stored in DB.
    To trigger a fresh fetch, call POST /{destination_id}/tables/refresh.
    """
    return service.get_table_list(destination_id)


@router.post(
    "/{destination_id}/tables/refresh",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch destination table list refresh",
    description="Enqueue a worker task to refresh the table list for a destination",
)
def refresh_destination_table_list(
    destination_id: int,
    service: DestinationService = Depends(get_destination_service),
) -> dict:
    """
    Dispatch a Celery task to refresh the table list for a destination.

    The task runs asynchronously in the worker.  Poll GET /{destination_id}/tables
    to check when results have been persisted.

    Returns:
        task_id if worker is available.
    """
    task_id = service.dispatch_table_list_task(destination_id)
    if task_id:
        return {"message": "Table list refresh dispatched", "task_id": task_id}
    return {"message": "Worker is disabled; table list refresh not dispatched", "task_id": None}

