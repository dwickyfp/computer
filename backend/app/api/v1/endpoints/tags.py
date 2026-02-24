"""
Tag endpoints.

Provides REST API for managing tags and tag associations.
"""

from typing import List

from fastapi import APIRouter, Depends, Path, Query, status

from app.api.deps import get_tag_service
from app.domain.schemas.tag import (
    AlphabetGroupedTags,
    SmartTagsResponse,
    TableSyncTagAssociationCreate,
    TableSyncTagAssociationResponse,
    TableSyncTagsResponse,
    TagCreate,
    TagListResponse,
    TagResponse,
    TagSuggestionResponse,
    TagUsageResponse,
    TagRelationsResponse,
    TagWithUsageCount,
)
from app.domain.services.tag import TagService

router = APIRouter()


@router.post(
    "",
    response_model=TagResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create tag",
    description="Create a new tag for smart grouping",
)
def create_tag(
    tag_data: TagCreate, service: TagService = Depends(get_tag_service)
) -> TagResponse:
    """
    Create a new tag.

    Args:
        tag_data: Tag creation data
        service: Tag service instance

    Returns:
        Created tag
    """
    return service.create_tag(tag_data)


@router.get(
    "",
    response_model=TagListResponse,
    summary="List tags",
    description="Get a list of all tags with pagination",
)
def list_tags(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    service: TagService = Depends(get_tag_service),
) -> TagListResponse:
    """
    List all tags with pagination.

    Args:
        skip: Number of tags to skip
        limit: Maximum number of tags to return
        service: Tag service instance

    Returns:
        List of tags with total count
    """
    return service.get_all_tags(skip=skip, limit=limit)


@router.get(
    "/search",
    response_model=TagSuggestionResponse,
    summary="Search tags",
    description="Search tags for autocomplete (partial match)",
)
def search_tags(
    q: str = Query("", description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of suggestions"),
    service: TagService = Depends(get_tag_service),
) -> TagSuggestionResponse:
    """
    Search tags for autocomplete.

    Args:
        q: Search query
        limit: Maximum number of suggestions
        service: Tag service instance

    Returns:
        List of suggested tags
    """
    return service.search_tags(query=q, limit=limit)


@router.get(
    "/smart-tags",
    response_model=SmartTagsResponse,
    summary="Get smart tags",
    description="Get all tags grouped by alphabet with usage counts",
)
def get_smart_tags(
    pipeline_id: int | None = Query(None, ge=1, description="Filter by pipeline ID"),
    destination_id: int | None = Query(None, ge=1, description="Filter by destination ID"),
    source_id: int | None = Query(None, ge=1, description="Filter by source ID"),
    service: TagService = Depends(get_tag_service),
) -> SmartTagsResponse:
    """
    Get all tags grouped by alphabet with usage counts.

    Returns:
        Tags grouped by first letter with usage statistics
    """
    return service.get_smart_tags(
        pipeline_id=pipeline_id,
        destination_id=destination_id,
        source_id=source_id,
    )


@router.get(
    "/relations",
    response_model=TagRelationsResponse,
    summary="Get tag relations",
    description="Get tag nodes and edges for network graph visualization",
)
def get_tag_relations(
    service: TagService = Depends(get_tag_service),
) -> TagRelationsResponse:
    """
    Get tag relations for network visualization.

    Returns nodes (tags) and edges (shared table_sync connections).
    """
    return service.get_tag_relations()


@router.get(
    "/{tag_id}",
    response_model=TagResponse,
    summary="Get tag",
    description="Get a specific tag by ID",
)
def get_tag(
    tag_id: int = Path(..., ge=1, description="Tag ID"),
    service: TagService = Depends(get_tag_service),
) -> TagResponse:
    """
    Get tag by ID.

    Args:
        tag_id: Tag identifier
        service: Tag service instance

    Returns:
        Tag details
    """
    return service.get_tag(tag_id)


@router.get(
    "/{tag_id}/usage",
    response_model=TagUsageResponse,
    summary="Get tag usage",
    description="Get detailed usage information for a tag",
)
def get_tag_usage(
    tag_id: int = Path(..., ge=1, description="Tag ID"),
    service: TagService = Depends(get_tag_service),
) -> TagUsageResponse:
    """
    Get detailed usage information for a tag.

    Args:
        tag_id: Tag identifier
        service: Tag service instance

    Returns:
        Structured usage details
    """
    return service.get_tag_usage(tag_id)


@router.delete(
    "/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete tag",
    description="Delete a tag by ID",
)
def delete_tag(
    tag_id: int = Path(..., ge=1, description="Tag ID"),
    service: TagService = Depends(get_tag_service),
) -> None:
    """
    Delete a tag.

    Args:
        tag_id: Tag identifier
        service: Tag service instance
    """
    service.delete_tag(tag_id)


# Table Sync Tag Association Endpoints


@router.get(
    "/table-sync/{table_sync_id}",
    response_model=TableSyncTagsResponse,
    summary="Get tags for table sync",
    description="Get all tags associated with a pipeline destination table sync",
)
def get_table_sync_tags(
    table_sync_id: int = Path(..., ge=1, description="Pipeline destination table sync ID"),
    service: TagService = Depends(get_tag_service),
) -> TableSyncTagsResponse:
    """
    Get all tags for a table sync.

    Args:
        table_sync_id: Pipeline destination table sync ID
        service: Tag service instance

    Returns:
        List of tags associated with the table sync
    """
    return service.get_tags_for_table_sync(table_sync_id)


@router.post(
    "/table-sync/{table_sync_id}",
    response_model=TableSyncTagAssociationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add tag to table sync",
    description="Add a tag to a pipeline destination table sync (creates tag if doesn't exist)",
)
def add_tag_to_table_sync(
    table_sync_id: int = Path(..., ge=1, description="Pipeline destination table sync ID"),
    tag_data: TableSyncTagAssociationCreate = ...,
    service: TagService = Depends(get_tag_service),
) -> TableSyncTagAssociationResponse:
    """
    Add a tag to a table sync.

    If the tag doesn't exist, it will be created automatically.

    Args:
        table_sync_id: Pipeline destination table sync ID
        tag_data: Tag association data
        service: Tag service instance

    Returns:
        Created association
    """
    return service.add_tag_to_table_sync(table_sync_id, tag_data)


@router.delete(
    "/table-sync/{table_sync_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove tag from table sync",
    description="Remove a tag association from a pipeline destination table sync",
)
def remove_tag_from_table_sync(
    table_sync_id: int = Path(..., ge=1, description="Pipeline destination table sync ID"),
    tag_id: int = Path(..., ge=1, description="Tag ID"),
    service: TagService = Depends(get_tag_service),
) -> None:
    """
    Remove a tag from a table sync.

    Args:
        table_sync_id: Pipeline destination table sync ID
        tag_id: Tag ID
        service: Tag service instance
    """
    service.remove_tag_from_table_sync(table_sync_id, tag_id)


@router.get(
    "/tag/{tag_id}/table-syncs",
    response_model=List[int],
    summary="Get table syncs by tag",
    description="Get all pipeline destination table sync IDs associated with a tag",
)
def get_table_syncs_by_tag(
    tag_id: int = Path(..., ge=1, description="Tag ID"),
    service: TagService = Depends(get_tag_service),
) -> List[int]:
    """
    Get all table sync IDs associated with a tag.

    Args:
        tag_id: Tag ID
        service: Tag service instance

    Returns:
        List of table sync IDs
    """
    return service.get_table_syncs_by_tag(tag_id)
