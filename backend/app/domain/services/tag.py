"""
Tag service containing business logic.

Implements business rules and orchestrates repository operations for tags.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList
from app.domain.repositories.tag import TableSyncTagRepository, TagRepository
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
    TagWithUsageCount,
    TagUsageResponse,
    TagRelationsResponse,
    PipelineUsage,
    DestinationUsage,
)

logger = get_logger(__name__)


class TagService:
    """
    Service layer for Tag entity.

    Implements business logic for managing tags and tag associations.
    """

    def __init__(self, db: Session):
        """
        Initialize tag service.

        Args:
            db: Database session
        """
        self.db = db
        self.tag_repository = TagRepository(db)
        self.table_sync_tag_repository = TableSyncTagRepository(db)

    def create_tag(self, tag_data: TagCreate) -> TagResponse:
        """
        Create a new tag.

        Args:
            tag_data: Tag creation data

        Returns:
            Created tag
        """
        logger.info("Creating new tag", extra={"tag": tag_data.tag})

        tag = self.tag_repository.create(tag=tag_data.tag)
        self.db.commit()
        self.db.refresh(tag)

        logger.info(
            "Tag created successfully", extra={"tag_id": tag.id, "tag": tag.tag}
        )

        return TagResponse.from_orm(tag)

    def get_tag(self, tag_id: int) -> TagResponse:
        """
        Get tag by ID.

        Args:
            tag_id: Tag identifier

        Returns:
            Tag response

        Raises:
            EntityNotFoundError: If tag not found
        """
        tag = self.tag_repository.get_by_id(tag_id)
        return TagResponse.from_orm(tag)

    def get_tag_usage(self, tag_id: int) -> TagUsageResponse:
        """
        Get tag usage details.

        Args:
            tag_id: Tag identifier

        Returns:
            Tag usage details

        Raises:
            EntityNotFoundError: If tag not found
        """
        tag = self.tag_repository.get_by_id(tag_id)
        if not tag:
            raise EntityNotFoundError(f"Tag with id {tag_id} not found")

        rows = self.tag_repository.get_tag_usage_details(tag_id)

        # Group by pipeline -> destination -> tables
        structure = {}
        
        for row in rows:
            p_id = row.pipeline_id
            p_name = row.pipeline_name
            d_id = row.destination_id
            d_name = row.destination_name
            t_name = row.table_name
            
            p_key = (p_id, p_name)
            d_key = (d_id, d_name)
            
            if p_key not in structure:
                structure[p_key] = {}
            
            if d_key not in structure[p_key]:
                structure[p_key][d_key] = []
                
            structure[p_key][d_key].append(t_name)
            
        # Convert to response schema
        usage_list = []
        for (p_id, p_name), dests in structure.items():
            dest_list = []
            for (d_id, d_name), tables in dests.items():
                dest_list.append(
                    DestinationUsage(destination_id=d_id, destination_name=d_name, tables=tables)
                )
            
            usage_list.append(
                PipelineUsage(pipeline_id=p_id, pipeline_name=p_name, destinations=dest_list)
            )
            
        return TagUsageResponse(tag=tag.tag, usage=usage_list)

    def get_all_tags(self, skip: int = 0, limit: int = 100) -> TagListResponse:
        """
        Get all tags with pagination.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records

        Returns:
            List of tags with total count
        """
        tags = self.tag_repository.get_all_ordered(skip=skip, limit=limit)
        total = self.tag_repository.count()

        return TagListResponse(
            tags=[TagResponse.from_orm(tag) for tag in tags], total=total
        )

    def search_tags(self, query: str, limit: int = 10) -> TagSuggestionResponse:
        """
        Search tags for autocomplete.

        Args:
            query: Search query
            limit: Maximum number of results

        Returns:
            List of suggested tags
        """
        if not query or len(query.strip()) == 0:
            # Return all tags if query is empty
            tags = self.tag_repository.get_all_ordered(skip=0, limit=limit)
        else:
            tags = self.tag_repository.search_tags(query=query, limit=limit)

        return TagSuggestionResponse(
            suggestions=[TagResponse.from_orm(tag) for tag in tags]
        )

    def get_smart_tags(
        self,
        pipeline_id: int | None = None,
        destination_id: int | None = None,
        source_id: int | None = None,
    ) -> SmartTagsResponse:
        """
        Get all tags grouped by alphabet with usage counts.

        Returns:
            Tags grouped by first letter with usage statistics
        """
        # Get all tags with usage counts
        tags_with_counts = self.tag_repository.get_all_with_usage_count(
            pipeline_id=pipeline_id,
            destination_id=destination_id,
            source_id=source_id,
        )

        # Group by first letter
        groups_dict = {}
        total_tags = 0

        for tag, usage_count in tags_with_counts:
            first_char = tag.tag[0].upper() if tag.tag else "#"
            
            # Group by number if digit, letter if alpha, else #
            if first_char.isdigit():
                group_key = first_char
            elif first_char.isalpha():
                group_key = first_char
            else:
                group_key = "#"

            if group_key not in groups_dict:
                groups_dict[group_key] = []

            tag_with_usage = TagWithUsageCount(
                id=tag.id,
                tag=tag.tag,
                usage_count=usage_count,
                created_at=tag.created_at,
                updated_at=tag.updated_at,
            )
            groups_dict[group_key].append(tag_with_usage)
            total_tags += 1

        # Convert to list and sort by letter
        # Sort keys: digits first (0-9), then letters (A-Z), then #
        def sort_key(k):
            if k.isdigit():
                return (0, k)
            elif k.isalpha():
                return (1, k)
            else:
                return (2, k)

        groups = []
        for letter in sorted(groups_dict.keys(), key=sort_key):
            groups.append(
                AlphabetGroupedTags(
                    letter=letter,
                    tags=groups_dict[letter],
                    count=len(groups_dict[letter]),
                )
            )

        return SmartTagsResponse(groups=groups, total_tags=total_tags)

    def cleanup_unused_tag(self, tag_id: int) -> bool:
        """
        Delete a tag if it's not associated with any table sync.

        Args:
            tag_id: Tag identifier

        Returns:
            True if tag was deleted, False if it's still in use
        """
        try:
            if self.tag_repository.is_tag_unused(tag_id):
                tag = self.tag_repository.get_by_id(tag_id)
                logger.info(
                    "Auto-deleting unused tag",
                    extra={"tag_id": tag_id, "tag": tag.tag},
                )
                self.tag_repository.delete(tag_id)
                self.db.commit()
                logger.info(
                    "Unused tag deleted successfully",
                    extra={"tag_id": tag_id, "tag": tag.tag},
                )
                return True
            else:
                logger.debug(
                    "Tag still in use, not deleting",
                    extra={"tag_id": tag_id},
                )
                return False
        except EntityNotFoundError:
            # Tag already deleted, ignore
            logger.debug("Tag already deleted", extra={"tag_id": tag_id})
            return False

    def delete_tag(self, tag_id: int) -> None:
        """
        Delete a tag.

        Args:
            tag_id: Tag identifier

        Raises:
            EntityNotFoundError: If tag not found
        """
        tag = self.tag_repository.get_by_id(tag_id)

        logger.info("Deleting tag", extra={"tag_id": tag_id, "tag": tag.tag})

        self.tag_repository.delete(tag_id)
        self.db.commit()

    def get_tag_relations(self) -> TagRelationsResponse:
        """
        Get tag nodes and edges for network visualization.
        
        Returns:
            TagRelationsResponse with nodes and edges
        """
        logger.info("Getting tag relations for visualization")
        data = self.tag_repository.get_tag_relations()
        return TagRelationsResponse(**data)

        logger.info("Tag deleted successfully", extra={"tag_id": tag_id})

    # Table Sync Tag Association Methods

    def get_tags_for_table_sync(self, table_sync_id: int) -> TableSyncTagsResponse:
        """
        Get all tags associated with a table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID

        Returns:
            List of tags with total count
        """
        associations = self.table_sync_tag_repository.get_tags_for_table_sync(
            table_sync_id
        )

        tags = [TagResponse.from_orm(assoc.tag_item) for assoc in associations]

        return TableSyncTagsResponse(
            table_sync_id=table_sync_id, tags=tags, total=len(tags)
        )

    def add_tag_to_table_sync(
        self, table_sync_id: int, tag_data: TableSyncTagAssociationCreate
    ) -> TableSyncTagAssociationResponse:
        """
        Add a tag to a table sync.

        Creates the tag if it doesn't exist.

        Args:
            table_sync_id: Pipeline destination table sync ID
            tag_data: Tag association data

        Returns:
            Created association
        """
        logger.info(
            "Adding tag to table sync",
            extra={"table_sync_id": table_sync_id, "tag": tag_data.tag},
        )

        # Get or create tag
        tag = self.tag_repository.get_or_create(tag_name=tag_data.tag)

        # Create association
        association = self.table_sync_tag_repository.add_tag_to_table_sync(
            table_sync_id=table_sync_id, tag_id=tag.id
        )

        self.db.commit()
        self.db.refresh(association)

        logger.info(
            "Tag added to table sync successfully",
            extra={
                "table_sync_id": table_sync_id,
                "tag_id": tag.id,
                "association_id": association.id,
            },
        )

        return TableSyncTagAssociationResponse.from_orm(association)

    def remove_tag_from_table_sync(self, table_sync_id: int, tag_id: int) -> None:
        """
        Remove a tag from a table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID
            tag_id: Tag ID

        Raises:
            EntityNotFoundError: If association not found
        """
        logger.info(
            "Removing tag from table sync",
            extra={"table_sync_id": table_sync_id, "tag_id": tag_id},
        )

        deleted = self.table_sync_tag_repository.remove_tag_from_table_sync(
            table_sync_id=table_sync_id, tag_id=tag_id
        )

        if not deleted:
            raise EntityNotFoundError(
                entity_type="TableSyncTag",
                entity_id=f"table_sync_id={table_sync_id}, tag_id={tag_id}",
            )

        self.db.commit()

        logger.info(
            "Tag removed from table sync successfully",
            extra={"table_sync_id": table_sync_id, "tag_id": tag_id},
        )

        # Auto-cleanup: Delete tag if it's no longer used anywhere
        self.cleanup_unused_tag(tag_id)

    def get_table_syncs_by_tag(self, tag_id: int) -> List[int]:
        """
        Get all table sync IDs associated with a tag.

        Args:
            tag_id: Tag ID

        Returns:
            List of table sync IDs
        """
        return self.table_sync_tag_repository.get_table_syncs_by_tag(tag_id)

    def bulk_add_tags_to_table_sync(
        self, table_sync_id: int, tags: List[str]
    ) -> List[TableSyncTagAssociationResponse]:
        """
        Add multiple tags to a table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID
            tags: List of tag names

        Returns:
            List of created associations
        """
        logger.info(
            "Bulk adding tags to table sync",
            extra={"table_sync_id": table_sync_id, "tag_count": len(tags)},
        )

        associations = []

        for tag_name in tags:
            try:
                tag_data = TableSyncTagAssociationCreate(tag=tag_name)
                association = self.add_tag_to_table_sync(
                    table_sync_id=table_sync_id, tag_data=tag_data
                )
                associations.append(association)
            except Exception as e:
                logger.error(
                    f"Failed to add tag to table sync",
                    extra={
                        "table_sync_id": table_sync_id,
                        "tag": tag_name,
                        "error": str(e),
                    },
                )
                # Continue with other tags even if one fails

        logger.info(
            "Bulk tag addition completed",
            extra={
                "table_sync_id": table_sync_id,
                "successful": len(associations),
                "total": len(tags),
            },
        )

        return associations
