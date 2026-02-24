"""
Tag repository for data access operations.

Handles database operations for tags and tag associations.
"""

from typing import List, Optional

from sqlalchemy import delete, func, or_, select, and_
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, joinedload, aliased

from app.core.exceptions import DatabaseError, DuplicateEntityError, EntityNotFoundError
from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.models.pipeline import Pipeline, PipelineDestination, PipelineDestinationTableSync
from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList
from app.domain.repositories.base import BaseRepository

logger = get_logger(__name__)


class TagRepository(BaseRepository[TagList]):
    """
    Repository for tag operations.
    """

    def __init__(self, db: Session):
        """
        Initialize tag repository.

        Args:
            db: Database session
        """
        super().__init__(TagList, db)

    def get_by_tag_name(self, tag_name: str) -> Optional[TagList]:
        """
        Get tag by exact name.

        Args:
            tag_name: Tag name (case-insensitive)

        Returns:
            Tag instance or None if not found

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                select(TagList).where(func.lower(TagList.tag) == tag_name.lower())
            )
            return result.scalar_one_or_none()

        except SQLAlchemyError as e:
            logger.error(
                "Failed to get tag by name",
                extra={"tag_name": tag_name, "error": str(e)},
            )
            raise DatabaseError("Failed to get tag") from e

    def search_tags(self, query: str, limit: int = 10) -> List[TagList]:
        """
        Search tags by partial name match (for autocomplete).

        Args:
            query: Search query
            limit: Maximum number of results

        Returns:
            List of matching tags

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            search_pattern = f"%{query.lower()}%"
            result = self.db.execute(
                select(TagList)
                .where(func.lower(TagList.tag).like(search_pattern))
                .order_by(TagList.tag)
                .limit(limit)
            )
            return list(result.scalars().all())

        except SQLAlchemyError as e:
            logger.error(
                "Failed to search tags", extra={"query": query, "error": str(e)}
            )
            raise DatabaseError("Failed to search tags") from e

    def get_or_create(self, tag_name: str) -> TagList:
        """
        Get existing tag or create new one.

        Args:
            tag_name: Tag name

        Returns:
            Tag instance

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            # Try to get existing tag
            tag = self.get_by_tag_name(tag_name)
            
            if tag:
                logger.debug(f"Found existing tag: {tag_name}")
                return tag

            # Create new tag
            tag = TagList(tag=tag_name.strip())
            self.db.add(tag)
            self.db.flush()
            self.db.refresh(tag)

            logger.info(f"Created new tag: {tag_name}", extra={"tag_id": tag.id})
            return tag

        except IntegrityError as e:
            # Handle race condition - tag was created by another request
            self.db.rollback()
            tag = self.get_by_tag_name(tag_name)
            if tag:
                return tag
            raise DatabaseError(f"Failed to create tag: {tag_name}") from e
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                "Failed to get or create tag",
                extra={"tag_name": tag_name, "error": str(e)},
            )
            raise DatabaseError("Failed to get or create tag") from e

    def get_tag_usage_details(self, tag_id: int):
        """
        Get detailed usage of a tag.
        
        Args:
            tag_id: Tag identifier
            
        Returns:
            List of rows with pipeline_name, destination_name, table_name
        """
        try:
            return self.db.execute(
                select(
                    Pipeline.id.label("pipeline_id"),
                    Pipeline.name.label("pipeline_name"),
                    Destination.id.label("destination_id"),
                    Destination.name.label("destination_name"),
                    PipelineDestinationTableSync.table_name.label("table_name")
                )
                .join(PipelineDestination, PipelineDestination.pipeline_id == Pipeline.id)
                .join(
                    PipelineDestinationTableSync, 
                    PipelineDestinationTableSync.pipeline_destination_id == PipelineDestination.id
                )
                .join(
                    PipelineDestinationTableSyncTag,
                    PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id == PipelineDestinationTableSync.id
                )
                .join(
                    Destination,
                    Destination.id == PipelineDestination.destination_id
                )
                .where(PipelineDestinationTableSyncTag.tag_id == tag_id)
            ).all()

        except SQLAlchemyError as e:
            logger.error(
                "Failed to get tag usage details", extra={"tag_id": tag_id, "error": str(e)}
            )
            raise DatabaseError("Failed to get tag usage details") from e

    def get_all_ordered(self, skip: int = 0, limit: int = 100) -> List[TagList]:
        """
        Get all tags ordered by name.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records

        Returns:
            List of tags

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                select(TagList).order_by(TagList.tag).offset(skip).limit(limit)
            )
            return list(result.scalars().all())

        except SQLAlchemyError as e:
            logger.error("Failed to get all tags", extra={"error": str(e)})
            raise DatabaseError("Failed to get all tags") from e

    def get_all_with_usage_count(
        self,
        pipeline_id: Optional[int] = None,
        destination_id: Optional[int] = None,
        source_id: Optional[int] = None,
    ) -> List[tuple]:
        """
        Get all tags with their usage count.

        Returns:
            List of tuples (TagList, usage_count)

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            query = select(
                TagList,
                func.count(PipelineDestinationTableSyncTag.id).label("usage_count"),
            )

            has_filter = (
                pipeline_id is not None
                or destination_id is not None
                or source_id is not None
            )

            if has_filter:
                query = (
                    query.join(
                        PipelineDestinationTableSyncTag,
                        PipelineDestinationTableSyncTag.tag_id == TagList.id,
                    )
                    .join(
                        PipelineDestinationTableSync,
                        PipelineDestinationTableSync.id
                        == PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id,
                    )
                    .join(
                        PipelineDestination,
                        PipelineDestination.id
                        == PipelineDestinationTableSync.pipeline_destination_id,
                    )
                    .join(Pipeline, Pipeline.id == PipelineDestination.pipeline_id)
                )

                if pipeline_id is not None:
                    query = query.where(Pipeline.id == pipeline_id)

                if destination_id is not None:
                    query = query.where(PipelineDestination.destination_id == destination_id)

                if source_id is not None:
                    query = query.where(Pipeline.source_id == source_id)
            else:
                query = query.outerjoin(
                    PipelineDestinationTableSyncTag,
                    PipelineDestinationTableSyncTag.tag_id == TagList.id,
                )

            query = query.group_by(TagList.id).order_by(TagList.tag)
            result = self.db.execute(query)
            return list(result.all())

        except SQLAlchemyError as e:
            logger.error("Failed to get tags with usage count", extra={"error": str(e)})
            raise DatabaseError("Failed to get tags with usage count") from e

    def get_tag_relations(self) -> dict:
        """
        Get tag nodes and edges for visualization.
        Two tags are related if they share the same table_sync.
        
        Returns:
            dict with 'nodes' (list of tag info) and 'edges' (list of tag-pair relations)
        """
        try:
            # Get all tags with usage count
            tags_result = self.db.execute(
                select(
                    TagList.id,
                    TagList.tag,
                    func.count(PipelineDestinationTableSyncTag.id).label("usage_count"),
                )
                .outerjoin(
                    PipelineDestinationTableSyncTag,
                    PipelineDestinationTableSyncTag.tag_id == TagList.id,
                )
                .group_by(TagList.id)
                .order_by(TagList.tag)
            ).all()

            nodes = [
                {"id": row.id, "tag": row.tag, "usage_count": row.usage_count}
                for row in tags_result
            ]

            # Find edges: two tags that share a table_sync
            tag_a = aliased(PipelineDestinationTableSyncTag)
            tag_b = aliased(PipelineDestinationTableSyncTag)

            edges_result = self.db.execute(
                select(
                    tag_a.tag_id.label("source"),
                    tag_b.tag_id.label("target"),
                    func.count().label("shared_tables"),
                )
                .join(
                    tag_b,
                    and_(
                        tag_a.pipelines_destination_table_sync_id == tag_b.pipelines_destination_table_sync_id,
                        tag_a.tag_id < tag_b.tag_id,  # avoid duplicates & self-loops
                    ),
                )
                .group_by(tag_a.tag_id, tag_b.tag_id)
            ).all()

            edges = [
                {"source": row.source, "target": row.target, "shared_tables": row.shared_tables}
                for row in edges_result
            ]

            return {"nodes": nodes, "edges": edges}

        except SQLAlchemyError as e:
            logger.error("Failed to get tag relations", extra={"error": str(e)})
            raise DatabaseError("Failed to get tag relations") from e

    def is_tag_unused(self, tag_id: int) -> bool:
        """
        Check if a tag is not associated with any table sync.

        Args:
            tag_id: Tag identifier

        Returns:
            True if tag is unused (no associations), False otherwise

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            count = self.db.execute(
                select(func.count(PipelineDestinationTableSyncTag.id))
                .where(PipelineDestinationTableSyncTag.tag_id == tag_id)
            ).scalar()

            return count == 0

        except SQLAlchemyError as e:
            logger.error(
                "Failed to check if tag is unused",
                extra={"tag_id": tag_id, "error": str(e)},
            )
            raise DatabaseError("Failed to check tag usage") from e


class TableSyncTagRepository:
    """
    Repository for pipeline destination table sync tag associations.
    """

    def __init__(self, db: Session):
        """
        Initialize table sync tag repository.

        Args:
            db: Database session
        """
        self.db = db

    def get_tags_for_table_sync(
        self, table_sync_id: int
    ) -> List[PipelineDestinationTableSyncTag]:
        """
        Get all tags for a specific table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID

        Returns:
            List of tag associations

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                select(PipelineDestinationTableSyncTag)
                .where(
                    PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id
                    == table_sync_id
                )
                .options(joinedload(PipelineDestinationTableSyncTag.tag_item))
                .join(PipelineDestinationTableSyncTag.tag_item)
                .order_by(TagList.tag)
            )
            return list(result.scalars().all())

        except SQLAlchemyError as e:
            logger.error(
                "Failed to get tags for table sync",
                extra={"table_sync_id": table_sync_id, "error": str(e)},
            )
            raise DatabaseError("Failed to get tags for table sync") from e

    def add_tag_to_table_sync(
        self, table_sync_id: int, tag_id: int
    ) -> PipelineDestinationTableSyncTag:
        """
        Add tag to table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID
            tag_id: Tag ID

        Returns:
            Created association

        Raises:
            DuplicateEntityError: If association already exists
            DatabaseError: If database operation fails
        """
        try:
            # Check if association already exists
            existing = self.db.execute(
                select(PipelineDestinationTableSyncTag).where(
                    PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id
                    == table_sync_id,
                    PipelineDestinationTableSyncTag.tag_id == tag_id,
                )
            ).scalar_one_or_none()

            if existing:
                logger.debug(
                    f"Tag association already exists",
                    extra={"table_sync_id": table_sync_id, "tag_id": tag_id},
                )
                return existing

            # Create new association
            association = PipelineDestinationTableSyncTag(
                pipelines_destination_table_sync_id=table_sync_id, tag_id=tag_id
            )
            self.db.add(association)
            self.db.flush()
            self.db.refresh(association)

            logger.info(
                f"Added tag to table sync",
                extra={
                    "table_sync_id": table_sync_id,
                    "tag_id": tag_id,
                    "association_id": association.id,
                },
            )

            return association

        except IntegrityError as e:
            self.db.rollback()
            logger.warning(
                "Duplicate tag association",
                extra={"table_sync_id": table_sync_id, "tag_id": tag_id, "error": str(e)},
            )
            raise DuplicateEntityError(
                entity_type="TableSyncTag",
                field="tag_id",
                value=str(tag_id),
            ) from e
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                "Failed to add tag to table sync",
                extra={"table_sync_id": table_sync_id, "tag_id": tag_id, "error": str(e)},
            )
            raise DatabaseError("Failed to add tag to table sync") from e

    def remove_tag_from_table_sync(self, table_sync_id: int, tag_id: int) -> bool:
        """
        Remove tag from table sync.

        Args:
            table_sync_id: Pipeline destination table sync ID
            tag_id: Tag ID

        Returns:
            True if removed, False if association didn't exist

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                delete(PipelineDestinationTableSyncTag).where(
                    PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id
                    == table_sync_id,
                    PipelineDestinationTableSyncTag.tag_id == tag_id,
                )
            )
            self.db.flush()

            deleted = result.rowcount > 0

            if deleted:
                logger.info(
                    f"Removed tag from table sync",
                    extra={"table_sync_id": table_sync_id, "tag_id": tag_id},
                )
            else:
                logger.debug(
                    f"Tag association not found",
                    extra={"table_sync_id": table_sync_id, "tag_id": tag_id},
                )

            return deleted

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                "Failed to remove tag from table sync",
                extra={"table_sync_id": table_sync_id, "tag_id": tag_id, "error": str(e)},
            )
            raise DatabaseError("Failed to remove tag from table sync") from e

    def get_table_syncs_by_tag(self, tag_id: int) -> List[int]:
        """
        Get all table sync IDs associated with a tag.

        Args:
            tag_id: Tag ID

        Returns:
            List of table sync IDs

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                select(
                    PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id
                ).where(PipelineDestinationTableSyncTag.tag_id == tag_id)
            )
            return list(result.scalars().all())

        except SQLAlchemyError as e:
            logger.error(
                "Failed to get table syncs by tag",
                extra={"tag_id": tag_id, "error": str(e)},
            )
            raise DatabaseError("Failed to get table syncs by tag") from e
