"""
Pipeline repository for data access operations.

Extends base repository with pipeline-specific queries.
"""

from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.domain.models.pipeline import Pipeline, PipelineMetadata, PipelineStatus, PipelineProgress, PipelineDestination
from app.domain.repositories.base import BaseRepository


class PipelineRepository(BaseRepository[Pipeline]):
    """
    Repository for Pipeline entity.

    Provides data access methods for ETL pipeline configurations.
    """

    def __init__(self, db: Session):
        """Initialize pipeline repository."""
        super().__init__(Pipeline, db)

    def get_by_id_with_relations(self, pipeline_id: int) -> Pipeline:
        """
        Get pipeline by ID with all related entities loaded.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Pipeline with source, destination, and metadata
        """
        result = self.db.execute(
            select(Pipeline)
            .options(
                selectinload(Pipeline.source),
                selectinload(Pipeline.destinations).selectinload(
                    PipelineDestination.destination
                ),
                selectinload(Pipeline.pipeline_metadata),
                # selectinload(Pipeline.pipeline_progress),
            )
            .where(Pipeline.id == pipeline_id)
        )
        pipeline = result.scalar_one_or_none()

        if pipeline is None:
            from app.core.exceptions import EntityNotFoundError

            raise EntityNotFoundError(entity_type="Pipeline", entity_id=pipeline_id)

        return pipeline

    def get_all_with_relations(self, skip: int = 0, limit: int = 100) -> List[Pipeline]:
        """
        Get all pipelines with related entities loaded.

        Args:
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with relations
        """
        result = self.db.execute(
            select(Pipeline)
            .options(
                selectinload(Pipeline.source),
                selectinload(Pipeline.destinations).selectinload(
                    PipelineDestination.destination
                ),
                selectinload(Pipeline.pipeline_metadata),
                # selectinload(Pipeline.pipeline_progress),
            )
            .order_by(Pipeline.name.asc(), Pipeline.id.asc())
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())

    def get_by_status(
        self, status: PipelineStatus, skip: int = 0, limit: int = 100
    ) -> List[Pipeline]:
        """
        Get pipelines by status.

        Args:
            status: Pipeline status to filter by
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with specified status
        """
        result = self.db.execute(
            select(Pipeline)
            .where(Pipeline.status == status.value)
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())

    def create_with_metadata(self, **pipeline_data) -> Pipeline:
        """
        Create pipeline with associated metadata.

        Args:
            **pipeline_data: Pipeline attributes

        Returns:
            Created pipeline with metadata
        """
        # Create pipeline
        pipeline = self.create(**pipeline_data)

        # Create associated metadata
        metadata = PipelineMetadata(pipeline_id=pipeline.id, status="RUNNING")
        self.db.add(metadata)
        
        # progress = PipelineProgress(pipeline_id=pipeline.id, status="PENDING")
        # self.db.add(progress)

        self.db.flush()
        # self.db.refresh(pipeline)

        return pipeline
    def get_by_source_id(self, source_id: int) -> List[Pipeline]:
        """
        Get all pipelines for a specific source.
        
        Args:
            source_id: Source identifier
            
        Returns:
            List of pipelines with destination loaded
        """
        from app.domain.models.destination import Destination
        
        result = self.db.execute(
            select(Pipeline)
            .options(
                selectinload(Pipeline.destinations).selectinload(PipelineDestination.destination)
            )
            .where(Pipeline.source_id == source_id)
        )
        return list(result.scalars().all())
