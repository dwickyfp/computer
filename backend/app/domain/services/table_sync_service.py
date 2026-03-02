"""
Table Sync Service — manages table synchronization configurations.

Extracted from PipelineService (#4) for single-responsibility.
Delegates to PipelineService methods for backward compatibility.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.services.pipeline import PipelineService

logger = get_logger(__name__)


class TableSyncService:
    """
    Service for managing table sync configurations within a pipeline.

    Handles CRUD for PipelineDestinationTableSync entities and
    tag cleanup for orphaned tags.
    """

    def __init__(self, db: Session):
        self.db = db
        self._pipeline_service = PipelineService(db)

    def get_destination_tables(self, pipeline_id: int, pipeline_destination_id: int):
        """Get tables available for sync with current configuration."""
        return self._pipeline_service.get_destination_tables(
            pipeline_id, pipeline_destination_id
        )

    def save_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_sync_data
    ):
        """Create or update table sync configuration."""
        return self._pipeline_service.save_table_sync(
            pipeline_id, pipeline_destination_id, table_sync_data
        )

    def save_table_syncs_bulk(
        self, pipeline_id: int, pipeline_destination_id: int, bulk_request
    ):
        """Bulk create or update table sync configurations."""
        return self._pipeline_service.save_table_syncs_bulk(
            pipeline_id, pipeline_destination_id, bulk_request
        )

    def delete_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ):
        """Remove table from sync configuration."""
        return self._pipeline_service.delete_table_sync(
            pipeline_id, pipeline_destination_id, table_name
        )

    def delete_table_sync_by_id(
        self, pipeline_id: int, pipeline_destination_id: int, sync_config_id: int
    ):
        """Remove specific table sync by ID."""
        return self._pipeline_service.delete_table_sync_by_id(
            pipeline_id, pipeline_destination_id, sync_config_id
        )
