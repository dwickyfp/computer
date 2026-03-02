"""
Pipeline Provision Service — handles Snowflake resource provisioning.

Extracted from PipelineService (#4) for single-responsibility.
Delegates to PipelineService methods for backward compatibility.
"""

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.services.pipeline import PipelineService

logger = get_logger(__name__)


class PipelineProvisionService:
    """
    Service for provisioning pipeline resources in destination warehouses.

    Handles Snowflake table creation, stream/task setup, DDL generation,
    and full pipeline initialization workflows.
    """

    def __init__(self, db: Session):
        self.db = db
        self._pipeline_service = PipelineService(db)

    def initialize_pipeline(self, pipeline_id: int) -> None:
        """Background task to initialize pipeline resources in Snowflake."""
        return self._pipeline_service.initialize_pipeline(pipeline_id)

    def init_snowflake_table(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> dict:
        """Initialize Snowflake objects for a single table."""
        return self._pipeline_service.init_snowflake_table(
            pipeline_id, pipeline_destination_id, table_name
        )
