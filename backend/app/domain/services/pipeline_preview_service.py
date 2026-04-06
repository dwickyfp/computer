"""
Pipeline Preview Service — handles data preview and SQL validation.

Extracted from PipelineService (#4) for single-responsibility.
Delegates to PipelineService methods for backward compatibility.
"""

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.services.pipeline import PipelineService
from app.domain.schemas.pipeline import TableValidationResponse
from app.domain.schemas.pipeline_preview import (
    PipelinePreviewRequest,
    PipelinePreviewResponse,
)

logger = get_logger(__name__)


class PipelinePreviewService:
    """
    Service for previewing pipeline data and validating SQL/table names.

    Handles DuckDB-based preview execution and target table validation
    against Snowflake/Postgres destinations.
    """

    def __init__(self, db: Session):
        self.db = db
        self._pipeline_service = PipelineService(db)

    def preview_custom_sql(
        self, request: PipelinePreviewRequest
    ) -> PipelinePreviewResponse:
        """Preview table data using DuckDB with attached Postgres databases."""
        return self._pipeline_service.preview_custom_sql(request)

    def validate_target_table(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> TableValidationResponse:
        """Validate table name for a destination."""
        return self._pipeline_service.validate_target_table(
            pipeline_id, pipeline_destination_id, table_name
        )
