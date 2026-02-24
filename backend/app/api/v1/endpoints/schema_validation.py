"""
Schema Compatibility Validation endpoint (B2).

Validates source/destination schema compatibility before pipeline start.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.deps import get_db
from app.core.logging import get_logger
from app.domain.services.schema_compatibility import SchemaCompatibilityService

from sqlalchemy.orm import Session

logger = get_logger(__name__)
router = APIRouter()


@router.get(
    "/validate-schema",
    summary="Validate schema compatibility",
)
def validate_schema(
    source_id: int = Query(..., description="Source database ID"),
    table_name: str = Query(..., description="Source table name"),
    destination_id: int = Query(..., description="Destination ID"),
    target_table: str = Query(default=None, description="Destination table name"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Validate schema compatibility between source and destination tables.

    Returns compatibility result with errors and warnings.
    Should be called before starting a pipeline to catch mismatches early.
    """
    try:
        service = SchemaCompatibilityService(db)
        result = service.validate_pipeline_schemas(
            source_id=source_id,
            table_name=table_name,
            destination_id=destination_id,
            target_table=target_table,
        )
        return result.to_dict()
    except Exception as e:
        logger.error(f"Schema validation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema validation error: {e}",
        )
