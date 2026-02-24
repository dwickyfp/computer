"""
Lineage API endpoints.

Provides endpoints for fetching and regenerating data lineage.
"""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from app.api.deps import get_db, get_db_readonly, get_pipeline_service, get_pipeline_service_readonly
from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.models.pipeline import PipelineDestinationTableSync
from app.domain.repositories.notification_log_repo import NotificationLogRepository
from app.domain.schemas.notification_log import NotificationLogCreate
from app.domain.services.pipeline import PipelineService

router = APIRouter()
settings = get_settings()
logger = get_logger(__name__)


@router.get(
    "/{pipeline_id}/destinations/{dest_id}/tables/{sync_id}",
    response_model=dict,
    summary="Get table sync details",
    description="Get complete details for a table sync config including lineage",
)
def get_table_sync_details(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    dest_id: int = Path(..., description="Pipeline Destination ID"),
    sync_id: int = Path(..., description="Table Sync Config ID"),
    db: Session = Depends(get_db_readonly),
    pipeline_service: PipelineService = Depends(get_pipeline_service_readonly),
) -> dict[str, Any]:
    """Get complete details for a table sync config including lineage."""
    sync = (
        db.query(PipelineDestinationTableSync)
        .filter(
            PipelineDestinationTableSync.id == sync_id,
            PipelineDestinationTableSync.pipeline_destination_id == dest_id,
        )
        .first()
    )

    if not sync:
        raise HTTPException(status_code=404, detail="Table sync not found")

    # Get pipeline and related info
    pipeline = pipeline_service.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Get destination info
    dest = next((d for d in pipeline.destinations if d.id == dest_id), None)

    # Get tags
    tags = (
        [assoc.tag_item.tag for assoc in sync.tag_associations]
        if sync.tag_associations
        else []
    )

    # Get data flow count
    from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring

    data_flow = (
        db.query(DataFlowRecordMonitoring)
        .filter(DataFlowRecordMonitoring.pipeline_destination_table_sync_id == sync_id)
        .first()
    )
    record_count = data_flow.record_count if data_flow else 0

    return {
        "id": sync.id,
        "pipeline": {
            "id": pipeline.id,
            "name": pipeline.name,
            "status": pipeline.status,
        },
        "source": {
            "id": pipeline.source.id,
            "name": pipeline.source.name,
            "database": pipeline.source.pg_database,
        },
        "destination": {
            "id": dest.destination.id if dest else None,
            "name": dest.destination.name if dest else None,
            "type": dest.destination.type if dest else None,
        },
        "table_name": sync.table_name,
        "table_name_target": sync.table_name_target,
        "custom_sql": sync.custom_sql,
        "filter_sql": sync.filter_sql,
        "primary_key_column_target": sync.primary_key_column_target,
        "tags": tags,
        "record_count": record_count,
        "is_error": sync.is_error,
        "error_message": sync.error_message,
        "lineage_metadata": sync.lineage_metadata,
        "lineage_status": sync.lineage_status or "PENDING",
        "lineage_error": sync.lineage_error,
        "lineage_generated_at": sync.lineage_generated_at,
        "created_at": sync.created_at,
        "updated_at": sync.updated_at,
    }


@router.get(
    "/{pipeline_id}/destinations/{dest_id}/tables/{sync_id}/lineage",
    response_model=dict,
    summary="Get table lineage",
    description="Get lineage metadata for a specific table sync config",
)
def get_table_lineage(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    dest_id: int = Path(..., description="Pipeline Destination ID"),
    sync_id: int = Path(..., description="Table Sync Config ID"),
    db: Session = Depends(get_db_readonly),
) -> dict[str, Any]:
    """Get lineage metadata for a specific table sync config."""
    sync = (
        db.query(PipelineDestinationTableSync)
        .filter(
            PipelineDestinationTableSync.id == sync_id,
            PipelineDestinationTableSync.pipeline_destination_id == dest_id,
        )
        .first()
    )

    if not sync:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Table sync configuration not found",
        )

    return {
        "id": sync.id,
        "table_name": sync.table_name,
        "table_name_target": sync.table_name_target,
        "custom_sql": sync.custom_sql,
        "filter_sql": sync.filter_sql,
        "lineage_metadata": sync.lineage_metadata,
        "lineage_status": sync.lineage_status or "PENDING",
        "lineage_error": sync.lineage_error,
        "lineage_generated_at": sync.lineage_generated_at,
    }


@router.post(
    "/{pipeline_id}/destinations/{dest_id}/tables/{sync_id}/lineage/generate",
    response_model=dict,
    summary="Generate lineage",
    description="Trigger lineage generation for a table sync config",
)
def generate_table_lineage(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    dest_id: int = Path(..., description="Pipeline Destination ID"),
    sync_id: int = Path(..., description="Table Sync Config ID"),
    db: Session = Depends(get_db),
    pipeline_service: PipelineService = Depends(get_pipeline_service),
) -> dict[str, Any]:
    """
    Trigger lineage generation for a table sync config.

    If worker is enabled, dispatches async task.
    Otherwise, generates synchronously.
    """
    sync = (
        db.query(PipelineDestinationTableSync)
        .filter(
            PipelineDestinationTableSync.id == sync_id,
            PipelineDestinationTableSync.pipeline_destination_id == dest_id,
        )
        .first()
    )

    if not sync:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Table sync configuration not found",
        )

    # Get pipeline to access source
    pipeline = pipeline_service.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Get source columns from table metadata
    source_columns = _get_source_table_columns(db, pipeline.source_id, sync.table_name)

    # Update status to GENERATING
    sync.lineage_status = "GENERATING"
    sync.lineage_error = None
    db.commit()

    if settings.worker_enabled:
        # Dispatch to worker
        from app.infrastructure.worker_client import get_worker_client

        try:
            worker = get_worker_client()
            task_id = worker.submit_lineage_task(
                table_sync_id=sync.id,
                custom_sql=sync.custom_sql,
                source_table=sync.table_name,
                source_columns=source_columns,
            )
            logger.info(
                f"Lineage task submitted for sync_id={sync_id}",
                extra={"task_id": task_id},
            )
            return {
                "status": "submitted",
                "task_id": task_id,
                "message": "Lineage generation started in background",
            }
        except Exception as e:
            logger.error(f"Failed to submit lineage task: {e}")
            sync.lineage_status = "FAILED"
            sync.lineage_error = f"Worker error: {str(e)}"
            db.commit()
            # Push notification for lineage dispatch failure
            try:
                NotificationLogRepository(db).upsert_notification_by_key(
                    NotificationLogCreate(
                        key_notification=f"lineage_error_sync_{sync_id}",
                        title=f"Lineage Generation Failed — {sync.table_name}",
                        message=(
                            f"Table sync ID {sync_id} (table: {sync.table_name}) lineage could not be "
                            f"dispatched to the worker. Error: {e}"
                        ),
                        type="ERROR",
                        is_read=False,
                        is_deleted=False,
                        iteration_check=1,
                        is_sent=False,
                    )
                )
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=str(e))
    else:
        # Generate synchronously
        from app.domain.services.lineage_parser import parse_lineage

        try:
            lineage_metadata = parse_lineage(
                sql=sync.custom_sql,
                source_table=sync.table_name,
                source_columns=source_columns,
            )
            sync.lineage_metadata = lineage_metadata
            sync.lineage_status = "COMPLETED"
            sync.lineage_generated_at = datetime.utcnow()
            db.commit()

            return {
                "status": "completed",
                "lineage": lineage_metadata,
            }
        except Exception as e:
            sync.lineage_status = "FAILED"
            sync.lineage_error = str(e)
            db.commit()
            # Push notification for sync lineage generation failure
            try:
                NotificationLogRepository(db).upsert_notification_by_key(
                    NotificationLogCreate(
                        key_notification=f"lineage_error_sync_{sync_id}",
                        title=f"Lineage Generation Failed — {sync.table_name}",
                        message=(
                            f"Table sync ID {sync_id} (table: {sync.table_name}) lineage generation failed. "
                            f"Error: {e}"
                        ),
                        type="ERROR",
                        is_read=False,
                        is_deleted=False,
                        iteration_check=1,
                        is_sent=False,
                    )
                )
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=str(e))


def _get_source_table_columns(
    db: Session,
    source_id: int,
    table_name: str,
) -> list[str]:
    """Get column names from table metadata."""
    from app.domain.models.table_metadata import TableMetadata

    metadata = (
        db.query(TableMetadata)
        .filter(
            TableMetadata.source_id == source_id,
            TableMetadata.table_name == table_name,
        )
        .first()
    )

    if not metadata or not metadata.schema_table:
        return []

    # schema_table is a list of column definitions
    columns = metadata.schema_table
    if isinstance(columns, list):
        return [col.get("name") or col.get("column_name", "") for col in columns]

    return []
