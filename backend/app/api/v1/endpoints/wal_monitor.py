"""
WAL Monitor API endpoints.

Provides REST API for managing real-time WAL monitoring status.
Implements upsert pattern: 1 source = 1 monitor record.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.schemas.wal_monitor import (
    WALMonitorCreate,
    WALMonitorListResponse,
    WALMonitorResponse,
    WALMonitorStatusUpdate,
)
from app.domain.services.wal_monitor_service import WALMonitorService

logger = get_logger(__name__)
router = APIRouter(prefix="/wal-monitor", tags=["WAL Monitor"])


@router.post(
    "/sources/{source_id}",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Upsert WAL monitor record",
    description=(
        "Create or update WAL monitor record for a source. "
        "If record exists, it will be updated. Otherwise, created. "
        "Ensures 1 source has exactly 1 monitor record."
    ),
)
def upsert_wal_monitor(
    source_id: int,
    data: WALMonitorCreate,
    db: Session = Depends(get_db),
):
    """
    Create or update WAL monitor record for a source.

    This endpoint implements upsert logic:
    - If monitor record exists for source_id -> UPDATE
    - If monitor record doesn't exist -> INSERT

    Args:
        source_id: Source identifier
        data: WAL monitor data
        db: Database session

    Returns:
        Created or updated WAL monitor record

    Raises:
        404: Source not found
        500: Database error
    """
    try:
        # Ensure source_id in path matches body (if provided)
        data.source_id = source_id

        service = WALMonitorService(db)
        monitor = service.upsert_monitor(source_id, data)

        logger.info(
            "WAL monitor upserted via API",
            extra={"source_id": source_id, "monitor_id": monitor.id},
        )

        return monitor

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to upsert WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upsert WAL monitor record",
        )


@router.get(
    "/sources/{source_id}",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Get WAL monitor by source",
    description="Retrieve WAL monitor record for a specific source",
)
def get_wal_monitor_by_source(
    source_id: int,
    db: Session = Depends(get_db),
):
    """
    Get WAL monitor record for a source.

    Args:
        source_id: Source identifier
        db: Database session

    Returns:
        WAL monitor record

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        monitor = service.get_by_source(source_id)

        if not monitor:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"WAL monitor not found for source_id={source_id}",
            )

        return monitor

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve WAL monitor record",
        )


@router.get(
    "/",
    response_model=WALMonitorListResponse,
    status_code=status.HTTP_200_OK,
    summary="List all WAL monitors",
    description="Retrieve all WAL monitor records with threshold status",
)
def list_wal_monitors(
    db: Session = Depends(get_db),
):
    """
    Get all WAL monitor records with threshold calculation.

    Args:
        db: Database session

    Returns:
        List of all WAL monitor records with threshold status

    Raises:
        500: Database error
    """
    try:
        from app.domain.services.configuration import ConfigurationService
        import httpx
        import re
        
        service = WALMonitorService(db)
        config_service = ConfigurationService(db)
        
        # Get thresholds from configuration
        thresholds = config_service.get_wal_thresholds()
        
        monitors = service.list_monitors()
        
        # Add threshold status to each monitor
        for monitor in monitors:
            # Parse WAL size to bytes if it's a string
            wal_size_bytes = 0
            if monitor.total_wal_size:
                # Parse formats like "5.2 MB", "1.5 GB", etc.
                match = re.match(r'([\d.]+)\s*([KMGT]?B)', monitor.total_wal_size, re.IGNORECASE)
                if match:
                    value = float(match.group(1))
                    unit = match.group(2).upper()
                    
                    multipliers = {
                        'B': 1,
                        'KB': 1024,
                        'MB': 1024 ** 2,
                        'GB': 1024 ** 3,
                        'TB': 1024 ** 4
                    }
                    wal_size_bytes = int(value * multipliers.get(unit, 1))
            
            # Set wal_size_bytes attribute
            monitor.wal_size_bytes = wal_size_bytes
            
            # Determine threshold status
            if wal_size_bytes < thresholds.warning:
                monitor.wal_threshold_status = "OK"
            elif wal_size_bytes < thresholds.error:
                monitor.wal_threshold_status = "WARNING"
            else:
                monitor.wal_threshold_status = "ERROR"
    
        
        return WALMonitorListResponse(
            monitors=monitors,
            total=len(monitors),
        )

    except Exception as e:
        logger.error("Failed to list WAL monitors", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve WAL monitor records",
        )


@router.patch(
    "/sources/{source_id}/status",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Update WAL monitor status",
    description="Update status field of WAL monitor record",
)
def update_wal_monitor_status(
    source_id: int,
    data: WALMonitorStatusUpdate,
    db: Session = Depends(get_db),
):
    """
    Update WAL monitor status.

    Args:
        source_id: Source identifier
        data: Status update data
        db: Database session

    Returns:
        Updated WAL monitor record

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        monitor = service.update_status(source_id, data)

        logger.info(
            "WAL monitor status updated via API",
            extra={"source_id": source_id, "status": data.status},
        )

        return monitor

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to update WAL monitor status", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update WAL monitor status",
        )


@router.delete(
    "/sources/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete WAL monitor record",
    description="Delete WAL monitor record for a source",
)
def delete_wal_monitor(
    source_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete WAL monitor record for a source.

    Args:
        source_id: Source identifier
        db: Database session

    Returns:
        No content on success

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        service.delete_by_source(source_id)

        logger.info("WAL monitor deleted via API", extra={"source_id": source_id})

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to delete WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete WAL monitor record",
        )
