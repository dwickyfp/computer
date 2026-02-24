from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import get_logger
from app.domain.schemas.system_metric import SystemMetricResponse
from app.domain.services.system_metric import SystemMetricService

logger = get_logger(__name__)
router = APIRouter(prefix="/system-metrics", tags=["System Metrics"])


@router.get(
    "/latest",
    response_model=SystemMetricResponse,
    status_code=status.HTTP_200_OK,
    summary="Get latest system metrics",
    description="Retrieve the most recent system metrics (CPU, Memory, Swap)",
)
def get_latest_system_metrics(
    db: Session = Depends(get_db),
):
    """
    Get latest system metrics.

    Args:
        db: Database session

    Returns:
        Latest system metrics
    """
    try:
        service = SystemMetricService(db)
        metric = service.get_latest_metrics()

        if not metric:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No system metrics found",
            )

        return metric

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get system metrics", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system metrics",
        )


@router.get(
    "/history",
    response_model=List[SystemMetricResponse],
    status_code=status.HTTP_200_OK,
    summary="Get system metrics history",
    description="Retrieve historical system metrics",
)
def get_system_metrics_history(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """
    Get system metrics history.

    Args:
        limit: limit number of records
        db: Database session

    Returns:
        List of system metrics
    """
    try:
        service = SystemMetricService(db)
        metrics = service.get_metrics_history(limit)
        return metrics

    except Exception as e:
        logger.error("Failed to get system metrics history", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system metrics history",
        )
