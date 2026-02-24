"""
WAL metrics endpoints.

Provides REST API for querying WAL size metrics.
"""

from typing import List

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_db
from app.domain.repositories.wal_metric import WALMetricRepository
from app.domain.schemas.wal_metric import WALMetricResponse, WALMetricsQuery
from sqlalchemy.orm import Session

router = APIRouter()


@router.get(
    "/wal",
    response_model=List[WALMetricResponse],
    summary="Get WAL metrics",
    description="Query historical WAL size metrics with optional filtering",
)
def get_wal_metrics(
    source_id: int | None = Query(None, description="Filter by source ID"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of metrics to return"
    ),
    db: Session = Depends(get_db),
) -> List[WALMetricResponse]:
    """
    Get WAL metrics.

    Args:
        source_id: Optional source ID to filter by
        limit: Maximum number of metrics to return
        db: Database session

    Returns:
        List of WAL metrics ordered by timestamp (newest first)
    """
    repo = WALMetricRepository(db)

    if source_id is not None:
        metrics = repo.get_by_source(source_id, limit=limit)
    else:
        metrics = repo.get_by_time_range(limit=limit)

    # Convert to response models and add calculated fields
    response_metrics = []
    for metric in metrics:
        response = WALMetricResponse.from_orm(metric)
        # Add calculated fields
        response.size_mb = metric.size_mb
        response.size_gb = metric.size_gb
        response_metrics.append(response)

    return response_metrics
