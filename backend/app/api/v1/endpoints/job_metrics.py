"""
Job Metrics API endpoints.
"""

from typing import Any, Iterable, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.domain.models.job_metric import JobMetric
from app.domain.repositories.job_metric import JobMetricRepository

router = APIRouter()

HIDDEN_JOB_METRIC_KEYS = {
    # Retired scheduler job kept in historical DB rows only.
    "client_database_sync",
}


def serialize_job_metrics(metrics: Iterable[JobMetric]) -> list[dict[str, Any]]:
    visible_metrics = [
        metric
        for metric in metrics
        if metric.key_job_scheduler not in HIDDEN_JOB_METRIC_KEYS
    ]
    visible_metrics.sort(key=lambda metric: metric.last_run_at, reverse=True)

    return [
        {
            "key_job_scheduler": metric.key_job_scheduler,
            "last_run_at": metric.last_run_at,
            "created_at": metric.created_at,
            "updated_at": metric.updated_at,
        }
        for metric in visible_metrics
    ]


@router.get("", response_model=List[dict])
def get_job_metrics(
    db: Session = Depends(get_db),
) -> Any:
    """
    Get all job metrics.
    """
    repository = JobMetricRepository(db)
    metrics = repository.get_all()

    return serialize_job_metrics(metrics)
