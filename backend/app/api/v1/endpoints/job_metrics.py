"""
Job Metrics API endpoints.
"""

from typing import Any, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.domain.models.job_metric import JobMetric
from app.domain.repositories.job_metric import JobMetricRepository

router = APIRouter()


@router.get("", response_model=List[dict])
def get_job_metrics(
    db: Session = Depends(get_db),
) -> Any:
    """
    Get all job metrics.
    """
    repository = JobMetricRepository(db)
    metrics = repository.get_all()
    
    # Simple list response for now, can be Pydantic model
    return [
        {
            "key_job_scheduler": m.key_job_scheduler,
            "last_run_at": m.last_run_at,
            "created_at": m.created_at,
            "updated_at": m.updated_at
        }
        for m in metrics
    ]
