"""
Job Metric repository.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.domain.models.job_metric import JobMetric
from app.domain.repositories.base import BaseRepository


class JobMetricRepository(BaseRepository[JobMetric]):
    """
    Repository for JobMetric entity.
    """

    def __init__(self, db: Session):
        super().__init__(JobMetric, db)

    def upsert_metric(self, key: str, last_run: datetime) -> JobMetric:
        """
        Upsert job metric.

        If key exists, update last_run_at.
        If not, insert new record.
        """
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        stmt = insert(JobMetric).values(
            key_job_scheduler=key,
            last_run_at=last_run,
            updated_at=now
        )
        
        stmt = stmt.on_conflict_do_update(
            index_elements=[JobMetric.key_job_scheduler],
            set_={
                "last_run_at": stmt.excluded.last_run_at,
                "updated_at": now
            }
        )
        
        self.db.execute(stmt)
        # We handle commit/flush at service layer usually, but for metric recording 
        # it's often fire-and-forget or implicit commit. 
        # BaseRepository methods invoke flush(), but execute() does not.
        self.db.flush()
        
        # Determine if we should return the object? 
        # For performance we usually don't fetch back immediately unless needed.
        # But to be consistent with repo pattern:
        return self.get_by_key(key)

    def get_by_key(self, key: str) -> JobMetric:
        """Get metric by job key."""
        return self.db.query(JobMetric).filter(JobMetric.key_job_scheduler == key).first()
