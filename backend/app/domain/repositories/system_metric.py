from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.domain.models.system_metric import SystemMetric
from app.domain.schemas.system_metric import SystemMetricCreate


class SystemMetricRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, obj_in: SystemMetricCreate) -> SystemMetric:
        db_obj = SystemMetric(
            cpu_usage=obj_in.cpu_usage,
            total_memory=obj_in.total_memory,
            used_memory=obj_in.used_memory,
            total_swap=obj_in.total_swap,
            used_swap=obj_in.used_swap,
        )
        self.db.add(db_obj)
        self.db.commit()
        self.db.refresh(db_obj)
        return db_obj

    def get_latest(self) -> Optional[SystemMetric]:
        return (
            self.db.query(SystemMetric).order_by(desc(SystemMetric.recorded_at)).first()
        )

    def get_history(self, limit: int = 100) -> List[SystemMetric]:
        return (
            self.db.query(SystemMetric)
            .order_by(desc(SystemMetric.recorded_at))
            .limit(limit)
            .all()
        )
