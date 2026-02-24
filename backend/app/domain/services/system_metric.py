import psutil
from typing import List, Optional
from sqlalchemy.orm import Session
from app.domain.repositories.system_metric import SystemMetricRepository
from app.domain.schemas.system_metric import SystemMetricCreate
from app.domain.models.system_metric import SystemMetric

class SystemMetricService:
    def __init__(self, db: Session):
        self.repository = SystemMetricRepository(db)

    def collect_and_save_metrics(self) -> SystemMetric:
        # Get system metrics using psutil
        cpu_usage = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()

        metric_data = SystemMetricCreate(
            cpu_usage=cpu_usage,
            total_memory=memory.total,
            used_memory=memory.used,
            total_swap=swap.total,
            used_swap=swap.used,
        )
        
        return self.repository.create(metric_data)

    def get_latest_metrics(self) -> Optional[SystemMetric]:
        return self.repository.get_latest()

    def get_metrics_history(self, limit: int = 100) -> List[SystemMetric]:
        return self.repository.get_history(limit)
