import psutil
from typing import List, Optional
from sqlalchemy.orm import Session
from app.core.logging import get_logger
from app.domain.repositories.system_metric import SystemMetricRepository
from app.domain.schemas.system_metric import SystemMetricCreate
from app.domain.models.system_metric import SystemMetric

logger = get_logger(__name__)


class SystemMetricService:
    def __init__(self, db: Session):
        self.repository = SystemMetricRepository(db)

    def collect_and_save_metrics(self) -> SystemMetric:
        # Get system metrics using psutil
        cpu_usage = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()
        total_swap = 0
        used_swap = 0
        try:
            swap = psutil.swap_memory()
            total_swap = swap.total
            used_swap = swap.used
        except (OSError, NotImplementedError) as exc:
            logger.warning(
                "Swap metrics unavailable, defaulting to zero",
                error=str(exc),
            )

        metric_data = SystemMetricCreate(
            cpu_usage=cpu_usage,
            total_memory=memory.total,
            used_memory=memory.used,
            total_swap=total_swap,
            used_swap=used_swap,
        )
        
        return self.repository.create(metric_data)

    def get_latest_metrics(self) -> Optional[SystemMetric]:
        return self.repository.get_latest()

    def get_metrics_history(self, limit: int = 100) -> List[SystemMetric]:
        return self.repository.get_history(limit)
