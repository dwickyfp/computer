from sqlalchemy import Column, Float, BigInteger, DateTime, func
from app.domain.models.base import Base

class SystemMetric(Base):
    __tablename__ = "system_metrics"

    id = Column(BigInteger, primary_key=True, index=True)
    cpu_usage = Column(Float, nullable=True)
    total_memory = Column(BigInteger, nullable=True)
    used_memory = Column(BigInteger, nullable=True)
    total_swap = Column(BigInteger, nullable=True)
    used_swap = Column(BigInteger, nullable=True)
    recorded_at = Column(DateTime(timezone=True), server_default=func.now())

    @property
    def memory_usage_percent(self):
        if self.total_memory and self.total_memory > 0:
            return (self.used_memory / self.total_memory) * 100
        return 0.0

    @property
    def swap_usage_percent(self):
        if self.total_swap and self.total_swap > 0:
            return (self.used_swap / self.total_swap) * 100
        return 0.0
