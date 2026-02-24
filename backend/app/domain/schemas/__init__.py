"""
Domain schemas initialization.

Exports all Pydantic validation schemas.
"""

from app.domain.schemas.common import HealthResponse, PaginatedResponse
from app.domain.schemas.destination import (
    DestinationCreate,
    DestinationResponse,
    DestinationUpdate,
)
from app.domain.schemas.pipeline import (
    PipelineCreate,
    PipelineMetadataResponse,
    PipelineResponse,
    PipelineStatusUpdate,
    PipelineUpdate,
)
from app.domain.schemas.source import (
    SourceCreate,
    SourceResponse,
    SourceUpdate,
)
from app.domain.schemas.wal_metric import (
    WALMetricResponse,
    WALMetricsQuery,
)
from app.domain.schemas.wal_monitor import (
    WALMonitorCreate,
    WALMonitorUpdate,
    WALMonitorResponse,
    WALMonitorStatusUpdate,
    WALMonitorListResponse,
)

__all__ = [
    # Common
    "HealthResponse",
    "PaginatedResponse",
    # Source
    "SourceCreate",
    "SourceUpdate",
    "SourceResponse",
    # Destination
    "DestinationCreate",
    "DestinationUpdate",
    "DestinationResponse",
    # Pipeline
    "PipelineCreate",
    "PipelineUpdate",
    "PipelineResponse",
    "PipelineStatusUpdate",
    "PipelineMetadataResponse",
    # WAL Metric
    "WALMetricResponse",
    "WALMetricsQuery",
    # WAL Monitor
    "WALMonitorCreate",
    "WALMonitorUpdate",
    "WALMonitorResponse",
    "WALMonitorStatusUpdate",
    "WALMonitorListResponse",
]
