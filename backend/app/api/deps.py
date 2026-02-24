"""
API dependencies for dependency injection.

Provides common dependencies used across API endpoints.
"""

from typing import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.database import get_db_session, get_db_session_readonly
from app.domain.services.destination import DestinationService
from app.domain.services.pipeline import PipelineService
from app.domain.services.source import SourceService
from app.domain.services.backfill import BackfillService
from app.domain.services.tag import TagService


def get_db() -> Generator[Session, None, None]:
    """
    Get database session dependency (read-write).

    Yields database session for use in endpoint functions.
    """
    yield from get_db_session()


def get_db_readonly() -> Generator[Session, None, None]:
    """
    Get read-only database session dependency.

    Skips COMMIT on success — use for GET endpoints that only read data.
    Saves ~0.1-0.5ms per request by avoiding unnecessary COMMIT round-trip.
    """
    yield from get_db_session_readonly()


def get_source_service(db: Session = Depends(get_db)) -> SourceService:
    """
    Get source service dependency (read-write).

    Args:
        db: Database session

    Returns:
        Source service instance
    """
    return SourceService(db)


def get_source_service_readonly(
    db: Session = Depends(get_db_readonly),
) -> SourceService:
    """Get source service dependency (read-only, no COMMIT)."""
    return SourceService(db)


def get_destination_service(db: Session = Depends(get_db)) -> DestinationService:
    """
    Get destination service dependency (read-write).

    Args:
        db: Database session

    Returns:
        Destination service instance
    """
    return DestinationService(db)


def get_destination_service_readonly(
    db: Session = Depends(get_db_readonly),
) -> DestinationService:
    """Get destination service dependency (read-only, no COMMIT)."""
    return DestinationService(db)


def get_pipeline_service(db: Session = Depends(get_db)) -> PipelineService:
    """
    Get pipeline service dependency (read-write).

    Args:
        db: Database session

    Returns:
        Pipeline service instance
    """
    return PipelineService(db)


def get_pipeline_service_readonly(
    db: Session = Depends(get_db_readonly),
) -> PipelineService:
    """Get pipeline service dependency (read-only, no COMMIT)."""
    return PipelineService(db)


def get_preset_service(db: Session = Depends(get_db)) -> Generator:
    from app.domain.services.preset import PresetService

    return PresetService(db)


def get_backfill_service(db: Session = Depends(get_db)) -> BackfillService:
    """
    Get backfill service dependency.

    Args:
        db: Database session

    Returns:
        Backfill service instance
    """
    return BackfillService(db)


def get_tag_service(db: Session = Depends(get_db)) -> TagService:
    """
    Get tag service dependency.

    Args:
        db: Database session

    Returns:
        Tag service instance
    """
    return TagService(db)


def get_flow_task_service(db: Session = Depends(get_db)) -> "FlowTaskService":
    """
    Get flow task service dependency.

    Args:
        db: Database session

    Returns:
        FlowTask service instance
    """
    from app.domain.services.flow_task import FlowTaskService

    return FlowTaskService(db)


def get_linked_task_service(db: Session = Depends(get_db)) -> "LinkedTaskService":
    """Get linked task service dependency."""
    from app.domain.services.linked_task import LinkedTaskService

    return LinkedTaskService(db)


def get_schedule_service(db: Session = Depends(get_db)) -> "ScheduleService":
    """Get schedule service dependency."""
    from app.domain.services.schedule import ScheduleService

    return ScheduleService(db)


