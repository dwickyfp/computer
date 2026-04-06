"""
Credit usage endpoints.

Provides API to access and refresh Snowflake credit usage monitoring.
"""

from fastapi import APIRouter, Depends, status, HTTPException

from app.core.config import get_settings
from app.domain.schemas.credit import CreditUsageResponse
from app.domain.schemas.common import TaskDispatchResponse
from app.domain.services.credit_monitor import CreditMonitorService
from app.core.database import db_manager
from app.domain.models.destination import Destination

router = APIRouter()


def get_credit_monitor_service() -> CreditMonitorService:
    """Dependency provider for CreditMonitorService."""
    return CreditMonitorService()


@router.get(
    "/{destination_id}/credits",
    response_model=CreditUsageResponse,
    summary="Get credit usage",
    description="Get Snowflake credit usage statistics for a destination",
)
def get_destination_credits(
    destination_id: int,
    service: CreditMonitorService = Depends(get_credit_monitor_service),
) -> CreditUsageResponse:
    """
    Get credit usage for a destination.
    """
    result = service.get_credit_usage(destination_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Destination not found or no data available"
        )
    return result


@router.post(
    "/{destination_id}/credits/refresh",
    response_model=TaskDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Refresh credit usage",
    description="Trigger immediate update of Snowflake credit usage data",
)
def refresh_destination_credits(
    destination_id: int,
    service: CreditMonitorService = Depends(get_credit_monitor_service),
) -> TaskDispatchResponse:
    """
    Force refresh of credit usage data.
    """
    try:
        with db_manager.session() as session:
            destination = session.get(Destination, destination_id)
            if not destination:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Destination not found"
                )

        settings = get_settings()
        if settings.worker_enabled:
            try:
                from app.infrastructure.worker_client import get_worker_client

                task_id = get_worker_client().submit_backend_job(
                    "destination.refresh_credits",
                    {"destination_id": destination_id},
                )
                return TaskDispatchResponse(
                    message="Credit refresh dispatched",
                    task_id=task_id,
                )
            except ConnectionError as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Worker service unavailable. Please ensure the worker is running.",
                ) from exc

        with db_manager.session() as session:
            destination = session.get(Destination, destination_id)
            if not destination:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Destination not found"
                )
            service.refresh_credits_for_destination(session, destination)

        return TaskDispatchResponse(
            message="Credit data refreshed successfully",
            task_id=None,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to refresh credits: {str(e)}"
        )
