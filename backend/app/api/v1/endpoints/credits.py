"""
Credit usage endpoints.

Provides API to access and refresh Snowflake credit usage monitoring.
"""

from fastapi import APIRouter, Depends, status, HTTPException

from app.domain.schemas.credit import CreditUsageResponse
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
    status_code=status.HTTP_200_OK,
    summary="Refresh credit usage",
    description="Trigger immediate update of Snowflake credit usage data",
)
def refresh_destination_credits(
    destination_id: int,
    service: CreditMonitorService = Depends(get_credit_monitor_service),
) -> dict:
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
            service.refresh_credits_for_destination(session, destination)
            
        return {"message": "Credit data refreshed successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to refresh credits: {str(e)}"
        )
