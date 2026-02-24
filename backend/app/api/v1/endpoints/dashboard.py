from typing import Any, Dict, List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.domain.services.dashboard import DashboardService

router = APIRouter()

@router.get("/summary", response_model=Dict[str, Any])
def get_dashboard_summary(
    db: Session = Depends(get_db),
):
    """
    Get dashboard summary metrics.
    """
    service = DashboardService(db)
    
    pipeline_status = service.get_pipeline_status_summary()
    flow_stats = service.get_global_data_flow_stats(days=7) # Get short term for summary cards if needed, or just today/yesterday
    credit_stats = service.get_total_credit_usage(days=30)
    backfill_stats = service.get_backfill_summary()

    return {
        "pipelines": pipeline_status,
        "data_flow": {
            "today": flow_stats["total_today"],
            "yesterday": flow_stats["total_yesterday"]
        },
        "credits": {
            "month_total": credit_stats["current_month_total"]
        },
        "backfills": backfill_stats
    }

@router.get("/flow-chart", response_model=Dict[str, Any])
def get_flow_chart_data(
    days: int = 7,
    db: Session = Depends(get_db),
):
    """
    Get data flow chart data.
    """
    service = DashboardService(db)
    return service.get_global_data_flow_stats(days=days)

@router.get("/credit-chart", response_model=Dict[str, Any])
def get_credit_chart_data(
    days: int = 30,
    db: Session = Depends(get_db),
):
    """
    Get credit usage chart data.
    """
    service = DashboardService(db)
    return service.get_total_credit_usage(days=days)

@router.get("/health/sources", response_model=Dict[str, int])
def get_source_health_summary(
    db: Session = Depends(get_db),
):
    """
    Get source health summary.
    """
    service = DashboardService(db)
    return service.get_source_health_summary()

@router.get("/charts/replication-lag", response_model=Dict[str, Any])
def get_replication_lag_chart(
    days: int = 1,
    db: Session = Depends(get_db),
):
    """
    Get replication lag chart data.
    """
    service = DashboardService(db)
    return service.get_replication_lag_history(days=days)

@router.get("/charts/top-tables", response_model=List[Dict[str, Any]])
def get_top_tables_chart(
    limit: int = 5,
    db: Session = Depends(get_db),
):
    """
    Get top tables by volume.
    """
    service = DashboardService(db)
    return service.get_top_tables_by_volume(limit=limit)

@router.get("/activity-feed", response_model=List[Dict[str, Any]])
def get_activity_feed(
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """
    Get recent activity feed.
    """
    service = DashboardService(db)
    return service.get_recent_activities(limit=limit)
