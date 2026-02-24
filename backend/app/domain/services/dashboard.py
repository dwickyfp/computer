"""
Dashboard Service.

Aggregates data for the main dashboard view.
"""

from typing import Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, func, desc, case, literal_column
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.core.database import db_manager
from app.core.logging import get_logger
from app.domain.models.pipeline import Pipeline, PipelineStatus
from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
from app.domain.models.credit_snowflake_monitoring import CreditSnowflakeMonitoring
from app.domain.models.wal_monitor import WALMonitor
from app.domain.models.wal_metric import WALMetric
from app.domain.models.source import Source
from app.domain.models.pipeline import PipelineDestination, Pipeline, PipelineStatus, PipelineMetadata
from app.domain.repositories.backfill import BackfillRepository

logger = get_logger(__name__)

class DashboardService:
    """
    Service for aggregating dashboard data.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_pipeline_status_summary(self) -> Dict[str, int]:
        """
        Get count of pipelines by status.
        """
        # Group by status and count
        results = self.db.execute(
            select(
                Pipeline.status,
                func.count(Pipeline.id)
            ).group_by(Pipeline.status)
        ).all()
        
        # Initialize with defaults (ensure all statuses are present if 0)
        summary = {
            "START": 0,
            "PAUSE": 0,
            # Add other known statuses if any, or mapped from generic status
        }
        
        total = 0
        for status, count in results:
            summary[status] = count
            total += count
            
        summary["total"] = total
        summary["total"] = total
        return summary

    def get_backfill_summary(self) -> Dict[str, int]:
        """
        Get global backfill summary.
        """
        repo = BackfillRepository(self.db)
        return repo.get_global_status_summary()

    def get_global_data_flow_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        Get aggregated data flow stats, grouped by Pipeline.
        Returns:
            - total_today: int
            - total_yesterday: int
            - history: List[Dict] -> [{"date": "2023-01-01", "Pipeline A": 100, "Pipeline B": 20}]
            - pipelines: List[str] -> ["Pipeline A", "Pipeline B"]
        """
        today = datetime.now(ZoneInfo('Asia/Jakarta')).date()
        yesterday = today - timedelta(days=1)
        start_date = today - timedelta(days=days)
        
        # 1. Totals (Global)
        total_today = self.db.execute(
            select(func.sum(DataFlowRecordMonitoring.record_count))
            .where(func.date(DataFlowRecordMonitoring.created_at) == today)
        ).scalar() or 0
        
        total_yesterday = self.db.execute(
            select(func.sum(DataFlowRecordMonitoring.record_count))
            .where(func.date(DataFlowRecordMonitoring.created_at) == yesterday)
        ).scalar() or 0
        
        # 2. History Grouped by Pipeline
        # We need Pipeline Name. Join with Pipeline table.
        history_results = self.db.execute(
            select(
                func.date(DataFlowRecordMonitoring.created_at).label("day"),
                Pipeline.name,
                func.sum(DataFlowRecordMonitoring.record_count)
            )
            .join(Pipeline, Pipeline.id == DataFlowRecordMonitoring.pipeline_id)
            .where(DataFlowRecordMonitoring.created_at >= start_date)
            .group_by(func.date(DataFlowRecordMonitoring.created_at), Pipeline.name)
            .order_by("day")
        ).all()
        
        # Pivot data for Recharts
        # Structure: {"2023-01-01": {"date": "2023-01-01", "Pipeline A": 100}}
        pivot_data = {}
        pipeline_names = set()

        # Initialize all dates first like before? 
        # Better to fill gaps later or just let frontend handle it.
        # Let's initialize structure with 0s for known pipelines if possible, 
        # but we don't know all pipelines efficiently without another query.
        # Let's just pivot what we have and fill gaps.
        
        for row in history_results:
            d_str = row[0].isoformat()
            p_name = row[1]
            count = row[2]
            pipeline_names.add(p_name)
            
            if d_str not in pivot_data:
                pivot_data[d_str] = {"date": d_str}
            
            pivot_data[d_str][p_name] = count

        # Fill missing dates
        filled_history = []
        for i in range(days + 1):
            d = (start_date + timedelta(days=i))
            d_str = d.isoformat()
            
            entry = pivot_data.get(d_str, {"date": d_str})
            # Ensure all pipelines exist in entry with 0 if missing (good for stacked charts)
            for p_name in pipeline_names:
                if p_name not in entry:
                    entry[p_name] = 0
            
            filled_history.append(entry)
            
        return {
            "total_today": total_today,
            "total_yesterday": total_yesterday,
            "history": filled_history,
            "pipelines": list(pipeline_names)
        }

    def get_total_credit_usage(self, days: int = 30) -> Dict[str, Any]:
        """
        Get aggregated credit usage, grouped by Destination.
        Returns:
            - current_month_total: float
            - history: List[Dict] -> [{"date": "...", "Dest A": 1.5, "Dest B": 0.5}]
            - destinations: List[str]
        """
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_date = now - timedelta(days=days)
        
        # 1. Total (Global Month)
        month_total = self.db.execute(
            select(func.sum(CreditSnowflakeMonitoring.total_credit))
            .where(CreditSnowflakeMonitoring.usage_date >= start_of_month)
        ).scalar() or 0.0
        
        # 2. History Grouped by Destination
        # Need Destination name. Join.
        from app.domain.models.destination import Destination
        
        history_results = self.db.execute(
            select(
                CreditSnowflakeMonitoring.usage_date,
                Destination.name,
                func.sum(CreditSnowflakeMonitoring.total_credit)
            )
            .join(Destination, Destination.id == CreditSnowflakeMonitoring.destination_id)
            .where(CreditSnowflakeMonitoring.usage_date >= start_date)
            .group_by(CreditSnowflakeMonitoring.usage_date, Destination.name)
            .order_by(CreditSnowflakeMonitoring.usage_date)
        ).all()
        
        pivot_data = {}
        dest_names = set()
        
        for row in history_results:
            d_str = row[0].date().isoformat() # usage_date is datetime, want YYYY-MM-DD
            d_name = row[1]
            credits = float(row[2])
            dest_names.add(d_name)
            
            if d_str not in pivot_data:
                pivot_data[d_str] = {"date": d_str}
            
            pivot_data[d_str][d_name] = credits

        # Fill missing dates
        filled_history = []
        for i in range(days + 1):
            d = (start_date + timedelta(days=i))
            d_str = d.date().isoformat()
            
            entry = pivot_data.get(d_str, {"date": d_str})
            for name in dest_names:
                if name not in entry:
                    entry[name] = 0.0
            
            filled_history.append(entry)

        return {
            "current_month_total": month_total,
            "history": filled_history,
            "destinations": list(dest_names)
        }

    def get_source_health_summary(self) -> Dict[str, int]:
        """
        Get count of sources by WAL Monitor status.
        """
        results = self.db.execute(
            select(
                WALMonitor.status,
                func.count(WALMonitor.id)
            ).group_by(WALMonitor.status)
        ).all()
        
        summary = {
            "ACTIVE": 0,
            "IDLE": 0,
            "ERROR": 0,
            "total": 0
        }
        
        total = 0
        for status, count in results:
            # Normalize status to uppercase just in case
            s = status.upper() if status else "UNKNOWN"
            if s in summary:
                summary[s] = count
            else:
                summary[s] = count # dynamic statuses
            total += count
            
        summary["total"] = total
        return summary

    def get_replication_lag_history(self, days: int = 1) -> Dict[str, Any]:
        """
        Get aggregated replication lag (size_bytes), grouped by Source.
        """
        now = datetime.now(ZoneInfo('Asia/Jakarta'))
        start_date = now - timedelta(days=days)
        
        # Join WALMetric with Source to get names
        history_results = self.db.execute(
            select(
                func.date_trunc('hour', WALMetric.recorded_at).label("time_bucket"), # aggregate by hour to reduce noise
                Source.name,
                func.avg(WALMetric.size_bytes)
            )
            .join(Source, Source.id == WALMetric.source_id)
            .where(WALMetric.recorded_at >= start_date)
            .group_by(func.date_trunc('hour', WALMetric.recorded_at), Source.name)
            .order_by("time_bucket")
        ).all()
        
        pivot_data = {}
        source_names = set()
        
        for row in history_results:
            t_str = row[0].isoformat()
            s_name = row[1]
            lag_bytes = float(row[2])
            source_names.add(s_name)
            
            if t_str not in pivot_data:
                pivot_data[t_str] = {"date": t_str}
            
            pivot_data[t_str][s_name] = lag_bytes
            
        return {
            "history": list(pivot_data.values()),
            "sources": list(source_names)
        }

    def get_top_tables_by_volume(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get top tables by record count today.
        """
        today = datetime.now(ZoneInfo('Asia/Jakarta')).date()
        
        results = self.db.execute(
            select(
                DataFlowRecordMonitoring.table_name,
                func.sum(DataFlowRecordMonitoring.record_count).label("total_records")
            )
            .where(func.date(DataFlowRecordMonitoring.created_at) == today)
            .group_by(DataFlowRecordMonitoring.table_name)
            .order_by(desc("total_records"))
            .limit(limit)
        ).all()
        
        return [
            {"table_name": row[0], "record_count": row[1]}
            for row in results
        ]

    def get_recent_activities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get combined list of recent errors and status changes.
        """
        # 1. Recent Pipeline Destination Errors
        error_query = (
            select(
                PipelineDestination.last_error_at.label("timestamp"),
                PipelineDestination.error_message.label("message"),
                Pipeline.name.label("source"),
                literal_column("'ERROR'").label("type")
            )
            .join(Pipeline, Pipeline.id == PipelineDestination.pipeline_id)
            .where(PipelineDestination.is_error == True)
            .order_by(desc(PipelineDestination.last_error_at))
            .limit(limit)
        )
        
        # 2. Recent Status Changes (from metadata)
        # Note: pipeline_metadata tracks current state, maybe not history. 
        # Using updated_at as proxy for "last change"
        status_query = (
            select(
                PipelineMetadata.updated_at.label("timestamp"),
                PipelineMetadata.status.label("message"), # e.g. "RUNNING"
                Pipeline.name.label("source"),
                literal_column("'STATUS'").label("type")
            )
            .join(Pipeline, Pipeline.id == PipelineMetadata.pipeline_id)
            .order_by(desc(PipelineMetadata.updated_at))
            .limit(limit)
        )
        
        # Combine manually or union if needed. Manual is easier for diverse sources.
        errors = self.db.execute(error_query).all()
        statuses = self.db.execute(status_query).all()
        
        combined = []
        for row in errors:
            combined.append({
                "timestamp": row[0],
                "message": row[1],
                "source": row[2],
                "type": row[3]
            })
            
        for row in statuses:
             combined.append({
                "timestamp": row[0],
                "message": f"Status changed to {row[1]}",
                "source": row[2],
                "type": row[3]
            })
            
        # Sort and limit
        combined.sort(key=lambda x: x["timestamp"] or datetime.min, reverse=True)
        return combined[:limit]
