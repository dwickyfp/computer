"""
Flow Task repositories.

Provides data access for FlowTask, FlowTaskGraph, FlowTaskRunHistory,
FlowTaskRunNodeLog, FlowTaskGraphVersion, and FlowTaskWatermark models.
"""

from datetime import datetime
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.flow_task import (
    FlowTask,
    FlowTaskGraph,
    FlowTaskRunHistory,
    FlowTaskRunNodeLog,
)
from app.domain.models.flow_task_graph_version import FlowTaskGraphVersion
from app.domain.models.flow_task_watermark import FlowTaskWatermark
from app.domain.repositories.base import BaseRepository

logger = get_logger(__name__)


class FlowTaskRepository(BaseRepository[FlowTask]):
    """Repository for FlowTask CRUD and custom queries."""

    def __init__(self, db: Session):
        super().__init__(FlowTask, db)

    def get_all_paginated(
        self,
        skip: int = 0,
        limit: int = 20,
    ) -> tuple[List[FlowTask], int]:
        """Return paginated flow tasks with total count."""
        stmt = select(FlowTask).order_by(desc(FlowTask.updated_at))
        total_stmt = select(func.count()).select_from(FlowTask)

        total = self.db.execute(total_stmt).scalar_one()
        items = list(self.db.execute(stmt.offset(skip).limit(limit)).scalars().all())
        return items, total

    def get_by_status(self, status: str) -> List[FlowTask]:
        """Fetch all flow tasks with a given status."""
        stmt = select(FlowTask).where(FlowTask.status == status)
        return list(self.db.execute(stmt).scalars().all())

    def update_run_summary(
        self,
        flow_task_id: int,
        status: str,
        last_run_at: datetime,
        last_run_status: str,
        last_run_record_count: Optional[int],
    ) -> Optional[FlowTask]:
        """Update last run summary fields after a run completes."""
        return self.update(
            flow_task_id,
            status=status,
            last_run_at=last_run_at,
            last_run_status=last_run_status,
            last_run_record_count=last_run_record_count,
        )


class FlowTaskGraphRepository(BaseRepository[FlowTaskGraph]):
    """Repository for FlowTaskGraph with upsert support."""

    def __init__(self, db: Session):
        super().__init__(FlowTaskGraph, db)

    def get_by_flow_task_id(self, flow_task_id: int) -> Optional[FlowTaskGraph]:
        """Get the graph for a flow task."""
        stmt = select(FlowTaskGraph).where(
            FlowTaskGraph.flow_task_id == flow_task_id
        )
        return self.db.execute(stmt).scalars().first()

    def upsert_graph(
        self,
        flow_task_id: int,
        nodes_json: list,
        edges_json: list,
    ) -> FlowTaskGraph:
        """
        Insert or update the graph for a flow task.

        If a graph already exists for the flow_task_id, update it and
        increment version. Otherwise create a new record.
        """
        existing = self.get_by_flow_task_id(flow_task_id)
        if existing:
            existing.nodes_json = nodes_json
            existing.edges_json = edges_json
            existing.version = (existing.version or 1) + 1
            existing.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
            self.db.flush()
            return existing
        else:
            graph = FlowTaskGraph(
                flow_task_id=flow_task_id,
                nodes_json=nodes_json,
                edges_json=edges_json,
                version=1,
            )
            self.db.add(graph)
            self.db.flush()
            return graph


class FlowTaskRunHistoryRepository(BaseRepository[FlowTaskRunHistory]):
    """Repository for FlowTaskRunHistory."""

    def __init__(self, db: Session):
        super().__init__(FlowTaskRunHistory, db)

    def get_by_flow_task_paginated(
        self,
        flow_task_id: int,
        skip: int = 0,
        limit: int = 20,
    ) -> tuple[List[FlowTaskRunHistory], int]:
        """Return paginated run history for a flow task."""
        stmt = (
            select(FlowTaskRunHistory)
            .where(FlowTaskRunHistory.flow_task_id == flow_task_id)
            .order_by(desc(FlowTaskRunHistory.started_at))
        )
        total_stmt = (
            select(func.count())
            .select_from(FlowTaskRunHistory)
            .where(FlowTaskRunHistory.flow_task_id == flow_task_id)
        )
        total = self.db.execute(total_stmt).scalar_one()
        items = list(self.db.execute(stmt.offset(skip).limit(limit)).scalars().all())
        return items, total

    def get_by_celery_task_id(self, celery_task_id: str) -> Optional[FlowTaskRunHistory]:
        """Find a run history record by Celery task ID."""
        stmt = select(FlowTaskRunHistory).where(
            FlowTaskRunHistory.celery_task_id == celery_task_id
        )
        return self.db.execute(stmt).scalars().first()

    def get_latest_running(self, flow_task_id: int) -> Optional[FlowTaskRunHistory]:
        """Get the most recent RUNNING run record for a flow task."""
        stmt = (
            select(FlowTaskRunHistory)
            .where(
                FlowTaskRunHistory.flow_task_id == flow_task_id,
                FlowTaskRunHistory.status == "RUNNING",
            )
            .order_by(desc(FlowTaskRunHistory.started_at))
            .limit(1)
        )
        return self.db.execute(stmt).scalars().first()

    def complete_run(
        self,
        run_id: int,
        status: str,
        finished_at: datetime,
        total_input_records: int = 0,
        total_output_records: int = 0,
        error_message: Optional[str] = None,
    ) -> Optional[FlowTaskRunHistory]:
        """Mark a run as completed (success or failure)."""
        return self.update(
            run_id,
            status=status,
            finished_at=finished_at,
            total_input_records=total_input_records,
            total_output_records=total_output_records,
            error_message=error_message,
        )


class FlowTaskRunNodeLogRepository(BaseRepository[FlowTaskRunNodeLog]):
    """Repository for FlowTaskRunNodeLog."""

    def __init__(self, db: Session):
        super().__init__(FlowTaskRunNodeLog, db)

    def get_by_run_history_id(self, run_history_id: int) -> List[FlowTaskRunNodeLog]:
        """Get all node logs for a run."""
        stmt = (
            select(FlowTaskRunNodeLog)
            .where(FlowTaskRunNodeLog.run_history_id == run_history_id)
            .order_by(FlowTaskRunNodeLog.id)
        )
        return list(self.db.execute(stmt).scalars().all())

    def bulk_create_for_run(
        self,
        run_history_id: int,
        flow_task_id: int,
        node_logs: List[dict],
    ) -> List[FlowTaskRunNodeLog]:
        """Bulk-insert node logs for a completed run."""
        records = []
        for log in node_logs:
            record = FlowTaskRunNodeLog(
                run_history_id=run_history_id,
                flow_task_id=flow_task_id,
                node_id=log.get("node_id", ""),
                node_type=log.get("node_type", ""),
                node_label=log.get("node_label"),
                row_count_in=log.get("row_count_in", 0),
                row_count_out=log.get("row_count_out", 0),
                duration_ms=log.get("duration_ms"),
                status=log.get("status", "SUCCESS"),
                error_message=log.get("error_message"),
            )
            self.db.add(record)
            records.append(record)
        self.db.flush()
        return records


class FlowTaskGraphVersionRepository(BaseRepository[FlowTaskGraphVersion]):
    """Repository for FlowTaskGraphVersion (D4 versioning)."""

    def __init__(self, db: Session):
        super().__init__(FlowTaskGraphVersion, db)

    def get_versions_by_flow_task(
        self, flow_task_id: int, skip: int = 0, limit: int = 20
    ) -> tuple[List[FlowTaskGraphVersion], int]:
        """Return paginated version history for a flow task."""
        stmt = (
            select(FlowTaskGraphVersion)
            .where(FlowTaskGraphVersion.flow_task_id == flow_task_id)
            .order_by(desc(FlowTaskGraphVersion.version))
        )
        total = self.db.execute(
            select(func.count())
            .select_from(FlowTaskGraphVersion)
            .where(FlowTaskGraphVersion.flow_task_id == flow_task_id)
        ).scalar_one()
        items = list(
            self.db.execute(stmt.offset(skip).limit(limit)).scalars().all()
        )
        return items, total

    def get_latest_version_number(self, flow_task_id: int) -> int:
        """Get the latest version number for a flow task (0 if none)."""
        stmt = (
            select(func.coalesce(func.max(FlowTaskGraphVersion.version), 0))
            .where(FlowTaskGraphVersion.flow_task_id == flow_task_id)
        )
        return self.db.execute(stmt).scalar_one()

    def get_by_version(
        self, flow_task_id: int, version: int
    ) -> Optional[FlowTaskGraphVersion]:
        """Get a specific version snapshot."""
        stmt = select(FlowTaskGraphVersion).where(
            FlowTaskGraphVersion.flow_task_id == flow_task_id,
            FlowTaskGraphVersion.version == version,
        )
        return self.db.execute(stmt).scalars().first()

    def create_snapshot(
        self,
        flow_task_id: int,
        nodes_json: list,
        edges_json: list,
        change_summary: str = None,
    ) -> FlowTaskGraphVersion:
        """Create a new version snapshot."""
        next_version = self.get_latest_version_number(flow_task_id) + 1
        return self.create(
            flow_task_id=flow_task_id,
            version=next_version,
            nodes_json=nodes_json,
            edges_json=edges_json,
            change_summary=change_summary,
        )


class FlowTaskWatermarkRepository(BaseRepository[FlowTaskWatermark]):
    """Repository for FlowTaskWatermark (D8 incremental execution)."""

    def __init__(self, db: Session):
        super().__init__(FlowTaskWatermark, db)

    def get_by_flow_task_and_node(
        self, flow_task_id: int, node_id: str
    ) -> Optional[FlowTaskWatermark]:
        """Get watermark for a specific node in a flow task."""
        stmt = select(FlowTaskWatermark).where(
            FlowTaskWatermark.flow_task_id == flow_task_id,
            FlowTaskWatermark.node_id == node_id,
        )
        return self.db.execute(stmt).scalars().first()

    def get_by_flow_task(self, flow_task_id: int) -> List[FlowTaskWatermark]:
        """Get all watermarks for a flow task."""
        stmt = select(FlowTaskWatermark).where(
            FlowTaskWatermark.flow_task_id == flow_task_id
        )
        return list(self.db.execute(stmt).scalars().all())

    def upsert_watermark(
        self,
        flow_task_id: int,
        node_id: str,
        watermark_column: str,
        last_watermark_value: str,
        watermark_type: str = "TIMESTAMP",
        record_count: int = 0,
    ) -> FlowTaskWatermark:
        """Insert or update a watermark entry."""
        existing = self.get_by_flow_task_and_node(flow_task_id, node_id)
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        if existing:
            existing.watermark_column = watermark_column
            existing.last_watermark_value = last_watermark_value
            existing.watermark_type = watermark_type
            existing.last_run_at = now
            existing.record_count = record_count
            existing.updated_at = now
            self.db.flush()
            return existing
        else:
            wm = FlowTaskWatermark(
                flow_task_id=flow_task_id,
                node_id=node_id,
                watermark_column=watermark_column,
                last_watermark_value=last_watermark_value,
                watermark_type=watermark_type,
                last_run_at=now,
                record_count=record_count,
            )
            self.db.add(wm)
            self.db.flush()
            return wm
