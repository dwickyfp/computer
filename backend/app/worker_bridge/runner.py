"""
Backend job runner used by the worker bridge.

This module is executed in a dedicated subprocess with PYTHONPATH pointing to
the backend package so worker tasks can run backend-owned business logic
without importing the backend package into the worker process itself.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import pkgutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

import httpx


JAKARTA_TZ = ZoneInfo("Asia/Jakarta")


def _import_backend_models() -> None:
    import app.domain.models as model_package

    for module_info in pkgutil.iter_modules(model_package.__path__):
        if module_info.name.startswith("_"):
            continue
        importlib.import_module(f"{model_package.__name__}.{module_info.name}")


def _bootstrap_backend_runtime() -> None:
    from app.core.database import db_manager
    from app.core.logging import setup_logging

    setup_logging()
    _import_backend_models()
    db_manager.initialize()


def _shutdown_backend_runtime() -> None:
    from app.core.database import db_manager
    from app.infrastructure.redis import RedisClient

    try:
        RedisClient.close()
    except Exception:
        pass

    try:
        db_manager.close()
    except Exception:
        pass


def _record_job_metric(key: str) -> None:
    from app.core.database import db_manager
    from app.domain.repositories.job_metric import JobMetricRepository

    db = db_manager.session_factory()
    try:
        repo = JobMetricRepository(db)
        repo.upsert_metric(key, datetime.now(JAKARTA_TZ))
        db.commit()
    finally:
        db.close()


def _run_wal_monitor(_: dict[str, Any]) -> dict[str, Any]:
    from app.domain.services.wal_monitor import WALMonitorService

    asyncio.run(WALMonitorService().monitor_all_sources())
    return {"message": "WAL monitoring cycle completed"}


def _run_replication_monitor(_: dict[str, Any]) -> dict[str, Any]:
    from app.domain.services.replication_monitor import ReplicationMonitorService

    asyncio.run(ReplicationMonitorService().monitor_all_sources())
    return {"message": "Replication monitoring cycle completed"}


def _run_schema_monitor(_: dict[str, Any]) -> dict[str, Any]:
    from app.domain.services.schema_monitor import SchemaMonitorService

    asyncio.run(SchemaMonitorService().monitor_all_sources())
    return {"message": "Schema monitoring cycle completed"}


def _run_credit_monitor(_: dict[str, Any]) -> dict[str, Any]:
    from app.domain.services.credit_monitor import CreditMonitorService

    CreditMonitorService().monitor_all_destinations()
    return {"message": "Credit monitoring cycle completed"}


def _run_source_table_list_refresh(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.source import SourceService

    session_factory = db_manager.session_factory
    db = session_factory()
    refreshed = 0
    failed = 0

    try:
        sources = SourceService(db).list_sources(limit=1000)
    finally:
        db.close()

    def _refresh_one(source_id: int) -> None:
        session = session_factory()
        try:
            SourceService(session).refresh_available_tables(source_id)
            session.commit()
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_refresh_one, source.id): source.id for source in sources}
        for future in as_completed(futures):
            try:
                future.result()
                refreshed += 1
            except Exception:
                failed += 1

    return {
        "message": "Source table refresh cycle completed",
        "sources": len(sources),
        "refreshed": refreshed,
        "failed": failed,
    }


def _run_destination_table_list_refresh(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.destination import DestinationService

    db = db_manager.session_factory()
    try:
        service = DestinationService(db)
        service.refresh_table_list_all()
        return {"message": "Destination table refresh cycle dispatched"}
    finally:
        db.close()


def _run_system_metric_collection(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.system_metric import SystemMetricService

    db = db_manager.session_factory()
    try:
        metric = SystemMetricService(db).collect_and_save_metrics()
        db.commit()
        return {
            "message": "System metrics collected",
            "metric_id": getattr(metric, "id", None),
        }
    finally:
        db.close()


def _run_notification_sender(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.notification_service import NotificationService

    db = db_manager.session_factory()
    try:
        processed = NotificationService(db).process_pending_notifications()
        return {
            "message": "Notification sender cycle completed",
            "processed": processed,
        }
    finally:
        db.close()


def _run_worker_health_check(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.config import get_settings
    from app.core.database import db_manager
    from app.domain.repositories.worker_health_repo import WorkerHealthRepository

    settings = get_settings()
    if not settings.worker_enabled:
        return {"message": "Worker health check skipped because worker is disabled"}

    db = db_manager.session_factory()
    try:
        repo = WorkerHealthRepository(db)
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(f"{settings.worker_health_url}/health")
                if response.status_code == 200:
                    payload = response.json()
                    repo.upsert_status(
                        healthy=payload.get("healthy", False),
                        active_workers=payload.get("active_workers", 0),
                        active_tasks=payload.get("active_tasks", 0),
                        reserved_tasks=payload.get("reserved_tasks", 0),
                        error_message=payload.get("error"),
                        extra_data=payload,
                    )
                    return {"message": "Worker health status updated", "healthy": payload.get("healthy", False)}
                repo.upsert_status(
                    healthy=False,
                    error_message=f"HTTP {response.status_code}",
                )
                return {"message": "Worker health status updated", "healthy": False}
        except Exception as exc:
            repo.upsert_status(healthy=False, error_message=str(exc))
            return {"message": "Worker health status updated", "healthy": False, "error": str(exc)}
    finally:
        db.close()


def _run_pipeline_refresh_check(_: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.models.pipeline import Pipeline, PipelineStatus

    db = db_manager.session_factory()
    refreshed = 0
    try:
        pipelines = db.query(Pipeline).filter(Pipeline.ready_refresh == True).all()
        now = datetime.now(timezone.utc)
        for pipeline in pipelines:
            pipeline.status = PipelineStatus.REFRESH.value
            pipeline.ready_refresh = False
            pipeline.last_refresh_at = now
            refreshed += 1
        if refreshed:
            db.commit()
        return {
            "message": "Pipeline refresh check completed",
            "refreshed": refreshed,
        }
    finally:
        db.close()


def _run_source_refresh_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.source import SourceService

    source_id = int(payload["source_id"])
    db = db_manager.session_factory()
    try:
        SourceService(db).refresh_source_metadata(source_id)
        return {"message": "Source refresh completed", "source_id": source_id}
    finally:
        db.close()


def _run_source_refresh_available_tables(payload: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.source import SourceService

    source_id = int(payload["source_id"])
    db = db_manager.session_factory()
    try:
        tables = SourceService(db).refresh_available_tables(source_id)
        return {
            "message": "Source available tables refreshed",
            "source_id": source_id,
            "total_tables": len(tables),
        }
    finally:
        db.close()


def _run_destination_refresh_credits(payload: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.models.destination import Destination
    from app.domain.services.credit_monitor import CreditMonitorService

    destination_id = int(payload["destination_id"])
    with db_manager.session() as session:
        destination = session.get(Destination, destination_id)
        if destination is None:
            raise ValueError(f"Destination {destination_id} not found")
        CreditMonitorService().refresh_credits_for_destination(session, destination)
    return {
        "message": "Destination credits refreshed",
        "destination_id": destination_id,
    }


def _run_pipeline_refresh(payload: dict[str, Any]) -> dict[str, Any]:
    from app.core.database import db_manager
    from app.domain.services.pipeline import PipelineService

    pipeline_id = int(payload["pipeline_id"])
    db = db_manager.session_factory()
    try:
        pipeline = PipelineService(db).refresh_pipeline(pipeline_id)
        return {
            "message": "Pipeline refresh triggered",
            "pipeline_id": pipeline.id,
            "status": pipeline.status,
        }
    finally:
        db.close()


JOB_HANDLERS: dict[str, tuple[Callable[[dict[str, Any]], dict[str, Any]], str | None]] = {
    "wal_monitor": (_run_wal_monitor, "wal_monitor"),
    "replication_monitor": (_run_replication_monitor, "replication_monitor"),
    "schema_monitor": (_run_schema_monitor, "schema_monitor"),
    "credit_monitor": (_run_credit_monitor, "credit_monitor"),
    "table_list_refresh": (_run_source_table_list_refresh, "table_list_refresh"),
    "destination_table_list_refresh": (
        _run_destination_table_list_refresh,
        "destination_table_list_refresh",
    ),
    "system_metric_collection": (
        _run_system_metric_collection,
        "system_metric_collection",
    ),
    "notification_sender": (_run_notification_sender, "notification_sender"),
    "worker_health_check": (_run_worker_health_check, "worker_health_check"),
    "pipeline_refresh_check": (_run_pipeline_refresh_check, "pipeline_refresh_check"),
    "source.refresh_metadata": (_run_source_refresh_metadata, None),
    "source.refresh_available_tables": (_run_source_refresh_available_tables, None),
    "destination.refresh_credits": (_run_destination_refresh_credits, None),
    "pipeline.refresh": (_run_pipeline_refresh, None),
}


def run_job(job_name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    handler_entry = JOB_HANDLERS.get(job_name)
    if handler_entry is None:
        raise ValueError(f"Unsupported backend bridge job: {job_name}")

    _bootstrap_backend_runtime()

    handler, metric_key = handler_entry
    result = handler(payload or {})
    if metric_key:
        _record_job_metric(metric_key)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a backend bridge job")
    parser.add_argument("job_name")
    parser.add_argument("--payload", default="{}")
    args = parser.parse_args()

    try:
        payload = json.loads(args.payload)
        result = run_job(args.job_name, payload)
        print(json.dumps(result, default=str))
    finally:
        _shutdown_backend_runtime()


if __name__ == "__main__":
    main()
