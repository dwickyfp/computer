"""
Flow Task service — orchestrates all flow task business logic.

Handles CRUD, graph persistence, run triggering via Celery,
node preview, and run history retrieval.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.models.flow_task import (
    FlowTask,
    FlowTaskGraph,
    FlowTaskRunHistory,
    FlowTaskRunNodeLog,
    FlowTaskRunStatus,
    FlowTaskStatus,
    FlowTaskTriggerType,
)
from app.domain.repositories.flow_task import (
    FlowTaskGraphRepository,
    FlowTaskRepository,
    FlowTaskRunHistoryRepository,
    FlowTaskRunNodeLogRepository,
    FlowTaskGraphVersionRepository,
    FlowTaskWatermarkRepository,
)
from app.domain.repositories.notification_log_repo import NotificationLogRepository
from app.domain.schemas.flow_task import (
    FlowTaskCreate,
    FlowTaskGraphSave,
    FlowTaskUpdate,
    NodePreviewRequest,
)
from app.domain.schemas.notification_log import NotificationLogCreate

logger = get_logger(__name__)


# ─── Graph diff utilities ─────────────────────────────────────────────────────


def _strip_position(node: dict) -> dict:
    """Return a copy of *node* without the ``position`` key."""
    return {k: v for k, v in node.items() if k != "position"}


def _compute_graph_diff(
    old_nodes: list,
    old_edges: list,
    new_nodes: list,
    new_edges: list,
) -> Optional[str]:
    """Compare two graph snapshots ignoring node positions.

    Returns:
        ``None``  – no meaningful change (position-only or identical).
        A human-readable summary string when structural/data changes exist.
    """
    # Normalize nodes (strip positions) and sort by id for stable comparison
    old_norm = sorted(
        [_strip_position(n) for n in old_nodes], key=lambda n: n.get("id", "")
    )
    new_norm = sorted(
        [_strip_position(n) for n in new_nodes], key=lambda n: n.get("id", "")
    )

    # Normalize edges and sort by id
    old_edges_sorted = sorted(old_edges, key=lambda e: e.get("id", ""))
    new_edges_sorted = sorted(new_edges, key=lambda e: e.get("id", ""))

    if old_norm == new_norm and old_edges_sorted == new_edges_sorted:
        return None  # identical (or position-only change)

    # Build change summary ---------------------------------------------------
    changes: list[str] = []

    old_ids = {n["id"] for n in old_nodes}
    new_ids = {n["id"] for n in new_nodes}

    # Added nodes
    for nid in sorted(new_ids - old_ids):
        node = next(n for n in new_nodes if n["id"] == nid)
        label = node.get("data", {}).get("label") or node.get("label", nid)
        changes.append(f"Added {node.get('type', 'node')} '{label}'")

    # Removed nodes
    for nid in sorted(old_ids - new_ids):
        node = next(n for n in old_nodes if n["id"] == nid)
        label = node.get("data", {}).get("label") or node.get("label", nid)
        changes.append(f"Removed {node.get('type', 'node')} '{label}'")

    # Modified nodes (data/type changed, not position)
    old_map = {n["id"]: _strip_position(n) for n in old_nodes}
    new_map = {n["id"]: _strip_position(n) for n in new_nodes}
    for nid in sorted(old_ids & new_ids):
        if old_map[nid] != new_map[nid]:
            node = next(n for n in new_nodes if n["id"] == nid)
            label = node.get("data", {}).get("label") or node.get("label", nid)
            changes.append(f"Updated {node.get('type', 'node')} '{label}'")

    # Edge changes
    old_edge_ids = {e["id"] for e in old_edges}
    new_edge_ids = {e["id"] for e in new_edges}
    added_edges = len(new_edge_ids - old_edge_ids)
    removed_edges = len(old_edge_ids - new_edge_ids)
    if added_edges:
        changes.append(f"Added {added_edges} connection(s)")
    if removed_edges:
        changes.append(f"Removed {removed_edges} connection(s)")

    # Edge data modifications (same id but different content)
    old_edge_map = {e["id"]: e for e in old_edges}
    new_edge_map = {e["id"]: e for e in new_edges}
    modified_edges = sum(
        1
        for eid in old_edge_ids & new_edge_ids
        if old_edge_map[eid] != new_edge_map[eid]
    )
    if modified_edges:
        changes.append(f"Modified {modified_edges} connection(s)")

    return "; ".join(changes) if changes else "Graph updated"


class FlowTaskService:
    """
    Business logic for Flow Task management.

    Coordinates between repositories, Celery worker client, and
    enforces domain rules.
    """

    def __init__(self, db: Session):
        self.db = db
        self.flow_task_repo = FlowTaskRepository(db)
        self.graph_repo = FlowTaskGraphRepository(db)
        self.run_history_repo = FlowTaskRunHistoryRepository(db)
        self.node_log_repo = FlowTaskRunNodeLogRepository(db)
        self.notification_repo = NotificationLogRepository(db)
        self.version_repo = FlowTaskGraphVersionRepository(db)
        self.watermark_repo = FlowTaskWatermarkRepository(db)

    # ─── Notification helper ───────────────────────────────────────────────

    def _push_notification(
        self,
        key: str,
        title: str,
        message: str,
        notif_type: str = "ERROR",
    ) -> None:
        """Upsert a notification into notification_log.

        Uses upsert_notification_by_key which increments iteration_check
        on repeated same-key entries, triggering dispatch once it reaches
        NOTIFICATION_ITERATION_DEFAULT (default 3).
        Swallows exceptions so notification failures never break the caller.
        """
        try:
            self.notification_repo.upsert_notification_by_key(
                NotificationLogCreate(
                    key_notification=key,
                    title=title,
                    message=message[:2000],
                    type=notif_type,
                    is_read=False,
                    is_deleted=False,
                    iteration_check=1,
                    is_sent=False,
                )
            )
        except Exception as exc:
            logger.warning(
                f"Failed to push notification key={key}: {exc}"
            )

    # ─── CRUD ─────────────────────────────────────────────────────────────────

    def create_flow_task(self, data: FlowTaskCreate) -> FlowTask:
        """Create a new flow task."""
        flow_task = self.flow_task_repo.create(
            name=data.name,
            description=data.description,
            status=FlowTaskStatus.IDLE,
            trigger_type=data.trigger_type,
        )
        self.db.commit()
        self.db.refresh(flow_task)
        logger.info(f"FlowTask created: id={flow_task.id} name={flow_task.name}")
        return flow_task

    def get_flow_task(self, flow_task_id: int) -> FlowTask:
        """Get a flow task by ID. Raises EntityNotFoundError if missing."""
        task = self.flow_task_repo.get_by_id(flow_task_id)
        if not task:
            raise EntityNotFoundError(f"FlowTask {flow_task_id} not found")
        return task

    def list_flow_tasks(
        self, skip: int = 0, limit: int = 20
    ) -> Tuple[List[FlowTask], int]:
        """Return paginated list of all flow tasks."""
        return self.flow_task_repo.get_all_paginated(skip=skip, limit=limit)

    def update_flow_task(self, flow_task_id: int, data: FlowTaskUpdate) -> FlowTask:
        """Update flow task metadata."""
        # Ensure exists
        self.get_flow_task(flow_task_id)
        update_kwargs = data.dict(exclude_unset=True, exclude_none=True)
        if not update_kwargs:
            return self.get_flow_task(flow_task_id)
        task = self.flow_task_repo.update(flow_task_id, **update_kwargs)
        self.db.commit()
        self.db.refresh(task)
        return task

    def delete_flow_task(self, flow_task_id: int) -> None:
        """Delete a flow task and all its children (cascade)."""
        task = self.get_flow_task(flow_task_id)
        # Prevent deletion while running
        if task.status == FlowTaskStatus.RUNNING:
            raise ValueError(
                f"Cannot delete flow task {flow_task_id} while it is running"
            )
        self.flow_task_repo.delete(flow_task_id)
        self.db.commit()
        logger.info(f"FlowTask deleted: id={flow_task_id}")

    def duplicate_flow_task(self, flow_task_id: int) -> FlowTask:
        """
        Duplicate a flow task and its graph.
        
        Creates a new task with name "{original_name} - copy".
        Copies the graph structure (nodes, edges) if it exists.
        Resulting task is IDLE and has no history.
        """
        original = self.get_flow_task(flow_task_id)
        
        # 1. Create new task metadata
        new_name = c = f"{original.name} - copy"
        # If name is too long, truncate? For now typical usage won't hit limits quickly.
        
        new_task = self.flow_task_repo.create(
            name=new_name,
            description=original.description,
            status=FlowTaskStatus.IDLE,
            trigger_type=original.trigger_type,
        )
        self.db.flush()
        
        # 2. Copy graph if exists
        original_graph = self.graph_repo.get_by_flow_task_id(flow_task_id)
        if original_graph:
            self.graph_repo.upsert_graph(
                flow_task_id=new_task.id,
                nodes_json=original_graph.nodes_json,
                edges_json=original_graph.edges_json,
            )
        
        self.db.commit()
        self.db.refresh(new_task)
        logger.info(f"FlowTask duplicated: src={flow_task_id} dst={new_task.id}")
        return new_task

    # ─── Graph ─────────────────────────────────────────────────────────────────

    def save_graph(self, flow_task_id: int, data: FlowTaskGraphSave) -> FlowTaskGraph:
        """Upsert the node/edge graph for a flow task."""
        # Ensure parent exists
        self.get_flow_task(flow_task_id)

        nodes_json = [
            {
                "id": n.id,
                "type": n.type,
                "position": {"x": n.position.x, "y": n.position.y},
                "data": n.data,
                **({"label": n.label} if n.label else {}),
            }
            for n in data.nodes
        ]
        edges_json = [
            {
                "id": e.id,
                "source": e.source,
                "target": e.target,
                **({"sourceHandle": e.source_handle} if e.source_handle else {}),
                **({"targetHandle": e.target_handle} if e.target_handle else {}),
            }
            for e in data.edges
        ]

        graph = self.graph_repo.upsert_graph(
            flow_task_id=flow_task_id,
            nodes_json=nodes_json,
            edges_json=edges_json,
        )

        # D4: Auto-create version snapshot only on meaningful changes
        # Compare with the latest version; skip if only positions changed.
        change_summary = None
        if hasattr(data, "change_summary") and data.change_summary:
            change_summary = data.change_summary

        should_snapshot = True
        try:
            latest_ver = self.version_repo.get_latest_version_number(flow_task_id)
            if latest_ver > 0 and change_summary is None:
                prev = self.version_repo.get_by_version(flow_task_id, latest_ver)
                if prev:
                    diff_summary = _compute_graph_diff(
                        prev.nodes_json or [],
                        prev.edges_json or [],
                        nodes_json,
                        edges_json,
                    )
                    if diff_summary is None:
                        # Position-only change — skip version snapshot
                        should_snapshot = False
                    else:
                        change_summary = diff_summary
        except Exception as e:
            logger.warning(f"Graph diff comparison failed, will snapshot anyway: {e}")

        if should_snapshot:
            try:
                self.version_repo.create_snapshot(
                    flow_task_id=flow_task_id,
                    nodes_json=nodes_json,
                    edges_json=edges_json,
                    change_summary=change_summary,
                )
            except Exception as e:
                logger.warning(f"Failed to create graph version snapshot: {e}")

        self.db.commit()
        self.db.refresh(graph)
        logger.info(
            f"Graph saved: flow_task_id={flow_task_id} version={graph.version} "
            f"nodes={len(nodes_json)} edges={len(edges_json)}"
        )
        return graph

    def get_graph(self, flow_task_id: int) -> Optional[FlowTaskGraph]:
        """Get the saved graph for a flow task (None if not yet saved)."""
        self.get_flow_task(flow_task_id)
        return self.graph_repo.get_by_flow_task_id(flow_task_id)

    # ─── Run Triggering ────────────────────────────────────────────────────────

    def trigger_run(
        self,
        flow_task_id: int,
        trigger_type: str = FlowTaskTriggerType.MANUAL,
    ) -> Dict[str, Any]:
        """
        Trigger a flow task execution.

        1. Load graph; raise if no graph saved
        2. Set flow_task.status = RUNNING
        3. Create FlowTaskRunHistory record
        4. Dispatch Celery task via WorkerClient
        5. Update run record with celery_task_id
        Returns {run_id, celery_task_id, status}
        """
        from app.infrastructure.worker_client import get_worker_client

        task = self.get_flow_task(flow_task_id)
        graph = self.graph_repo.get_by_flow_task_id(flow_task_id)
        if not graph:
            raise ValueError(
                f"FlowTask {flow_task_id} has no saved graph. "
                "Please build and save the flow before running."
            )

        # Check not already running
        if task.status == FlowTaskStatus.RUNNING:
            raise ValueError(
                f"FlowTask {flow_task_id} is already running"
            )

        # Mark flow as RUNNING
        self.flow_task_repo.update(flow_task_id, status=FlowTaskStatus.RUNNING)

        # Create run history record
        run = self.run_history_repo.create(
            flow_task_id=flow_task_id,
            trigger_type=trigger_type,
            status=FlowTaskRunStatus.RUNNING,
            started_at=datetime.now(ZoneInfo("Asia/Jakarta")),
            run_metadata={
                "graph_version": graph.version,
                "node_count": len(graph.nodes_json),
                "edge_count": len(graph.edges_json),
            },
        )
        self.db.flush()

        # Dispatch to Celery
        try:
            worker_client = get_worker_client()
            graph_json = {
                "nodes": graph.nodes_json,
                "edges": graph.edges_json,
            }
            celery_task_id = worker_client.submit_flow_task_execute(
                flow_task_id=flow_task_id,
                run_history_id=run.id,
                graph_json=graph_json,
            )
            # Update run with celery task id
            self.run_history_repo.update(run.id, celery_task_id=celery_task_id)
            self.db.commit()
        except Exception as e:
            # Rollback run to FAILED if dispatch fails
            self.run_history_repo.complete_run(
                run_id=run.id,
                status=FlowTaskRunStatus.FAILED,
                finished_at=datetime.now(ZoneInfo("Asia/Jakarta")),
                error_message=f"Failed to dispatch task: {e}",
            )
            self.flow_task_repo.update(
                flow_task_id,
                status=FlowTaskStatus.FAILED,
                last_run_at=datetime.now(ZoneInfo("Asia/Jakarta")),
                last_run_status=FlowTaskRunStatus.FAILED,
            )
            self.db.commit()
            # Notify on dispatch failure
            self._push_notification(
                key=f"flow_task_dispatch_error_{flow_task_id}",
                title=f"Flow Task {flow_task_id} Failed to Start",
                message=f"Flow Task ‘{task.name}’ (ID: {flow_task_id}) could not be dispatched to the worker. Error: {e}",
            )
            raise ConnectionError(f"Worker task dispatch failed: {e}") from e

        logger.info(
            f"FlowTask run triggered: flow_task_id={flow_task_id} "
            f"run_id={run.id} celery_task_id={celery_task_id}"
        )
        return {
            "run_id": run.id,
            "celery_task_id": celery_task_id,
            "status": "RUNNING",
            "message": "Flow task execution started",
        }

    # ─── Cancel Run ───────────────────────────────────────────────────────────

    def cancel_run(self, flow_task_id: int) -> Dict[str, Any]:
        """
        Cancel the currently running flow task execution.

        Revokes the Celery task, marks run history as CANCELLED,
        and resets flow_task.status back to FAILED.
        """
        from app.infrastructure.worker_client import get_worker_client

        task = self.get_flow_task(flow_task_id)
        if task.status != FlowTaskStatus.RUNNING:
            raise ValueError(
                f"FlowTask {flow_task_id} is not currently running (status={task.status})"
            )

        # Find the latest RUNNING run record to get the celery_task_id
        run = self.run_history_repo.get_latest_running(flow_task_id)

        revoked = False
        if run and run.celery_task_id:
            try:
                worker_client = get_worker_client()
                revoked = worker_client.cancel_task(run.celery_task_id)
            except Exception as e:
                logger.warning(f"Could not revoke Celery task: {e}")

        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        # Mark run history as CANCELLED
        if run:
            self.run_history_repo.complete_run(
                run_id=run.id,
                status=FlowTaskRunStatus.CANCELLED,
                finished_at=now,
                error_message="Cancelled by user",
            )

        # Reset flow_task status
        self.flow_task_repo.update(
            flow_task_id,
            status=FlowTaskStatus.FAILED,
            last_run_at=now,
            last_run_status=FlowTaskRunStatus.CANCELLED,
        )
        self.db.commit()

        logger.info(
            f"FlowTask run cancelled: flow_task_id={flow_task_id} revoked={revoked}"
        )
        return {
            "flow_task_id": flow_task_id,
            "revoked": revoked,
            "status": "CANCELLED",
            "message": "Run cancelled",
        }

    # ─── Node Preview ──────────────────────────────────────────────────────────

    def preview_node(
        self,
        flow_task_id: int,
        request: NodePreviewRequest,
    ) -> Dict[str, Any]:
        """
        Submit a node preview task to the Celery worker.

        Accepts an unsaved graph snapshot so preview works before saving.
        Returns {task_id} for polling.
        """
        from app.infrastructure.worker_client import get_worker_client

        # Ensure flow task exists
        self.get_flow_task(flow_task_id)

        graph_snapshot = {
            "nodes": [
                {
                    "id": n.id,
                    "type": n.type,
                    "position": {"x": n.position.x, "y": n.position.y},
                    "data": n.data,
                }
                for n in request.nodes
            ],
            "edges": [
                {
                    "id": e.id,
                    "source": e.source,
                    "target": e.target,
                    **({"sourceHandle": e.source_handle} if e.source_handle else {}),
                    **({"targetHandle": e.target_handle} if e.target_handle else {}),
                }
                for e in request.edges
            ],
        }

        try:
            worker_client = get_worker_client()
            task_id = worker_client.submit_flow_task_preview(
                flow_task_id=flow_task_id,
                node_id=request.node_id,
                graph_snapshot=graph_snapshot,
                limit=request.limit,
            )
        except Exception as e:
            raise ConnectionError(f"Worker preview dispatch failed: {e}") from e

        return {
            "task_id": task_id,
            "status": "PENDING",
            "message": "Preview task submitted",
        }

    # ─── Run History ───────────────────────────────────────────────────────────

    def get_run_history(
        self,
        flow_task_id: int,
        skip: int = 0,
        limit: int = 100,
    ) -> Tuple[List[FlowTaskRunHistory], int]:
        """Return paginated run history for a flow task."""
        self.get_flow_task(flow_task_id)
        return self.run_history_repo.get_by_flow_task_paginated(
            flow_task_id=flow_task_id, skip=skip, limit=limit
        )

    def get_run_detail(self, run_id: int) -> FlowTaskRunHistory:
        """Get a single run history record with node logs."""
        run = self.run_history_repo.get_by_id(run_id)
        if not run:
            raise EntityNotFoundError(f"RunHistory {run_id} not found")
        return run

    def get_task_status(self, celery_task_id: str) -> Dict[str, Any]:
        """
        Poll the Celery worker for task status.

        Also updates the run history record if the task has completed.
        """
        from app.infrastructure.worker_client import get_worker_client

        worker_client = get_worker_client()
        status = worker_client.get_task_status(celery_task_id)

        # Sync terminal states back to DB
        if status["state"] in ("SUCCESS", "FAILURE") and status["state"] != "UNKNOWN":
            run = self.run_history_repo.get_by_celery_task_id(celery_task_id)
            if run and run.status == FlowTaskRunStatus.RUNNING:
                is_success = status["state"] == "SUCCESS"
                result_data = status.get("result") or {}
                self.run_history_repo.complete_run(
                    run_id=run.id,
                    status=FlowTaskRunStatus.SUCCESS if is_success else FlowTaskRunStatus.FAILED,
                    finished_at=datetime.now(ZoneInfo("Asia/Jakarta")),
                    total_input_records=result_data.get("total_input_records", 0),
                    total_output_records=result_data.get("total_output_records", 0),
                    error_message=status.get("error") if not is_success else None,
                )
                self.flow_task_repo.update_run_summary(
                    flow_task_id=run.flow_task_id,
                    status=FlowTaskStatus.SUCCESS if is_success else FlowTaskStatus.FAILED,
                    last_run_at=datetime.now(ZoneInfo("Asia/Jakarta")),
                    last_run_status=FlowTaskRunStatus.SUCCESS if is_success else FlowTaskRunStatus.FAILED,
                    last_run_record_count=result_data.get("total_output_records"),
                )
                self.db.commit()
                # Notify on run failure
                if not is_success:
                    task = self.flow_task_repo.get_by_id(run.flow_task_id)
                    task_name = task.name if task else str(run.flow_task_id)
                    error_msg = status.get("error") or "Unknown error"
                    self._push_notification(
                        key=f"flow_task_run_failed_{run.flow_task_id}",
                        title=f"Flow Task ‘{task_name}’ Run Failed",
                        message=(
                            f"Flow Task ‘{task_name}’ (ID: {run.flow_task_id}) finished with status FAILED. "
                            f"Run ID: {run.id}. Error: {error_msg}"
                        ),
                    )

        return status

    # ─── D4: Graph Versioning ─────────────────────────────────────────────

    def list_graph_versions(
        self, flow_task_id: int, skip: int = 0, limit: int = 20
    ) -> Tuple[list, int]:
        """List version history for a flow task graph."""
        self.get_flow_task(flow_task_id)
        return self.version_repo.get_versions_by_flow_task(
            flow_task_id=flow_task_id, skip=skip, limit=limit
        )

    def get_graph_version(self, flow_task_id: int, version: int):
        """Get a specific graph version snapshot."""
        self.get_flow_task(flow_task_id)
        v = self.version_repo.get_by_version(flow_task_id, version)
        if not v:
            raise EntityNotFoundError(
                f"Version {version} not found for flow task {flow_task_id}"
            )
        return v

    def rollback_graph(self, flow_task_id: int, version: int) -> FlowTaskGraph:
        """
        Rollback graph to a previous version.

        Restores the nodes/edges from the snapshot and creates a new version
        entry recording the rollback.
        """
        snapshot = self.get_graph_version(flow_task_id, version)

        # Upsert graph with the old snapshot data
        graph = self.graph_repo.upsert_graph(
            flow_task_id=flow_task_id,
            nodes_json=snapshot.nodes_json,
            edges_json=snapshot.edges_json,
        )

        # Create a new version entry for the rollback
        self.version_repo.create_snapshot(
            flow_task_id=flow_task_id,
            nodes_json=snapshot.nodes_json,
            edges_json=snapshot.edges_json,
            change_summary=f"Rollback to version {version}",
        )

        self.db.commit()
        self.db.refresh(graph)
        logger.info(
            f"Graph rolled back: flow_task_id={flow_task_id} "
            f"to_version={version} new_version={graph.version}"
        )
        return graph

    # ─── D8: Watermark Management ─────────────────────────────────────────

    def get_watermarks(self, flow_task_id: int) -> list:
        """Get all watermarks for a flow task."""
        self.get_flow_task(flow_task_id)
        return self.watermark_repo.get_by_flow_task(flow_task_id)

    def set_watermark(
        self,
        flow_task_id: int,
        node_id: str,
        watermark_column: str,
        watermark_type: str = "TIMESTAMP",
    ):
        """Configure a watermark for an input node."""
        self.get_flow_task(flow_task_id)
        wm = self.watermark_repo.upsert_watermark(
            flow_task_id=flow_task_id,
            node_id=node_id,
            watermark_column=watermark_column,
            last_watermark_value="",
            watermark_type=watermark_type,
        )
        self.db.commit()
        self.db.refresh(wm)
        return wm

    def reset_watermark(self, flow_task_id: int, node_id: str) -> None:
        """Reset watermark for an input node."""
        wm = self.watermark_repo.get_by_flow_task_and_node(flow_task_id, node_id)
        if wm:
            self.watermark_repo.delete(wm.id)
            self.db.commit()
