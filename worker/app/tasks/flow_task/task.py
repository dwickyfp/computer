"""
Flow Task Celery tasks.

Two tasks:
  - worker.flow_task.execute  — full graph execution (queue: default)
  - worker.flow_task.preview  — single-node preview (queue: preview)
"""

from typing import Any, Optional

import structlog

from app.celery_app import celery_app
from app.tasks.base import BaseTask
from app.tasks.flow_task.executor import execute_flow_task
from app.tasks.flow_task.preview_executor import execute_node_preview

logger = structlog.get_logger(__name__)


@celery_app.task(
    base=BaseTask,
    name="worker.flow_task.execute",
    bind=True,
    max_retries=0,          # No retries — state already committed to DB
    reject_on_worker_lost=False,  # Don't re-queue (would cause duplicate writes)
    queue="default",
    acks_late=True,
)
def execute_flow_task_task(
    self,
    flow_task_id: int,
    run_history_id: int,
    graph_json: dict,
) -> dict[str, Any]:
    """
    Execute a full flow task graph.

    Args:
        flow_task_id: PK of the FlowTask record.
        run_history_id: PK of the FlowTaskRunHistory record (already created).
        graph_json: Serialized {nodes: [...], edges: [...]} graph.

    Returns:
        Result dict with status, total_input_records, total_output_records,
        elapsed_ms, node_logs.
    """
    logger.info(
        "Flow task execution started",
        task_id=self.request.id,
        flow_task_id=flow_task_id,
        run_history_id=run_history_id,
    )

    self.update_state(
        state="PROGRESS",
        meta={
            "status": "executing",
            "flow_task_id": flow_task_id,
            "run_history_id": run_history_id,
        },
    )

    return execute_flow_task(
        flow_task_id=flow_task_id,
        run_history_id=run_history_id,
        graph_json=graph_json,
    )


@celery_app.task(
    base=BaseTask,
    name="worker.flow_task.preview",
    bind=True,
    max_retries=1,
    default_retry_delay=3,
    queue="preview",
    acks_late=True,
)
def preview_flow_task_node_task(
    self,
    flow_task_id: int,
    node_id: str,
    graph_snapshot: dict,
    limit: int = 500,
) -> dict[str, Any]:
    """
    Preview a single node's output within the given graph snapshot.

    Args:
        flow_task_id: PK of the FlowTask record (for logging context).
        node_id: ID of the node to preview up to.
        graph_snapshot: Unsaved {nodes, edges} graph snapshot from the editor.
        limit: Maximum rows to return (default 500).

    Returns:
        Dict with columns, column_types, rows, row_count, elapsed_ms.
    """
    logger.info(
        "Flow task node preview started",
        task_id=self.request.id,
        flow_task_id=flow_task_id,
        node_id=node_id,
        limit=limit,
    )

    self.update_state(
        state="PROGRESS",
        meta={
            "status": "previewing",
            "flow_task_id": flow_task_id,
            "node_id": node_id,
        },
    )

    return execute_node_preview(
        flow_task_id=flow_task_id,
        node_id=node_id,
        graph_snapshot=graph_snapshot,
        limit=limit,
    )
