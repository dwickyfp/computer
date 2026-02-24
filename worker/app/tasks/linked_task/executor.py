"""
Linked Task Celery executor.

Executes a linked task DAG by:
1. Topologically sorting steps.
2. Launching parallel groups using threads.
3. For each step, submitting the corresponding flow task to Celery and polling.
4. Respecting ON_SUCCESS / ALWAYS edge conditions.

NOTE: Uses raw SQL (sqlalchemy.text) — no ORM domain models — so this module
can be imported cleanly by the Celery worker without a backend/domain package.
"""

from __future__ import annotations

import concurrent.futures
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import text

from app.core.database import get_db_session
from app.config.settings import get_settings

logger = structlog.get_logger(__name__)

TZ = ZoneInfo("Asia/Jakarta")
MAX_POLL_TIMEOUT = 3600    # 1 hour max wait per step

# ─── Deadlock prevention ──────────────────────────────────────────────────────
# Linked task orchestration threads call celery_result.get() which blocks the
# Celery thread slot while waiting for child flow tasks.  If all thread slots
# are consumed by waiting parents, child tasks can never start → deadlock.
#
# Mitigation applied:
# 1. Linked tasks are routed to the 'orchestration' queue.
# 2. start.sh passes '-Q preview,default,orchestration' so children on
#    'default' always have capacity.
# 3. worker_concurrency must be >= duckdb_max_concurrent +
#    linked_task_max_parallel_steps + headroom (see config/settings.py).

# Edge condition constants (mirrors LinkedTaskEdgeCondition enum)
CONDITION_ON_SUCCESS = "ON_SUCCESS"
CONDITION_ALWAYS = "ALWAYS"

# Status constants
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"


def _now() -> datetime:
    return datetime.now(TZ)


# ─── DB helpers (raw SQL) ──────────────────────────────────────────────────────

def _get_linked_task(db, linked_task_id: int) -> dict | None:
    row = db.execute(
        text("SELECT id, name, status FROM linked_tasks WHERE id = :id"),
        {"id": linked_task_id},
    ).fetchone()
    return dict(row._mapping) if row else None


def _get_steps(db, linked_task_id: int) -> list[dict]:
    rows = db.execute(
        text(
            "SELECT id, linked_task_id, flow_task_id "
            "FROM linked_task_steps WHERE linked_task_id = :lt_id"
        ),
        {"lt_id": linked_task_id},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _get_edges(db, linked_task_id: int) -> list[dict]:
    rows = db.execute(
        text(
            "SELECT id, linked_task_id, source_step_id, target_step_id, condition "
            "FROM linked_task_edges WHERE linked_task_id = :lt_id"
        ),
        {"lt_id": linked_task_id},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _create_step_log(db, run_history_id: int, linked_task_id: int, step_id: int, flow_task_id: int) -> int:
    row = db.execute(
        text(
            "INSERT INTO linked_task_run_step_log "
            "(run_history_id, linked_task_id, step_id, flow_task_id, status, created_at, updated_at) "
            "VALUES (:rh, :lt, :step, :ft, :status, :now, :now) RETURNING id"
        ),
        {
            "rh": run_history_id,
            "lt": linked_task_id,
            "step": step_id,
            "ft": flow_task_id,
            "status": STATUS_PENDING,
            "now": _now(),
        },
    ).fetchone()
    db.commit()
    return row.id


def _update_step_log(db, step_log_id: int, status: str, error: str | None = None,
                     flow_task_run_history_id: int | None = None,
                     celery_task_id: str | None = None):
    now = _now()
    updates = {"status": status, "updated_at": now, "id": step_log_id}
    set_parts = ["status = :status", "updated_at = :updated_at"]

    if status == STATUS_RUNNING:
        set_parts.append("started_at = :now")
        updates["now"] = now
    if status in (STATUS_SUCCESS, STATUS_FAILED, STATUS_SKIPPED):
        set_parts.append("finished_at = :now")
        updates["now"] = now
    if error:
        set_parts.append("error_message = :error")
        updates["error"] = error
    if flow_task_run_history_id is not None:
        set_parts.append("flow_task_run_history_id = :ft_run_id")
        updates["ft_run_id"] = flow_task_run_history_id
    if celery_task_id is not None:
        set_parts.append("celery_task_id = :celery_task_id")
        updates["celery_task_id"] = celery_task_id

    db.execute(
        text(f"UPDATE linked_task_run_step_log SET {', '.join(set_parts)} WHERE id = :id"),
        updates,
    )
    db.commit()


def _create_flow_task_run(db, flow_task_id: int) -> int:
    """Create a flow_task_run_history row and return its id."""
    now = _now()
    row = db.execute(
        text(
            "INSERT INTO flow_task_run_history "
            "(flow_task_id, status, created_at, updated_at) "
            "VALUES (:ft, 'RUNNING', :now, :now) RETURNING id"
        ),
        {"ft": flow_task_id, "now": now},
    ).fetchone()
    db.commit()
    return row.id


def _get_flow_task_graph(db, flow_task_id: int) -> dict:
    row = db.execute(
        text(
            "SELECT nodes_json, edges_json FROM flow_task_graph "
            "WHERE flow_task_id = :ft_id LIMIT 1"
        ),
        {"ft_id": flow_task_id},
    ).fetchone()
    if row:
        return {"nodes": row.nodes_json or [], "edges": row.edges_json or []}
    return {"nodes": [], "edges": []}


# ─── Single step execution ─────────────────────────────────────────────────────

def _execute_single_step(run_history_id: int, step_log_id: int, flow_task_id: int) -> str:
    """
    Execute a single flow task step by submitting it to Celery and polling.

    Returns the final status string: SUCCESS, FAILED.
    """
    from app.celery_app import celery_app
    from app.tasks.flow_task.task import execute_flow_task_task

    # Create a flow_task_run_history sub-run and get the graph
    with get_db_session() as db:
        graph_json = _get_flow_task_graph(db, flow_task_id)
        sub_run_id = _create_flow_task_run(db, flow_task_id)
        _update_step_log(
            db, step_log_id,
            status=STATUS_RUNNING,
            flow_task_run_history_id=sub_run_id,
        )

    # Dispatch Celery task
    celery_result = execute_flow_task_task.apply_async(
        kwargs={
            "flow_task_id": flow_task_id,
            "run_history_id": sub_run_id,
            "graph_json": graph_json,
        },
        queue="default",
    )

    # Store celery task id on the step log
    with get_db_session() as db:
        _update_step_log(db, step_log_id, status=STATUS_RUNNING, celery_task_id=celery_result.id)

    # Wait for the Celery task result (polls Redis backend internally,
    # much more efficient than polling PostgreSQL)
    try:
        result = celery_result.get(
            timeout=MAX_POLL_TIMEOUT,
            propagate=False,
        )
        if isinstance(result, Exception):
            logger.error(
                "Flow task raised exception",
                step_log_id=step_log_id,
                error=str(result),
            )
            return STATUS_FAILED
        if isinstance(result, dict):
            return result.get("status", STATUS_FAILED)
        return STATUS_FAILED
    except Exception as e:
        logger.error(
            "Waiting for flow task result failed",
            step_log_id=step_log_id,
            error=str(e),
        )
        return STATUS_FAILED


# ─── Main DAG executor ─────────────────────────────────────────────────────────

def execute_linked_task(linked_task_id: int, run_history_id: int) -> dict:
    """
    Main entry point: execute the full linked task DAG.

    Returns result dict consumed by the Celery task.
    """
    log = logger.bind(linked_task_id=linked_task_id, run_history_id=run_history_id)
    log.info("linked_task execution started")

    # Load graph data
    with get_db_session() as db:
        linked_task = _get_linked_task(db, linked_task_id)
        if not linked_task:
            raise ValueError(f"LinkedTask {linked_task_id} not found")

        steps = _get_steps(db, linked_task_id)
        edges = _get_edges(db, linked_task_id)

        # Build adjacency data
        step_map = {s["id"]: s for s in steps}
        successors: dict[int, list[tuple[int, str]]] = {s["id"]: [] for s in steps}
        predecessors: dict[int, list[int]] = {s["id"]: [] for s in steps}

        for edge in edges:
            successors[edge["source_step_id"]].append(
                (edge["target_step_id"], edge["condition"])
            )
            predecessors[edge["target_step_id"]].append(edge["source_step_id"])

        # Create step logs (PENDING) upfront
        step_log_map: dict[int, int] = {}  # step_id → step_log_id
        for step in steps:
            sl_id = _create_step_log(db, run_history_id, linked_task_id, step["id"], step["flow_task_id"])
            step_log_map[step["id"]] = sl_id

    # Topological layer execution (BFS)
    in_degree = {s["id"]: len(predecessors[s["id"]]) for s in steps}
    queue = [s["id"] for s in steps if in_degree[s["id"]] == 0]
    step_result: dict[int, str] = {}
    overall_status = STATUS_SUCCESS

    while queue:
        max_parallel = get_settings().linked_task_max_parallel_steps
        log.info("executing layer", step_ids=queue, max_parallel=max_parallel)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(queue), max_parallel)) as pool:
            future_to_step = {
                pool.submit(
                    _execute_single_step,
                    run_history_id,
                    step_log_map[step_id],
                    step_map[step_id]["flow_task_id"],
                ): step_id
                for step_id in queue
                if step_id not in step_result  # skip already-SKIPPED steps
            }

            for future in concurrent.futures.as_completed(future_to_step):
                step_id = future_to_step[future]
                try:
                    result_status = future.result()
                except Exception as exc:
                    result_status = STATUS_FAILED
                    log.error("step raised exception", step_id=step_id, exc=str(exc))

                step_result[step_id] = result_status

                with get_db_session() as db:
                    _update_step_log(db, step_log_map[step_id], status=result_status)

                if result_status == STATUS_FAILED:
                    overall_status = STATUS_FAILED

        # Determine next layer
        next_queue: list[int] = []
        for step_id in queue:
            for target_id, condition in successors.get(step_id, []):
                in_degree[target_id] -= 1
                source_status = step_result.get(step_id, STATUS_FAILED)

                if condition == CONDITION_ON_SUCCESS and source_status != STATUS_SUCCESS:
                    step_result[target_id] = STATUS_SKIPPED
                    with get_db_session() as db:
                        _update_step_log(db, step_log_map[target_id], status=STATUS_SKIPPED)

                if in_degree[target_id] == 0 and target_id not in step_result:
                    next_queue.append(target_id)

        queue = next_queue

    # Finalize linked task run
    now = _now()
    with get_db_session() as db:
        db.execute(
            text(
                "UPDATE linked_task_run_history "
                "SET status = :status, finished_at = :now, updated_at = :now "
                "WHERE id = :id"
            ),
            {"status": overall_status, "now": now, "id": run_history_id},
        )
        db.execute(
            text(
                "UPDATE linked_tasks "
                "SET status = 'IDLE', last_run_at = :now, last_run_status = :status, updated_at = :now "
                "WHERE id = :id"
            ),
            {"status": overall_status, "now": now, "id": linked_task_id},
        )
        db.commit()

    log.info("linked_task execution finished", status=overall_status)
    return {"status": overall_status, "step_results": step_result}
