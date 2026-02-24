"""
Flow Task preview executor — runs a partial graph up to a target node and
returns the first N rows (default 500) as a JSON-serializable dict.

Used by the NodePreviewRequest endpoint for live "peek at data during
flow building" without saving the graph.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Dict, List, Optional

import duckdb
import structlog

from app.tasks.flow_task.compiler import GraphCompiler
from app.tasks.flow_task.connection_factory import SourceConnectionFactory
from app.tasks.flow_task.executor import (
    _setup_duckdb_connection,
    _inject_attach_configs,
    _load_optional_extension,
)

logger = structlog.get_logger(__name__)


# ─── Upstream graph trimming ───────────────────────────────────────────────────

def _get_upstream_subgraph(
    target_node_id: str,
    nodes: List[dict],
    edges: List[dict],
) -> tuple[List[dict], List[dict]]:
    """
    Return only the nodes and edges that are ancestors of (or equal to)
    `target_node_id` in the data-flow graph.

    This prevents compilation errors from unrelated or downstream nodes
    (e.g. a Join with no join keys) from breaking a preview of an upstream
    Input node.

    Algorithm: reverse-BFS from `target_node_id` along **incoming** edges.
    """
    # Build backwards adjacency: node_id -> list of source node_ids (parents)
    parents: Dict[str, List[str]] = {n["id"]: [] for n in nodes}
    for edge in edges:
        tgt = edge.get("target", "")
        src = edge.get("source", "")
        if tgt and src and tgt in parents:
            parents[tgt].append(src)

    # BFS backwards
    visited: set = set()
    queue: deque = deque([target_node_id])
    while queue:
        nid = queue.popleft()
        if nid in visited:
            continue
        visited.add(nid)
        for parent_id in parents.get(nid, []):
            if parent_id not in visited:
                queue.append(parent_id)

    # Filter nodes and edges to the ancestor set
    filtered_nodes = [n for n in nodes if n["id"] in visited]
    filtered_edges = [
        e for e in edges
        if e.get("source") in visited and e.get("target") in visited
    ]

    logger.debug(
        "Upstream subgraph trimmed",
        target=target_node_id,
        total_nodes=len(nodes),
        kept_nodes=len(filtered_nodes),
    )
    return filtered_nodes, filtered_edges


def execute_node_preview(
    flow_task_id: int,
    node_id: str,
    graph_snapshot: dict,
    limit: int = 500,
) -> dict:
    """
    Preview the output of a single node without executing the full graph.

    Steps:
    1. Compile the graph snapshot.
    2. Identify the CTE for the target node.
    3. Attach only the upstream input sources needed.
    4. Build the preview SQL: full CTE prefix up to target, then SELECT * LIMIT.
    5. Execute and return rows + metadata.

    Returns:
        {
            "columns": ["col1", "col2", ...],
            "column_types": {"col1": "VARCHAR", ...},
            "rows": [[val, val, ...], ...],
            "row_count": int,
            "elapsed_ms": int,
        }
    """
    start_time = time.time()
    conn: Optional[duckdb.DuckDBPyConnection] = None

    from app.core.concurrency import acquire_duckdb_slot, release_duckdb_slot
    acquire_duckdb_slot()
    try:
        conn = _setup_duckdb_connection()

        # Extract nodes/edges from the graph snapshot
        nodes: list = graph_snapshot.get("nodes", [])
        edges: list = graph_snapshot.get("edges", [])

        # Trim graph to only the target node + its upstream ancestors.
        # This prevents compilation errors from downstream nodes that are
        # partially configured (e.g. a Join with no join keys).
        nodes, edges = _get_upstream_subgraph(node_id, nodes, edges)

        # Inject sample_limit into input nodes so the LIMIT is applied at the
        # source level rather than on the final preview SELECT. This ensures
        # downstream transformations (aggregate, pivot, etc.) operate on the
        # already-limited dataset rather than having their output truncated.
        for n in nodes:
            if n.get("type") == "input":
                n.setdefault("data", {})
                # Only inject if user hasn't set their own sample_limit
                if not n["data"].get("sample_limit"):
                    n["data"]["sample_limit"] = limit

        # Inject ATTACH config into input nodes (after trimming)
        _inject_attach_configs(nodes, conn)

        # Compile only the upstream subgraph
        compiler = GraphCompiler({"nodes": nodes, "edges": edges}).compile()

        # Resolve target CTE (the trimmed graph contains only ancestors + target,
        # so target_cte will always be the LAST CTE in cte_sql_parts)
        target_cte = compiler.cte_map.get(node_id)
        if not target_cte:
            # Output nodes don't have CTEs — preview their upstream input instead
            node = next((n for n in nodes if n["id"] == node_id), None)
            if node and node.get("type") == "output":
                # Find the upstream node connected to this output
                upstream = [e["source"] for e in edges if e["target"] == node_id]
                if upstream:
                    target_cte = compiler.cte_map.get(upstream[0])

        if not target_cte:
            raise ValueError(
                f"Node '{node_id}' not found in compiled graph or has no CTE. "
                f"Available CTEs: {list(compiler.cte_map.keys())}"
            )

        # Build SQL for all CTEs up to and including the target CTE
        partial_prefix = compiler.get_cte_sql_up_to(target_cte)

        # No outer LIMIT — the limit is already injected into input node CTEs.
        # This ensures aggregate/pivot/join nodes show correct results on the
        # already-limited dataset rather than a truncated aggregate output.
        preview_sql = f"{partial_prefix}\nSELECT * FROM {target_cte}"
        logger.debug(f"Preview SQL for node {node_id}:\n{preview_sql}")

        # Execute
        rel = conn.execute(preview_sql)

        # Collect column metadata
        description = rel.description  # list of (name, type_code, ...)
        if not description:
            return {
                "columns": [],
                "column_types": {},
                "rows": [],
                "row_count": 0,
                "elapsed_ms": int((time.time() - start_time) * 1000),
            }

        columns = [col[0] for col in description]
        raw_rows = rel.fetchall()

        # Serialize rows (handle non-JSON-serializable types)
        serialized_rows = [_serialize_row(row) for row in raw_rows]

        # Map DuckDB type codes to human-readable strings
        column_types = _extract_column_types(description)

        elapsed = int((time.time() - start_time) * 1000)
        logger.info(
            "Node preview complete",
            flow_task_id=flow_task_id,
            node_id=node_id,
            row_count=len(serialized_rows),
            elapsed_ms=elapsed,
        )

        return {
            "columns": columns,
            "column_types": column_types,
            "rows": serialized_rows,
            "row_count": len(serialized_rows),
            "elapsed_ms": elapsed,
        }

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        release_duckdb_slot()
        from app.tasks.flow_task.connection_factory import cleanup_temp_files
        cleanup_temp_files()


def execute_node_schema(
    node_id: str,
    graph_snapshot: dict,
) -> dict:
    """
    Return the column schema (names + DuckDB type strings) for a node's output
    by executing the CTE chain up to that node with LIMIT 0.

    Zero rows are fetched from the remote DB — only the Arrow schema is read,
    so this is fast regardless of table size or transformation complexity.

    Returns:
        {
            "columns": [
                {"column_name": "...", "data_type": "VARCHAR"},
                ...
            ]
        }
    """
    conn: Optional[duckdb.DuckDBPyConnection] = None
    from app.core.concurrency import acquire_duckdb_slot, release_duckdb_slot
    acquire_duckdb_slot()
    try:
        conn = _setup_duckdb_connection()

        nodes = graph_snapshot.get("nodes", [])
        edges = graph_snapshot.get("edges", [])

        # Trim to only the upstream ancestors of the target node
        nodes, edges = _get_upstream_subgraph(node_id, nodes, edges)

        _inject_attach_configs(nodes, conn)

        compiler = GraphCompiler({"nodes": nodes, "edges": edges}).compile()

        target_cte = compiler.cte_map.get(node_id)
        if not target_cte:
            # Output nodes don't have CTEs — preview their upstream input instead
            node = next((n for n in nodes if n["id"] == node_id), None)
            if node and node.get("type") == "output":
                # Find the upstream node connected to this output
                upstream = [e["source"] for e in edges if e["target"] == node_id]
                if upstream:
                    target_cte = compiler.cte_map.get(upstream[0])

        if not target_cte:
            raise ValueError(
                f"Node '{node_id}' not found in compiled graph. "
                f"Available: {list(compiler.cte_map.keys())}"
            )

        partial_prefix = compiler.get_cte_sql_up_to(target_cte)

        schema_sql = f"{partial_prefix}\nSELECT * FROM {target_cte} LIMIT 0"
        logger.debug(f"Schema SQL for node {node_id}:\n{schema_sql}")

        rel = conn.execute(schema_sql)
        description = rel.description or []

        return {
            "columns": [
                {
                    "column_name": col[0],
                    "data_type": str(col[1]),
                }
                for col in description
            ]
        }

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        release_duckdb_slot()
        from app.tasks.flow_task.connection_factory import cleanup_temp_files
        cleanup_temp_files()


def _serialize_row(row: tuple) -> List[Any]:
    """Convert a tuple row to a JSON-serializable list."""
    result = []
    for val in row:
        if val is None:
            result.append(None)
        elif isinstance(val, (int, float, bool, str)):
            result.append(val)
        else:
            # Dates, Decimals, bytes, etc. → stringify
            result.append(str(val))
    return result


def _extract_column_types(description: list) -> Dict[str, str]:
    """
    Build a {column_name: type_name} dict from cursor description.

    DuckDB cursor description items: (name, type_code, display_size,
    internal_size, precision, scale, null_ok)
    type_code is a duckdb.typing object whose __str__ gives a readable name.
    """
    types: Dict[str, str] = {}
    for col in description:
        name = col[0]
        try:
            types[name] = str(col[1])
        except Exception:
            types[name] = "UNKNOWN"
    return types
