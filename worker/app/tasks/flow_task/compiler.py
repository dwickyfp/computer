"""
Flow Task graph compiler.

Converts a ReactFlow node/edge graph into DuckDB SQL.
Supports topological traversal so CTEs are emitted in dependency order.

Each node type produces one or more CTEs. The final compiled output is a
dict with:
  - cte_map: {node_id: cte_name}  — used for preview slicing
  - full_sql: str                  — full SQL including all CTEs + final outputs
  - output_nodes: list of dicts    — {node_id, cte_name, target_table, write_mode, upsert_keys}
"""

from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

# ─── Constants ─────────────────────────────────────────────────────────────────

VALID_NODE_TYPES = {
    "input", "clean", "aggregate", "join", "union",
    "pivot", "new_rows", "sql", "output",
}

SUPPORTED_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL"}
SUPPORTED_AGG_FUNCS = {"SUM", "COUNT", "AVG", "MIN", "MAX", "COUNT_DISTINCT"}
SUPPORTED_WRITE_MODES = {"APPEND", "UPSERT"}


# ─── Graph utilities ───────────────────────────────────────────────────────────

def _build_adjacency(nodes: List[dict], edges: List[dict]) -> Tuple[
    Dict[str, list], Dict[str, list]
]:
    """Build forward (successors) and backward (predecessors) adjacency maps."""
    succ: Dict[str, list] = defaultdict(list)   # node_id -> [child_node_ids]
    pred: Dict[str, list] = defaultdict(list)   # node_id -> [parent_node_ids]
    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        succ[src].append(tgt)
        pred[tgt].append(src)
    return dict(succ), dict(pred)


def _topological_sort(
    nodes: List[dict],
    pred: Dict[str, list],
) -> List[str]:
    """Kahn's algorithm topological sort. Returns ordered list of node IDs."""
    in_degree: Dict[str, int] = {n["id"]: len(pred.get(n["id"], [])) for n in nodes}
    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    order: List[str] = []

    # Build forward adjacency for in-degree decrement
    all_ids = {n["id"] for n in nodes}
    succ_tmp: Dict[str, list] = defaultdict(list)
    for nid in all_ids:
        for parent in pred.get(nid, []):
            succ_tmp[parent].append(nid)

    while queue:
        nid = queue.popleft()
        order.append(nid)
        for child in succ_tmp.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(nodes):
        raise ValueError(
            "Graph contains a cycle — cannot compile. "
            f"Processed {len(order)}/{len(nodes)} nodes."
        )
    return order


def _safe_identifier(s: str) -> str:
    """Sanitize a string into a safe SQL identifier."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", s)


def _cte_name(node_id: str, node_type: str) -> str:
    """Generate a deterministic CTE name from node id + type."""
    safe = _safe_identifier(node_id)
    return f"cte_{node_type}_{safe}"[-63:]   # PostgreSQL 63-char limit


# ─── Per-node SQL builders ─────────────────────────────────────────────────────

def _build_input_cte(node: dict, _pred_ctes: list) -> str:
    """
    INPUT node — attach a source connection and SELECT from it.

    data keys:
      source_type: POSTGRES | SNOWFLAKE
      attach_alias: str (unique alias for this connection)
      attach_dsn: str (DuckDB ATTACH string — injected by executor)
      schema_name: str (optional)
      table_name: str
      columns: list[str] (optional; defaults to *)
      sample_limit: int (optional)
      watermark_column: str (optional; D8 incremental — column to filter on)
      watermark_value: str (optional; D8 incremental — last processed value)
      filter_sql: str (optional — raw WHERE expression applied at source)
      filter_rows: list[{col, op, val}] (optional — UI filter builder rows compiled to filter_sql)
    """
    data = node.get("data", {})
    alias = _safe_identifier(data.get("attach_alias", "src"))
    schema = data.get("schema_name") or None  # None = omit schema part (use connection default)
    table = data.get("table_name", "")
    cols = data.get("columns")
    sample = data.get("sample_limit")
    watermark_col = data.get("watermark_column")
    watermark_val = data.get("watermark_value")
    filter_sql = (data.get("filter_sql") or "").strip()

    if not table:
        raise ValueError(f"Input node {node['id']} missing table_name")

    col_expr = ", ".join(cols) if cols else "*"
    fqt = f"{alias}.{schema}.{table}" if schema else f"{alias}.{table}"
    sql = f"SELECT {col_expr} FROM {fqt}"

    # Build WHERE clause from multiple sources
    where_parts: List[str] = []

    # User-defined filter SQL (from filter builder or raw SQL)
    if filter_sql:
        where_parts.append(f"({filter_sql})")

    # D8: Incremental execution — add WHERE clause for watermark
    if watermark_col and watermark_val is not None and watermark_val != "":
        # Escape single quotes in watermark value
        safe_val = str(watermark_val).replace("'", "''")
        where_parts.append(f"{watermark_col} > '{safe_val}'")

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    if sample:
        sql += f" LIMIT {int(sample)}"
    return sql


def _build_clean_cte(node: dict, pred_ctes: list) -> str:
    """
    CLEAN node — filter, rename, calculate, cast, expressions, deduplicate, select columns.

    data keys:
      filter_expr: str             — compiled SQL WHERE expression (from UI filter builder)
      filters: list[str]           — legacy raw SQL WHERE conditions (ANDed, fallback)
      select_columns: list[str]    — explicit column list to keep (empty = keep all)
      drop_columns: list[str]      — legacy columns to drop via EXCLUDE
      renames: [{old, new}]        — column renames
      calculations: [{expr, alias}] — new calculated columns
      cast_columns: [{column, target_type}] — column type casting
      expressions: [{expr, alias}] — SQL function expressions (COALESCE, NULLIF, etc.)
      drop_nulls: bool             — wrap in SELECT * WHERE col IS NOT NULL (skipped; handled downstream)
      deduplicate: bool            — emit SELECT DISTINCT
    """
    if not pred_ctes:
        raise ValueError(f"Clean node {node['id']} has no upstream nodes")

    data = node.get("data", {})
    source = pred_ctes[0]

    # --- Column selection ---
    # Priority: explicit select_columns > drop_columns (legacy) > *
    select_columns: List[str] = data.get("select_columns", [])
    drop_cols: List[str] = data.get("drop_columns", [])
    renames: List[dict] = data.get("renames", [])
    calcs: List[dict] = data.get("calculations", [])
    cast_columns: List[dict] = data.get("cast_columns", [])
    expressions: List[dict] = data.get("expressions", [])

    if select_columns:
        # Explicit column list wins
        col_parts = list(select_columns)
    else:
        col_parts = []
        select_all = "* EXCLUDE (" + ", ".join(drop_cols) + ")" if drop_cols else "*"
        col_parts.append(select_all)

    # Renames via alias (append after base columns)
    for r in renames:
        col_parts.append(f"{r['old']} AS {r['new']}")

    # Calculated columns
    for c in calcs:
        col_parts.append(f"{c['expr']} AS {c['alias']}")

    # Type casting — CAST(column AS target_type) AS column
    for cc in cast_columns:
        column = cc.get("column", "")
        target_type = cc.get("target_type", "")
        if column and target_type:
            col_parts.append(f"CAST({column} AS {target_type}) AS {column}")

    # SQL function expressions — COALESCE, NULLIF, TRIM, UPPER, LOWER, etc.
    for expr_item in expressions:
        expr = expr_item.get("expr", "")
        alias = expr_item.get("alias", "")
        if expr and alias:
            col_parts.append(f"{expr} AS {alias}")

    distinct_kw = "DISTINCT " if data.get("deduplicate") else ""
    col_expr = ", ".join(col_parts)
    sql = f"SELECT {distinct_kw}{col_expr} FROM {source}"

    # --- WHERE conditions ---
    # Priority: filter_expr (string from UI builder) > filters (legacy list)
    filter_expr: str = data.get("filter_expr", "").strip()
    legacy_filters: List[str] = data.get("filters", [])

    where_parts: List[str] = []
    if filter_expr:
        where_parts.append(f"({filter_expr})")
    for f in legacy_filters:
        if f and f.strip():
            where_parts.append(f"({f})")

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    return sql


def _build_aggregate_cte(node: dict, pred_ctes: list) -> str:
    """
    AGGREGATE node — GROUP BY + aggregations.

    data keys:
      group_by: list[str]
      aggregations: [{column, func, alias}]  — func in SUPPORTED_AGG_FUNCS
    """
    if not pred_ctes:
        raise ValueError(f"Aggregate node {node['id']} has no upstream nodes")

    data = node.get("data", {})
    source = pred_ctes[0]
    group_by: List[str] = data.get("group_by", [])
    aggs: List[dict] = data.get("aggregations", [])

    col_parts = list(group_by)
    for agg in aggs:
        # Support both "function" (from NodeConfigPanel) and legacy "func" key
        func = (agg.get("function") or agg.get("func") or "COUNT").upper()
        col = agg.get("column", "*")
        alias = agg.get("alias", "")
        if not alias:
            alias = f"{func.lower()}_{_safe_identifier(col)}"
        if func == "COUNT_DISTINCT":
            col_parts.append(f"COUNT(DISTINCT {col}) AS {alias}")
        else:
            col_parts.append(f"{func}({col}) AS {alias}")

    col_expr = ", ".join(col_parts) if col_parts else "*"
    sql = f"SELECT {col_expr} FROM {source}"
    if group_by:
        sql += " GROUP BY " + ", ".join(group_by)
    return sql


def _build_join_cte(node: dict, pred_ctes: list) -> str:
    """
    JOIN node — two-input join.

    Expects exactly two predecessors: pred_ctes[0]=left, pred_ctes[1]=right.

    data keys:
      join_type: INNER | LEFT | RIGHT | FULL
      left_keys: list[str]
      right_keys: list[str]
      left_prefix: str (optional; prefix for left columns)
      right_prefix: str (optional; prefix for right columns)
      output_columns: list[str] (optional; explicit output columns)
    """
    if len(pred_ctes) < 2:
        raise ValueError(
            f"Join node {node['id']} requires exactly 2 inputs, got {len(pred_ctes)}"
        )

    data = node.get("data", {})
    left, right = pred_ctes[0], pred_ctes[1]
    join_type = data.get("join_type", "INNER").upper()
    if join_type not in SUPPORTED_JOIN_TYPES:
        join_type = "INNER"

    left_keys: List[str] = data.get("left_keys", [])
    right_keys: List[str] = data.get("right_keys", [])

    if not left_keys or not right_keys or len(left_keys) != len(right_keys):
        raise ValueError(
            f"Join node {node['id']}: left_keys and right_keys must be non-empty "
            "and equal length"
        )

    output_cols = data.get("output_columns", [])
    col_expr = ", ".join(output_cols) if output_cols else "__l.*, __r.*"

    # Build ON clause
    on_clauses = " AND ".join(
        f"__l.{lk} = __r.{rk}" for lk, rk in zip(left_keys, right_keys)
    )

    sql = (
        f"SELECT {col_expr} "
        f"FROM {left} AS __l "
        f"{join_type} JOIN {right} AS __r ON {on_clauses}"
    )
    return sql


def _build_union_cte(node: dict, pred_ctes: list) -> str:
    """
    UNION node — stack multiple inputs.

    data keys:
      distinct: bool (default False → UNION ALL)
    """
    if len(pred_ctes) < 2:
        raise ValueError(
            f"Union node {node['id']} requires at least 2 inputs, got {len(pred_ctes)}"
        )

    data = node.get("data", {})
    distinct = data.get("distinct", False)
    union_kw = "UNION" if distinct else "UNION ALL"

    parts = [f"SELECT * FROM {c}" for c in pred_ctes]
    return f" {union_kw} ".join(parts)


def _build_pivot_cte(node: dict, pred_ctes: list) -> str:
    """
    PIVOT node — columns to rows (unpivot) or rows to columns (pivot).

    data keys:
      direction: PIVOT | UNPIVOT
      -- For PIVOT:
      pivot_column: str           — categorical column to pivot out
      value_column: str           — column whose values fill the new columns
      group_columns: list[str]    — columns to preserve as rows
      agg_func: str               — SUM | COUNT | AVG etc.
      pivot_values: list[str]     — explicit pivot values (optional; auto if DuckDB)
      -- For UNPIVOT:
      value_alias: str            — alias for the value column
      name_alias: str             — alias for the name column
      unpivot_columns: list[str]  — columns to unpivot
      id_columns: list[str]       — columns to preserve
    """
    if not pred_ctes:
        raise ValueError(f"Pivot node {node['id']} has no upstream nodes")

    data = node.get("data", {})
    source = pred_ctes[0]
    direction = data.get("direction", "PIVOT").upper()

    if direction == "PIVOT":
        pivot_col = data.get("pivot_column", "")
        value_col = data.get("value_column", "")
        group_cols = data.get("group_columns", [])
        agg_func = data.get("agg_func", "SUM")
        pivot_vals = data.get("pivot_values", [])

        if not pivot_col or not value_col:
            raise ValueError(
                f"Pivot node {node['id']}: pivot_column and value_column are required"
            )

        group_expr = ", ".join(group_cols) if group_cols else ""
        vals_expr = ""
        if pivot_vals:
            quoted = ", ".join(f"'{v}'" for v in pivot_vals)
            vals_expr = f" IN ({quoted})"

        sql = (
            f"PIVOT {source} ON {pivot_col}{vals_expr} "
            f"USING {agg_func}({value_col})"
        )
        if group_expr:
            sql += f" GROUP BY {group_expr}"
        return sql

    else:  # UNPIVOT
        id_cols = data.get("id_columns", [])
        unpivot_cols = data.get("unpivot_columns", [])
        val_alias = data.get("value_alias", "value")
        name_alias = data.get("name_alias", "name")

        if not unpivot_cols:
            raise ValueError(
                f"Pivot (UNPIVOT) node {node['id']}: unpivot_columns is required"
            )

        cols_expr = ", ".join(unpivot_cols)
        id_expr = ", ".join(id_cols) if id_cols else ""
        sql = (
            f"UNPIVOT {source} ON {cols_expr} "
            f"INTO NAME {name_alias} VALUE {val_alias}"
        )
        return sql


def _build_new_rows_cte(node: dict, pred_ctes: list) -> str:
    """
    NEW ROWS (Add Columns) node — appends new columns to an upstream CTE.

    Each column definition in data["columns"] is one of:
      { "alias": "col_name", "type": "static",     "value": <literal> }
      { "alias": "col_name", "type": "expression", "expr":  <sql_expr> }

    Static string values are single-quoted; numbers/booleans are used as-is.
    Expression values are injected verbatim as SQL expressions.

    Produces:
        SELECT *, <expr1> AS alias1, <expr2> AS alias2
        FROM {source_cte}
    """
    if not pred_ctes:
        raise ValueError(
            f"New Rows node {node['id']} has no upstream input. "
            "Connect an upstream node first."
        )

    data = node.get("data", {})
    columns: list = data.get("columns", [])

    if not columns:
        raise ValueError(
            f"New Rows node {node['id']}: define at least one column."
        )

    source = pred_ctes[0]
    col_parts: list[str] = []

    for col in columns:
        alias = _safe_identifier(col.get("alias", "new_col"))
        col_type = col.get("type", "static")

        if col_type == "expression":
            expr = col.get("expr", "NULL")
            col_parts.append(f"{expr} AS {alias}")
        else:
            # Static value — detect type and quote strings
            raw = col.get("value", "")
            if isinstance(raw, bool):
                sql_val = "TRUE" if raw else "FALSE"
            elif isinstance(raw, (int, float)):
                sql_val = str(raw)
            else:
                # Treat as string literal — escape single quotes
                escaped = str(raw).replace("'", "''")
                sql_val = f"'{escaped}'"
            col_parts.append(f"{sql_val} AS {alias}")

    extra_cols = ", ".join(col_parts)
    return f"SELECT *, {extra_cols} FROM {source}"


def _build_sql_cte(node: dict, pred_ctes: list) -> str:
    """
    SQL node — user-provided raw SQL with template variable substitution (D6).

    Allows advanced users to write arbitrary SQL transformations. The upstream
    CTEs are available via the ``{{input}}`` (first upstream) or ``{{input_N}}``
    (Nth upstream, 0-indexed) template variables. ``{{upstream}}`` is an alias
    for ``{{input}}``.

    data keys:
      sql_expression: str   — raw SQL query body (required)
                              Example: "SELECT *, col1 + col2 AS total FROM {{input}}"

    Security note:
      This node is intended for trusted ETL developers only. The SQL is injected
      verbatim into the CTE chain — no sandboxing is applied beyond what DuckDB
      enforces (read-only ATTACH, memory limits, statement timeout).
    """
    if not pred_ctes:
        raise ValueError(f"SQL node {node['id']} has no upstream nodes")

    data = node.get("data", {})
    sql_expr: str = data.get("sql_expression", "").strip()

    if not sql_expr:
        raise ValueError(
            f"SQL node {node['id']}: sql_expression is required. "
            f"Use {{{{input}}}} to reference the upstream CTE."
        )

    # Template variable substitution
    # {{input}} or {{upstream}} → first predecessor CTE
    result = sql_expr.replace("{{input}}", pred_ctes[0])
    result = result.replace("{{upstream}}", pred_ctes[0])

    # {{input_N}} → Nth predecessor CTE (0-indexed)
    for i, cte in enumerate(pred_ctes):
        result = result.replace(f"{{{{input_{i}}}}}", cte)

    # Strip trailing semicolons (DuckDB CTEs don't need them)
    result = result.rstrip(";").strip()

    return result


def _build_output_node_info(node: dict, pred_ctes: list) -> dict:
    """
    OUTPUT node — extract write configuration.

    Not a CTE itself; produces a dict consumed by the executor to
    write data from the upstream CTE to the destination table.

    data keys:
      target_table: str
      schema_name: str (default 'public')
      write_mode: APPEND | UPSERT
      upsert_keys: list[str]          — required if write_mode=UPSERT
      destination_id: int             — destination record ID
    """
    if not pred_ctes:
        raise ValueError(f"Output node {node['id']} has no upstream nodes")

    data = node.get("data", {})
    return {
        "node_id": node["id"],
        "source_cte": pred_ctes[0],
        # UI stores as 'table_name'; fall back to 'target_table' for backward compat
        "target_table": data.get("table_name") or data.get("target_table") or "",
        "schema_name": data.get("schema_name") or None,
        "write_mode": data.get("write_mode", "APPEND").upper(),
        "upsert_keys": data.get("upsert_keys", []),
        "destination_id": data.get("destination_id"),
    }


# ─── Main compiler ─────────────────────────────────────────────────────────────

class GraphCompiler:
    """
    Compiles a ReactFlow node/edge graph into DuckDB SQL.

    Usage::

        compiler = GraphCompiler(graph_json)
        result = compiler.compile()
        # result.cte_map        — {node_id: cte_name}
        # result.cte_sql_parts  — ordered list of (cte_name, sql) tuples
        # result.output_nodes   — list of output node configs
        # result.full_cte_sql   — "WITH cte1 AS (...), cte2 AS (...) SELECT 1"
    """

    def __init__(self, graph_json: dict):
        self.nodes: List[dict] = graph_json.get("nodes", [])
        self.edges: List[dict] = graph_json.get("edges", [])
        self._node_map: Dict[str, dict] = {n["id"]: n for n in self.nodes}
        self.cte_map: Dict[str, str] = {}            # node_id -> cte_name
        self.cte_sql_parts: List[Tuple[str, str]] = []  # [(cte_name, sql)]
        self.output_nodes: List[dict] = []
        self._order: List[str] = []
        self._pred_ctes_map: Dict[str, List[str]] = {}  # node_id -> [pred cte names]

    def compile(self) -> "GraphCompiler":
        """Run the full compilation pipeline. Returns self for chaining."""
        if not self.nodes:
            raise ValueError("Graph is empty — no nodes to compile")

        _, pred = _build_adjacency(self.nodes, self.edges)
        self._order = _topological_sort(self.nodes, pred)

        for node_id in self._order:
            node = self._node_map[node_id]
            node_type = node.get("type", "")
            pred_node_ids = pred.get(node_id, [])
            pred_ctes = [self.cte_map[pid] for pid in pred_node_ids if pid in self.cte_map]
            self._pred_ctes_map[node_id] = pred_ctes

            if node_type == "output":
                info = _build_output_node_info(node, pred_ctes)
                self.output_nodes.append(info)
                # Output nodes don't produce a CTE
                continue

            if node_type == "note":
                # Note nodes are purely visual annotations — no SQL, no CTE.
                continue

            cte_name = _cte_name(node_id, node_type)
            self.cte_map[node_id] = cte_name

            sql = self._build_node_sql(node, node_type, pred_ctes)
            self.cte_sql_parts.append((cte_name, sql))

        logger.info(
            "Graph compiled",
            nodes=len(self.nodes),
            ctes=len(self.cte_sql_parts),
            outputs=len(self.output_nodes),
        )
        return self

    def _build_node_sql(
        self, node: dict, node_type: str, pred_ctes: List[str]
    ) -> str:
        builders = {
            "input": _build_input_cte,
            "clean": _build_clean_cte,
            "aggregate": _build_aggregate_cte,
            "join": _build_join_cte,
            "union": _build_union_cte,
            "pivot": _build_pivot_cte,
            "new_rows": _build_new_rows_cte,
            "sql": _build_sql_cte,
        }
        if node_type not in builders:
            raise ValueError(
                f"Unknown node type '{node_type}' on node {node['id']}. "
                f"Valid types: {sorted(builders.keys())}"
            )
        return builders[node_type](node, pred_ctes)

    def get_cte_sql_up_to(self, target_cte: str) -> str:
        """
        Build a WITH ... SELECT query that includes only the CTEs up to
        and including target_cte. Used for node preview.
        """
        parts = []
        for cte_name, sql in self.cte_sql_parts:
            parts.append((cte_name, sql))
            if cte_name == target_cte:
                break

        if not parts:
            raise ValueError(f"CTE '{target_cte}' not found in compiled graph")

        cte_block = ",\n".join(f"{name} AS (\n{body}\n)" for name, body in parts)
        return f"WITH {cte_block}"

    @property
    def full_cte_prefix(self) -> str:
        """Return the WITH ... block (without a final SELECT). For use in write SQL."""
        if not self.cte_sql_parts:
            return ""
        cte_block = ",\n".join(
            f"{name} AS (\n{body}\n)" for name, body in self.cte_sql_parts
        )
        return f"WITH {cte_block}"
