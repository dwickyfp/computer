"""
Flow Task executor — full graph execution via DuckDB.

Responsible for:
1. Loading and compiling the graph
2. Installing/loading DuckDB extensions (postgres, snowflake, httpfs)
3. Injecting ATTACH DSN strings into input node configs
4. Executing all CTEs
5. Writing each output node via DestinationWriterRegistry
6. Capturing per-node row counts and timing
7. Persisting results back to flow_task_run_history and flow_task_run_node_log
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import duckdb
import structlog

from app.tasks.flow_task.compiler import GraphCompiler, _cte_name
from app.tasks.flow_task.connection_factory import SourceConnectionFactory
from app.tasks.flow_task.destination_writer import DestinationWriterRegistry

logger = structlog.get_logger(__name__)

# ─── Ensure ADBC Snowflake driver path is set for DuckDB ─────────────────────
# Per https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup,
# DuckDB auto-finds the driver from ~/.duckdb/extensions/<version>/<platform>/
# (installed at Docker build time). SNOWFLAKE_ADBC_DRIVER_PATH is an explicit
# fallback recognized by the extension. Set it from the installed Python package
# if not already provided via environment.
if not os.environ.get("SNOWFLAKE_ADBC_DRIVER_PATH"):
    try:
        import adbc_driver_snowflake as _adbc_sf
        _so = os.path.join(os.path.dirname(_adbc_sf.__file__), "libadbc_driver_snowflake.so")
        if os.path.exists(_so):
            os.environ["SNOWFLAKE_ADBC_DRIVER_PATH"] = _so
    except Exception:
        pass  # ADBC package not installed — driver must be in ~/.duckdb/extensions/


# ─── Extension management ──────────────────────────────────────────────────────

_REQUIRED_EXTENSIONS = ["postgres", "httpfs", "spatial"]
_COMMUNITY_EXTENSIONS = ["snowflake"]


def _setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB in-memory connection.

    Extensions are INSTALL-ed once at worker startup (see celery_app.py
    worker_init signal). Here we only LOAD them, which is fast.

    Memory and thread limits are applied to prevent OOM when multiple
    DuckDB connections run concurrently (max_concurrent × memory_limit
    must fit within available system RAM).
    """
    from app.config.settings import get_settings

    settings = get_settings()
    conn = duckdb.connect(database=":memory:")

    # Apply resource limits — critical for concurrent execution safety
    conn.execute(f"SET memory_limit='{settings.duckdb_memory_limit}';")
    conn.execute(f"SET threads={settings.duckdb_threads};")

    # Load required extensions (already installed at worker startup)
    for ext in _REQUIRED_EXTENSIONS:
        try:
            conn.execute(f"LOAD {ext};")
        except Exception as e:
            logger.warning(f"Extension {ext} load warning: {e}")
    # Load community extensions
    for ext in _COMMUNITY_EXTENSIONS:
        try:
            conn.execute(f"LOAD {ext};")
            logger.info(f"Loaded community extension: {ext}")
        except Exception as e:
            logger.warning(f"Community extension {ext} load warning: {e}")
    return conn


def _load_optional_extension(conn: duckdb.DuckDBPyConnection, ext: str) -> None:
    try:
        conn.execute(f"LOAD {ext};")
        logger.info(f"Loaded optional extension: {ext}")
    except Exception as e:
        logger.warning(f"Optional extension {ext} not available: {e}")


# ─── Input node DSN injection ──────────────────────────────────────────────────

def _inject_attach_configs(
    nodes: List[dict],
    conn: duckdb.DuckDBPyConnection,
) -> List[str]:
    """
    For each INPUT node, resolve the connection from source_id or destination_id,
    build the ATTACH SQL, and execute it. Injects `attach_alias` into node data
    so the compiler can reference the alias.

    Returns a list of attached aliases (for cleanup reference).
    """
    attached_aliases: List[str] = []
    seen_source_ids: Dict[str, str] = {}  # "source:id" -> alias

    for node in nodes:
        if node.get("type") != "input":
            continue

        data = node.get("data", {})
        source_type = data.get("source_type", "POSTGRES").upper()
        source_id = data.get("source_id")
        destination_id = data.get("destination_id")

        if not source_id and not destination_id:
            raise ValueError(
                f"Input node {node['id']} missing source_id or destination_id"
            )

        # Build a unique alias per input node
        raw_alias = f"src_{node['id'].replace('-', '_')}"[:30]
        alias = raw_alias

        # Deduplicate: same source -> same alias
        dedup_key = f"source:{source_id}" if source_id else f"dest:{destination_id}"
        if dedup_key in seen_source_ids:
            alias = seen_source_ids[dedup_key]
            data["attach_alias"] = alias
            continue

        seen_source_ids[dedup_key] = alias
        data["attach_alias"] = alias  # inject for compiler

        # Resolve attach SQL
        if source_type == "SNOWFLAKE" and destination_id:
            attach_sql, setup_sql, ext_name = (
                SourceConnectionFactory.build_attach_sql_from_destination(
                    destination_id=destination_id,
                    alias=alias,
                    read_only=True,
                )
            )
            if ext_name == "snowflake":
                _load_optional_extension(conn, "snowflake")
            # Inject schema from destination config when not explicitly set on the node.
            # Snowflake schemas are rarely 'public' — use the configured schema instead.
            if not data.get("schema_name"):
                _sf_schema = SourceConnectionFactory.get_destination_schema(destination_id)
                if _sf_schema:
                    data["schema_name"] = _sf_schema
        elif source_id:
            attach_sql, setup_sql, ext_name = (
                SourceConnectionFactory.build_attach_sql_from_source(
                    source_id=source_id,
                    alias=alias,
                    read_only=True,
                )
            )
        else:
            # Destination-based postgres
            attach_sql, setup_sql, ext_name = (
                SourceConnectionFactory.build_attach_sql_from_destination(
                    destination_id=destination_id,
                    alias=alias,
                    read_only=True,
                )
            )

        if setup_sql:
            conn.execute(setup_sql)
        conn.execute(attach_sql)
        attached_aliases.append(alias)
        logger.info(f"Attached source: alias={alias} type={source_type}")

    return attached_aliases


def _attach_output_destination(
    conn: duckdb.DuckDBPyConnection,
    destination_id: Optional[int],
    output_alias: str = "__dest",
) -> str:
    """Attach the output destination (PostgreSQL) to DuckDB. Returns the alias used."""
    if destination_id is None:
        # Default to the existing 'dest' alias if already attached from input
        return output_alias

    attach_sql, setup_sql, _ = (
        SourceConnectionFactory.build_attach_sql_from_destination(
            destination_id=destination_id,
            alias=output_alias,
            read_only=False,
        )
    )
    if setup_sql:
        conn.execute(setup_sql)
    conn.execute(attach_sql)
    return output_alias


# ─── Main executor ─────────────────────────────────────────────────────────────

def execute_flow_task(
    flow_task_id: int,
    run_history_id: int,
    graph_json: dict,
) -> dict:
    """
    Execute the full flow task graph.

    Compiles graph → DuckDB SQL, attaches all sources/destinations,
    runs all node CTEs in order, writes outputs, and records metrics.

    Supports incremental execution (D8) — if input nodes have watermark_column
    configured, the executor loads the last watermark value from the DB and
    injects it into the node data before compilation. After successful execution,
    watermarks are updated with the max value seen.

    Returns a result dict with:
      status, total_input_records, total_output_records, node_logs
    """
    start_time = time.time()
    node_logs: List[dict] = []
    total_input_records = 0
    total_output_records = 0

    conn: Optional[duckdb.DuckDBPyConnection] = None

    from app.core.concurrency import acquire_duckdb_slot, release_duckdb_slot
    acquire_duckdb_slot()
    try:
        conn = _setup_duckdb_connection()

        # Step 1: Compile graph
        nodes = graph_json.get("nodes", [])
        edges = graph_json.get("edges", [])

        # Step 2: Inject ATTACH configs into input nodes
        _inject_attach_configs(nodes, conn)

        # Step 2b (D8): Inject watermark values for incremental execution
        _inject_watermark_values(flow_task_id, nodes)

        # Recompile after injection (node data updated with attach_alias)
        compiler = GraphCompiler({"nodes": nodes, "edges": edges}).compile()

        # Step 3: Count input records
        for node in nodes:
            if node.get("type") == "input":
                cte_name = compiler.cte_map.get(node["id"])
                if cte_name:
                    try:
                        count_sql = (
                            f"{compiler.full_cte_prefix}\n"
                            f"SELECT COUNT(*) FROM {cte_name}"
                        )
                        result = conn.execute(count_sql).fetchone()
                        total_input_records += result[0] if result else 0
                    except Exception as e:
                        logger.warning(f"Could not count input rows for {cte_name}: {e}")

        # Step 4: Execute each output node
        attached_output_aliases: Dict[int, str] = {}

        for idx, output_info in enumerate(compiler.output_nodes):
            node_id = output_info["node_id"]
            source_cte = output_info["source_cte"]
            target_table = output_info["target_table"]
            schema_name = output_info.get("schema_name") or "public"
            write_mode = output_info.get("write_mode", "APPEND")
            upsert_keys = output_info.get("upsert_keys", [])
            destination_id = output_info.get("destination_id")

            node_start = time.time()
            node_log: dict = {
                "node_id": node_id,
                "node_type": "output",
                "node_label": f"Output → {target_table}",
                "row_count_in": 0,
                "row_count_out": 0,
                "status": "RUNNING",
                "error_message": None,
            }

            try:
                # Determine destination type first (needed for attach decision)
                dest_type = _get_destination_type(destination_id)
                writer = DestinationWriterRegistry.get_writer(dest_type)

                # Attach destination if not already done
                # Skip for non-DuckDB writers (e.g. Snowflake uses native connector)
                dest_alias = f"__out_{idx}"
                if dest_type not in ("SNOWFLAKE",):
                    dest_alias = attached_output_aliases.get(destination_id, dest_alias)
                    if destination_id not in attached_output_aliases:
                        dest_alias = _attach_output_destination(
                            conn, destination_id, output_alias=dest_alias
                        )
                        attached_output_aliases[destination_id] = dest_alias

                # Get row count before writing
                row_count_in = writer.get_row_count(
                    conn, compiler.full_cte_prefix, source_cte
                )

                # Write
                rows_written = writer.write(
                    conn=conn,
                    source_cte=source_cte,
                    cte_prefix=compiler.full_cte_prefix,
                    target_table=target_table,
                    schema_name=schema_name,
                    write_mode=write_mode,
                    upsert_keys=upsert_keys,
                    output_alias=dest_alias,
                    destination_id=destination_id,
                )

                total_output_records += rows_written
                node_log.update(
                    row_count_in=row_count_in,
                    row_count_out=rows_written,
                    status="SUCCESS",
                    duration_ms=int((time.time() - node_start) * 1000),
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(
                    f"Output node {node_id} failed: {error_msg}",
                    exc_info=True,
                )
                node_log.update(
                    status="FAILED",
                    error_message=error_msg,
                    duration_ms=int((time.time() - node_start) * 1000),
                )
                _notify_flow_task_error(
                    flow_task_id=flow_task_id,
                    error_msg=error_msg,
                    node_id=node_id,
                )

            node_logs.append(node_log)

        # Step 5: Determine overall status — FAILED if any output node failed
        failed_nodes = [nl for nl in node_logs if nl.get("status") == "FAILED"]
        overall_status = "FAILED" if failed_nodes else "SUCCESS"
        overall_error = (
            "; ".join(nl.get("error_message", "") for nl in failed_nodes)
            if failed_nodes
            else None
        )

        # Step 5b (D8): Update watermarks after successful execution
        if overall_status == "SUCCESS":
            _update_watermarks_after_execution(
                flow_task_id, nodes, conn, compiler
            )

        _persist_run_results(
            run_history_id=run_history_id,
            flow_task_id=flow_task_id,
            status=overall_status,
            total_input_records=total_input_records,
            total_output_records=total_output_records,
            node_logs=node_logs,
            error_message=overall_error,
        )

        elapsed = int((time.time() - start_time) * 1000)
        logger.info(
            "Flow task execution complete",
            flow_task_id=flow_task_id,
            run_history_id=run_history_id,
            total_input=total_input_records,
            total_output=total_output_records,
            elapsed_ms=elapsed,
            status=overall_status,
        )

        return {
            "status": overall_status,
            "total_input_records": total_input_records,
            "total_output_records": total_output_records,
            "elapsed_ms": elapsed,
            "node_logs": node_logs,
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Flow task execution failed: {error_msg}", exc_info=True)

        _notify_flow_task_error(
            flow_task_id=flow_task_id,
            error_msg=error_msg,
        )

        _persist_run_results(
            run_history_id=run_history_id,
            flow_task_id=flow_task_id,
            status="FAILED",
            total_input_records=total_input_records,
            total_output_records=total_output_records,
            node_logs=node_logs,
            error_message=error_msg,
        )

        raise

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        release_duckdb_slot()
        from app.tasks.flow_task.connection_factory import cleanup_temp_files
        cleanup_temp_files()


def _get_destination_type(destination_id: Optional[int]) -> str:
    """Look up the destination type from the config DB (cached 60s)."""
    if destination_id is None:
        return "POSTGRES"
    return _get_destination_type_cached(destination_id)


# ── simple TTL cache for destination type lookups ──
import threading as _th
_dest_type_cache: dict[int, tuple[float, str]] = {}
_dest_type_lock = _th.Lock()
_DEST_TYPE_TTL = 60.0  # seconds
_DEST_TYPE_MAX_SIZE = 128  # prevent unbounded growth


def _get_destination_type_cached(destination_id: int) -> str:
    import time as _time
    now = _time.monotonic()

    with _dest_type_lock:
        entry = _dest_type_cache.get(destination_id)
        if entry and (now - entry[0]) < _DEST_TYPE_TTL:
            return entry[1]

    # Cache miss — query DB outside lock
    try:
        from app.core.database import get_db_session
        from sqlalchemy import text

        with get_db_session() as db:
            row = db.execute(
                text("SELECT type FROM destinations WHERE id = :id"),
                {"id": destination_id},
            ).fetchone()
        result = row.type.upper() if row else "POSTGRES"
    except Exception:
        result = "POSTGRES"

    with _dest_type_lock:
        # Evict oldest entries if cache is full
        if len(_dest_type_cache) >= _DEST_TYPE_MAX_SIZE:
            sorted_keys = sorted(_dest_type_cache, key=lambda k: _dest_type_cache[k][0])
            for k in sorted_keys[: len(sorted_keys) // 2]:
                del _dest_type_cache[k]
        _dest_type_cache[destination_id] = (now, result)
    return result


def _inject_watermark_values(
    flow_task_id: int, nodes: List[dict]
) -> None:
    """
    D8: Load watermark values from DB and inject into input node data.

    For each input node with a `watermark_column` configured in its data,
    loads the last stored watermark_value from flow_task_watermarks table
    and injects it as `watermark_value` in the node data. The compiler
    will then add a WHERE clause to filter only new records.
    """
    input_nodes_with_watermark = [
        n for n in nodes
        if n.get("type") == "input" and n.get("data", {}).get("watermark_column")
    ]

    if not input_nodes_with_watermark:
        return

    try:
        from app.core.database import get_db_session
        from sqlalchemy import text

        with get_db_session() as db:
            for node in input_nodes_with_watermark:
                node_id = node["id"]
                row = db.execute(
                    text(
                        "SELECT watermark_value FROM flow_task_watermarks "
                        "WHERE flow_task_id = :ft_id AND node_id = :nid"
                    ),
                    {"ft_id": flow_task_id, "nid": node_id},
                ).fetchone()

                if row and row.watermark_value is not None:
                    node["data"]["watermark_value"] = row.watermark_value
                    logger.info(
                        f"Injected watermark for node {node_id}: "
                        f"column={node['data']['watermark_column']}, "
                        f"value={row.watermark_value}"
                    )
                else:
                    logger.debug(
                        f"No watermark found for node {node_id} — "
                        f"full scan will be performed"
                    )
    except Exception as e:
        logger.warning(f"Failed to load watermarks: {e}. Full scan will be used.")


def _update_watermarks_after_execution(
    flow_task_id: int,
    nodes: List[dict],
    conn: Optional[Any],
    compiler: Any,
) -> None:
    """
    D8: Update watermarks after successful execution.

    For each input node with a watermark_column, queries the CTE to find
    the MAX value of the watermark column, then upserts it into
    flow_task_watermarks.
    """
    input_nodes_with_watermark = [
        n for n in nodes
        if n.get("type") == "input" and n.get("data", {}).get("watermark_column")
    ]

    if not input_nodes_with_watermark or conn is None:
        return

    try:
        from app.core.database import get_db_session
        from sqlalchemy import text

        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        for node in input_nodes_with_watermark:
            node_id = node["id"]
            watermark_col = node["data"]["watermark_column"]
            cte_name = compiler.cte_map.get(node_id)

            if not cte_name:
                continue

            try:
                # Query the max watermark value from the CTE
                max_sql = (
                    f"{compiler.full_cte_prefix}\n"
                    f"SELECT MAX({watermark_col})::VARCHAR AS max_val FROM {cte_name}"
                )
                result = conn.execute(max_sql).fetchone()
                max_val = result[0] if result else None

                if max_val is not None:
                    # Upsert watermark to DB
                    with get_db_session() as db:
                        existing = db.execute(
                            text(
                                "SELECT id FROM flow_task_watermarks "
                                "WHERE flow_task_id = :ft_id AND node_id = :nid"
                            ),
                            {"ft_id": flow_task_id, "nid": node_id},
                        ).fetchone()

                        if existing:
                            db.execute(
                                text(
                                    "UPDATE flow_task_watermarks "
                                    "SET watermark_value = :val, updated_at = :now "
                                    "WHERE flow_task_id = :ft_id AND node_id = :nid"
                                ),
                                {
                                    "val": str(max_val),
                                    "now": now,
                                    "ft_id": flow_task_id,
                                    "nid": node_id,
                                },
                            )
                        else:
                            db.execute(
                                text(
                                    "INSERT INTO flow_task_watermarks "
                                    "(flow_task_id, node_id, watermark_column, "
                                    "watermark_value, created_at, updated_at) "
                                    "VALUES (:ft_id, :nid, :col, :val, :now, :now)"
                                ),
                                {
                                    "ft_id": flow_task_id,
                                    "nid": node_id,
                                    "col": watermark_col,
                                    "val": str(max_val),
                                    "now": now,
                                },
                            )

                    logger.info(
                        f"Updated watermark for node {node_id}: "
                        f"{watermark_col}={max_val}"
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to update watermark for node {node_id}: {e}"
                )

    except Exception as e:
        logger.warning(f"Failed to update watermarks after execution: {e}")


def _notify_flow_task_error(
    flow_task_id: int,
    error_msg: str,
    node_id: Optional[str] = None,
) -> None:
    """Upsert an ERROR notification into notification_log for a flow task failure.

    Uses the same upsert logic as compute/core/notification.py:
    - If the key exists and iteration_check < limit → UPDATE (increment iteration)
    - Otherwise → INSERT a new row
    """
    try:
        from app.core.database import get_db_session
        from sqlalchemy import text

        key = (
            f"flow_task_error_{flow_task_id}_{node_id}"
            if node_id
            else f"flow_task_error_{flow_task_id}"
        )
        title = (
            f"Flow Task {flow_task_id} Node Failed"
            if node_id
            else f"Flow Task {flow_task_id} Failed"
        )
        message = error_msg[:2000]  # guard against excessively long messages
        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        with get_db_session() as db:
            # Fetch iteration limit from settings (default 3)
            # Column names are config_key / config_value (matches rosetta_setting_configuration DDL)
            limit_row = db.execute(
                text(
                    "SELECT config_value FROM rosetta_setting_configuration "
                    "WHERE config_key = 'NOTIFICATION_ITERATION_DEFAULT' LIMIT 1"
                )
            ).fetchone()
            max_iter = int(limit_row.config_value) if limit_row else 3

            # Fetch latest regardless of is_deleted (matches backend upsert_notification_by_key)
            existing = db.execute(
                text(
                    "SELECT id, iteration_check FROM notification_log "
                    "WHERE key_notification = :key "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"key": key},
            ).fetchone()

            if existing and existing.iteration_check < max_iter:
                # Update: increment iteration, reset flags, mirror backend logic
                db.execute(
                    text("""
                        UPDATE notification_log
                        SET iteration_check = iteration_check + 1,
                            title           = :title,
                            message         = :message,
                            type            = 'ERROR',
                            is_read         = FALSE,
                            is_deleted      = FALSE,
                            is_sent         = FALSE,
                            updated_at      = :now
                        WHERE id = :id
                    """),
                    {"title": title, "message": message, "now": now, "id": existing.id},
                )
            else:
                db.execute(
                    text("""
                        INSERT INTO notification_log
                            (key_notification, title, message, type,
                             is_read, is_deleted, iteration_check,
                             is_sent, is_force_sent, created_at, updated_at)
                        VALUES
                            (:key, :title, :message, 'ERROR',
                             FALSE, FALSE, 1,
                             FALSE, FALSE, :now, :now)
                    """),
                    {"key": key, "title": title, "message": message, "now": now},
                )
    except Exception as e:
        logger.warning(f"Failed to write error notification for flow_task {flow_task_id}: {e}")


def _persist_run_results(
    run_history_id: int,
    flow_task_id: int,
    status: str,
    total_input_records: int,
    total_output_records: int,
    node_logs: List[dict],
    error_message: Optional[str] = None,
) -> None:
    """Persist run results back to the config database."""
    try:
        from app.core.database import get_db_session
        from sqlalchemy import text

        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        with get_db_session() as db:
            # Update run history record
            db.execute(
                text("""
                    UPDATE flow_task_run_history
                    SET status = :status,
                        finished_at = :finished_at,
                        total_input_records = :total_input_records,
                        total_output_records = :total_output_records,
                        error_message = :error_message,
                        updated_at = :now
                    WHERE id = :run_history_id
                """),
                {
                    "status": status,
                    "finished_at": now,
                    "total_input_records": total_input_records,
                    "total_output_records": total_output_records,
                    "error_message": error_message,
                    "now": now,
                    "run_history_id": run_history_id,
                },
            )

            # Insert per-node logs
            if node_logs:
                for nl in node_logs:
                    db.execute(
                        text("""
                            INSERT INTO flow_task_run_node_log
                                (run_history_id, flow_task_id, node_id, node_type, node_label,
                                 row_count_in, row_count_out, duration_ms, status, error_message,
                                 created_at, updated_at)
                            VALUES
                                (:run_history_id, :flow_task_id, :node_id, :node_type, :node_label,
                                 :row_count_in, :row_count_out, :duration_ms, :status, :error_message,
                                 :now, :now)
                        """),
                        {
                            "run_history_id": run_history_id,
                            "flow_task_id": flow_task_id,
                            "node_id": nl.get("node_id", ""),
                            "node_type": nl.get("node_type", "unknown"),
                            "node_label": nl.get("node_label"),
                            "row_count_in": nl.get("row_count_in", 0),
                            "row_count_out": nl.get("row_count_out", 0),
                            "duration_ms": nl.get("duration_ms"),
                            "status": nl.get("status", "SUCCESS"),
                            "error_message": nl.get("error_message"),
                            "now": now,
                        },
                    )

            # Update flow_tasks summary columns + reset status
            db.execute(
                text("""
                    UPDATE flow_tasks
                    SET status = :flow_task_status,
                        last_run_at = :last_run_at,
                        last_run_status = :last_run_status,
                        last_run_record_count = :last_run_record_count,
                        updated_at = :now
                    WHERE id = :flow_task_id
                """),
                {
                    "flow_task_status": status,  # SUCCESS or FAILED → resets from RUNNING
                    "last_run_at": now,
                    "last_run_status": status,
                    "last_run_record_count": total_output_records,
                    "now": now,
                    "flow_task_id": flow_task_id,
                },
            )

    except Exception as e:
        logger.error(f"Failed to persist run results: {e}", exc_info=True)
