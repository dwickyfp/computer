"""
Preview query executor.

Ported from backend's PipelineService.preview_custom_sql() to run
as an isolated Celery task. Connects to source/destination DBs via
DuckDB's Postgres extension.
"""

import hashlib
import json
import re
import threading
import time as _time_mod
from typing import Any

import duckdb

from app.config.settings import get_settings
from app.core.database import get_db_session
from app.core.exceptions import WorkerConnectionError, PreviewExecutionError, ValidationError
from app.core.redis_client import get_redis
from app.core.security import decrypt_value
from app.tasks.preview.serializer import (
    extract_column_types,
    serialize_error,
    serialize_preview_result,
)
from app.tasks.preview.validator import validate_preview_sql

import structlog

logger = structlog.get_logger(__name__)

# ─── orjson for fast JSON serialization ────────────────────────────────────────
try:
    import orjson

    def _json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

    def _json_loads(s: str | bytes) -> Any:
        return orjson.loads(s)
except ImportError:
    _json_dumps = json.dumps  # type: ignore[assignment]
    _json_loads = json.loads  # type: ignore[assignment]


# ─── Connection config TTL cache ──────────────────────────────────────────────
_conn_cache: dict[tuple[int, int], tuple[float, tuple[dict, dict]]] = {}
_conn_cache_lock = threading.Lock()
_CONN_CACHE_TTL = 60.0  # seconds
_CONN_CACHE_MAX_SIZE = 64  # prevent unbounded growth


def execute_preview(
    sql: str | None,
    source_id: int,
    destination_id: int,
    table_name: str,
    filter_sql: str | None = None,
    include_profiling: bool = False,
) -> dict[str, Any]:
    """
    Execute a preview query using DuckDB with attached Postgres databases.

    Flow:
    1. Check Redis cache (auto-regenerates if custom SQL or filter changes)
    2. Fetch source/destination config from config DB
    3. Build query (with optional filter + custom SQL via CTE)
    4. Execute in DuckDB with Postgres extension
    5. Serialize and cache results (5 min TTL)
    6. Optionally compute data profiling stats (D7)

    Cache Invalidation:
    - Cache key includes: custom SQL, filter SQL, source_id, dest_id, table_name
    - Any change to custom SQL or filter will result in cache miss → regenerates data
    - Cache TTL: 300 seconds (5 minutes)

    Args:
        sql: Optional custom SQL query
        source_id: Source database ID
        destination_id: Destination database ID
        table_name: Table name to preview
        filter_sql: Optional filter SQL (v2 JSON or legacy format)
        include_profiling: If True, compute and return column profiling stats (D7)

    Returns:
        Dict with columns, column_types, data, error keys (+ profile if requested)
    """
    settings = get_settings()

    try:
        # 0. Validate SQL
        if sql:
            validate_preview_sql(sql)

        # 1. Compute cache hash - include all parameters that affect the query result
        # This ensures cache regeneration when custom SQL or filter changes
        filter_str = filter_sql or ""
        sql_str = sql or ""
        # Use structured format with delimiters to avoid hash collisions
        cache_components = [
            f"sql:{sql_str}",
            f"source:{source_id}",
            f"dest:{destination_id}",
            f"table:{table_name}",
            f"filter:{filter_str}",
        ]
        input_string = "|".join(cache_components)
        query_hash = hashlib.sha256(input_string.encode()).hexdigest()
        cache_key = f"preview:{query_hash}"

        logger.info(
            "Preview cache key computed",
            cache_key=cache_key[:16] + "...",
            has_custom_sql=bool(sql),
            has_filter=bool(filter_sql),
        )

        # 2. Check cache
        redis_client = None
        try:
            redis_client = get_redis()
            if redis_client:
                cached = redis_client.get(cache_key)
                if cached:
                    logger.info(
                        "Preview cache hit - returning cached result",
                        cache_key=cache_key[:16] + "...",
                    )
                    return _json_loads(cached)
                else:
                    logger.info(
                        "Preview cache miss - will regenerate data",
                        cache_key=cache_key[:16] + "...",
                    )
        except Exception as e:
            logger.warning("Redis cache check failed", error=str(e))

        # 3. Get source & destination config from config DB
        source_config, dest_config = _fetch_connection_configs(
            source_id, destination_id
        )

        # 4. Build query
        sanitized_source_name = re.sub(
            r"[^a-zA-Z0-9_]", "_", source_config["name"].lower()
        )
        source_prefix = f"pg_src_{sanitized_source_name}"

        sanitized_dest_name = re.sub(r"[^a-zA-Z0-9_]", "_", dest_config["name"].lower())
        dest_prefix = f"pg_{sanitized_dest_name}"

        # Parse filter_sql into WHERE clause
        where_clause = ""
        if filter_sql:
            parsed_filter = _filter_sql_to_where_clause(filter_sql)
            if parsed_filter:
                where_clause = f" WHERE {parsed_filter}"

        row_limit = settings.preview_row_limit

        if sql:
            # Custom SQL mode: CTE + rewrite
            filtered_source_cte = f"SELECT * FROM {source_prefix}.{table_name}{where_clause} LIMIT {row_limit}"
            rewritten_sql = sql
            table_pattern = re.compile(
                rf'(?<![\.\w"]){re.escape(table_name)}(?![\.\w"])',
                re.IGNORECASE,
            )
            rewritten_sql = table_pattern.sub("filtered_source", rewritten_sql)
            rewritten_sql = rewritten_sql.strip().rstrip(";")

            final_query = (
                f"WITH filtered_source AS ({filtered_source_cte}) "
                f"SELECT * FROM ({rewritten_sql}) AS result_sql LIMIT {row_limit}"
            )
        else:
            # Direct table query
            base_query = f"SELECT * FROM {source_prefix}.{table_name}"
            final_query = f"{base_query}{where_clause} LIMIT {row_limit}"

        logger.info("Executing preview query", query=final_query)

        # 5. Execute in DuckDB (acquire concurrency slot)
        from app.core.concurrency import acquire_duckdb_slot, release_duckdb_slot
        acquire_duckdb_slot()
        con = None
        try:
            con = duckdb.connect(":memory:")
            # Configure DuckDB for performance
            con.execute(f"SET memory_limit='{settings.duckdb_memory_limit}'")
            con.execute(f"SET threads={getattr(settings, 'duckdb_threads', 4)}")
            # Extensions are pre-installed at worker startup — only LOAD here
            con.execute("LOAD postgres;")

            # Attach source
            try:
                con.execute(
                    f"ATTACH '{source_config['conn_str']}' AS {source_prefix} (TYPE postgres, READ_ONLY);"
                )
            except Exception as e:
                raise WorkerConnectionError(f"Could not connect to source database: {e}")

            # Attach destination (non-critical)
            try:
                con.execute(
                    f"ATTACH '{dest_config['conn_str']}' AS {dest_prefix} (TYPE postgres, READ_ONLY);"
                )
            except Exception as e:
                logger.warning("Failed to attach destination DB", error=str(e))

            # Execute query
            result = con.execute(final_query).fetch_arrow_table()
        finally:
            if con:
                con.close()
            release_duckdb_slot()

        # 6. Process results
        columns = result.column_names
        column_types = extract_column_types(result.schema)
        data = result.to_pylist()

        response = serialize_preview_result(columns, column_types, data)

        # 6b. Data profiling (D7) — compute column statistics if requested
        if include_profiling:
            try:
                from app.tasks.preview.profiler import profile_arrow_table
                response["profile"] = profile_arrow_table(result)
            except Exception as e:
                logger.warning("Data profiling failed", error=str(e))
                response["profile"] = []

        # 7. Cache result (5 minute TTL)
        try:
            if redis_client:
                redis_client.setex(cache_key, 300, _json_dumps(response))
                logger.info(
                    "Preview result cached successfully",
                    cache_key=cache_key[:16] + "...",
                    row_count=len(data),
                    ttl_seconds=300,
                )
        except Exception as e:
            logger.warning("Failed to cache preview result", error=str(e))

        return response

    except (ValidationError, WorkerConnectionError) as e:
        logger.warning("Preview validation/connection error", error=str(e))
        return serialize_error(str(e))
    except Exception as e:
        logger.error("Preview execution failed", error=str(e), exc_info=True)
        return serialize_error(str(e))


def _fetch_connection_configs(
    source_id: int, destination_id: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Fetch source and destination connection details from config DB.
    Results are cached for 60s to avoid repeated DB queries within the same
    worker process (connection details rarely change mid-run).

    Returns:
        Tuple of (source_config, dest_config) dicts with keys:
        name, conn_str
    """
    cache_key = (source_id, destination_id)
    now = _time_mod.monotonic()

    with _conn_cache_lock:
        entry = _conn_cache.get(cache_key)
        if entry and (now - entry[0]) < _CONN_CACHE_TTL:
            return entry[1]

    # Cache miss — fetch from DB
    result = _fetch_connection_configs_from_db(source_id, destination_id)

    with _conn_cache_lock:
        # Evict oldest entries if cache is full
        if len(_conn_cache) >= _CONN_CACHE_MAX_SIZE:
            sorted_keys = sorted(_conn_cache, key=lambda k: _conn_cache[k][0])
            for k in sorted_keys[: len(sorted_keys) // 2]:
                del _conn_cache[k]
        _conn_cache[cache_key] = (now, result)
    return result


def _fetch_connection_configs_from_db(
    source_id: int, destination_id: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Raw DB fetch for connection configs."""
    from sqlalchemy import text

    with get_db_session() as session:
        # Fetch source
        row = session.execute(
            text(
                "SELECT name, pg_host, pg_port, pg_database, pg_username, pg_password "
                "FROM sources WHERE id = :id"
            ),
            {"id": source_id},
        ).fetchone()

        if not row:
            raise WorkerConnectionError(f"Source {source_id} not found")

        src_pass = decrypt_value(row.pg_password) if row.pg_password else ""
        source_config = {
            "name": row.name,
            "conn_str": (
                f"postgresql://{row.pg_username}:{src_pass}"
                f"@{row.pg_host}:{row.pg_port}/{row.pg_database}"
            ),
        }

        # Fetch destination
        dest_row = session.execute(
            text("SELECT name, config FROM destinations WHERE id = :id"),
            {"id": destination_id},
        ).fetchone()

        if not dest_row:
            raise WorkerConnectionError(f"Destination {destination_id} not found")

        dest_cfg = dest_row.config
        if isinstance(dest_cfg, str):
            dest_cfg = json.loads(dest_cfg)

        dest_pass = (
            decrypt_value(dest_cfg.get("password", ""))
            if dest_cfg.get("password")
            else ""
        )
        dest_config = {
            "name": dest_row.name,
            "conn_str": (
                f"postgresql://{dest_cfg.get('user', '')}:{dest_pass}"
                f"@{dest_cfg.get('host', 'localhost')}:{dest_cfg.get('port', 5432)}/{dest_cfg.get('database', 'postgres')}"
            ),
        }

    return source_config, dest_config


def _filter_sql_to_where_clause(filter_sql: str) -> str:
    """
    Convert filter_sql (v2 JSON or legacy semicolon format) to SQL WHERE clause.

    Ported from backend's PipelineService._filter_sql_to_where_clause().

    Returns:
        SQL WHERE clause string (without WHERE keyword), or empty string.
    """
    if not filter_sql or not filter_sql.strip():
        return ""

    def condition_to_sql(c: dict) -> str:
        column = c.get("column", "")
        if not column:
            return ""
        # Sanitize column name to prevent injection
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_."]*$', column):
            return ""
        op = c.get("operator", "").upper()
        # Escape single quotes in values to prevent SQL injection
        value = c.get("value", "").replace("'", "''")
        value2 = c.get("value2", "").replace("'", "''")

        if op in ("IS NULL", "IS NOT NULL"):
            return f"{column} {op}"
        if not value and op != "IN":
            return ""
        if op == "BETWEEN" and value2:
            return f"{column} BETWEEN '{value}' AND '{value2}'"
        if op in ("LIKE", "ILIKE"):
            return f"{column} {op} '%{value}%'"
        if op == "IN":
            vals = [v.strip() for v in value.split(",") if v.strip()]
            if not vals:
                return ""
            quoted = ", ".join(
                v if re.match(r"^-?\d+(\.\d+)?$", v) else f"'{v}'" for v in vals
            )
            return f"{column} IN ({quoted})"
        is_num = bool(re.match(r"^-?\d+(\.\d+)?$", value))
        quoted_value = value if is_num else f"'{value}'"
        return f"{column} {c.get('operator', '=')} {quoted_value}"

    # Try V2 JSON format
    try:
        parsed = json.loads(filter_sql)
        if isinstance(parsed, dict) and parsed.get("version") == 2:
            group_sqls = []
            for g in parsed.get("groups", []):
                parts = [condition_to_sql(c) for c in g.get("conditions", [])]
                parts = [p for p in parts if p]
                if not parts:
                    continue
                intra = g.get("intraLogic", "AND")
                group_sqls.append(
                    f"({f' {intra} '.join(parts)})" if len(parts) > 1 else parts[0]
                )
            if not group_sqls:
                return ""
            result = group_sqls[0]
            inter_logic = parsed.get("interLogic", [])
            for i in range(1, len(group_sqls)):
                logic = inter_logic[i - 1] if i - 1 < len(inter_logic) else "AND"
                result += f" {logic} {group_sqls[i]}"
            return result
    except (json.JSONDecodeError, TypeError):
        pass

    # Legacy semicolon format
    parts = [s.strip() for s in filter_sql.split(";") if s.strip()]
    return " AND ".join(parts)
