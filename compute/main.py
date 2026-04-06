#!/usr/bin/env python3
"""
Rosetta Compute Engine - Main Entry Point

Modular Debezium-based CDC engine for data streaming.

Configuration via environment variables:
    PIPELINE_ID     - Optional: Run specific pipeline by ID
    DEBUG           - Enable debug logging (true/false)
    LOG_LEVEL       - Logging level (DEBUG, INFO, WARNING, ERROR)
"""

import logging
import json
import os
import re
import sys
import threading
import time


from config.config import get_config
from core.database import (
    init_connection_pool,
    close_connection_pool,
    get_db_connection,
    return_db_connection,
)
from core.manager import PipelineManager
from core.backfill_manager import BackfillManager
from core.runtime_health import mark_worker
from server import run_server


def run_migration(logger: logging.Logger) -> None:
    """Run database migration on startup."""
    # Robust path resolution: assuming 'migrations' is at project root, and this script is in 'compute/'
    # Or assuming CWD is project root.
    # Let's check typical CWD first, then relative to file.

    # Try project root (CWD)
    migration_path_cwd = os.path.join(os.getcwd(), "migrations", "001_create_table.sql")

    # Try relative to this file (compute/main.py -> ../migrations)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    migration_path_rel = os.path.join(base_dir, "migrations", "001_create_table.sql")

    if os.path.exists(migration_path_cwd):
        migration_file = migration_path_cwd
    elif os.path.exists(migration_path_rel):
        migration_file = migration_path_rel
    else:
        # User requested strict dependency: fail if missing
        error_msg = (
            f"Migration file not found at {migration_path_cwd} or {migration_path_rel}"
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    conn = None
    try:
        with open(migration_file, "r", encoding="utf-8") as f:
            sql_script = f.read()

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql_script)
            conn.commit()

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            return_db_connection(conn)


_LEGACY_FILTER_MIGRATION_ERROR = (
    "Legacy filter_sql could not be migrated to JSON v2. Edit the filter and save it again."
)


def _parse_legacy_filter_clause(clause: str) -> dict[str, str] | None:
    clause = clause.strip()
    if not clause:
        return None

    match = re.match(r"^([\w.]+)\s+(IS\s+(?:NOT\s+)?NULL)$", clause, re.IGNORECASE)
    if match:
        return {
            "column": match.group(1),
            "operator": match.group(2).upper(),
            "value": "",
        }

    match = re.match(
        r"^([\w.]+)\s+BETWEEN\s+'([^']*)'\s+AND\s+'([^']*)'$",
        clause,
        re.IGNORECASE,
    )
    if match:
        return {
            "column": match.group(1),
            "operator": "BETWEEN",
            "value": match.group(2),
            "value2": match.group(3),
        }

    match = re.match(r"^([\w.]+)\s+(LIKE|ILIKE)\s+'%([^%]*)%'$", clause, re.IGNORECASE)
    if match:
        return {
            "column": match.group(1),
            "operator": match.group(2).upper(),
            "value": match.group(3),
        }

    match = re.match(r"^([\w.]+)\s+IN\s*\((.*)\)$", clause, re.IGNORECASE)
    if match:
        raw_values = [value.strip() for value in match.group(2).split(",") if value.strip()]
        cleaned = [value[1:-1] if value.startswith("'") and value.endswith("'") else value for value in raw_values]
        return {
            "column": match.group(1),
            "operator": "IN",
            "value": ",".join(cleaned),
        }

    match = re.match(r"^([\w.]+)\s*(=|!=|>|>=|<=|<)\s*(.+)$", clause)
    if match:
        raw_value = match.group(3).strip()
        if raw_value.startswith("'") and raw_value.endswith("'"):
            raw_value = raw_value[1:-1]
        return {
            "column": match.group(1),
            "operator": match.group(2),
            "value": raw_value,
        }

    return None


def _legacy_filter_to_v2_json(filter_sql: str | None) -> str | None:
    if not filter_sql:
        return None

    try:
        parsed = json.loads(filter_sql)
        if isinstance(parsed, dict) and parsed.get("version") == 2:
            return filter_sql
    except (json.JSONDecodeError, TypeError):
        pass

    conditions: list[dict[str, str]] = []
    for clause in [part.strip() for part in filter_sql.split(";") if part.strip()]:
        parsed_clause = _parse_legacy_filter_clause(clause)
        if not parsed_clause:
            return None
        conditions.append(parsed_clause)

    if not conditions:
        return None

    return json.dumps(
        {
            "version": 2,
            "groups": [{"conditions": conditions, "intraLogic": "AND"}],
            "interLogic": [],
        }
    )


def migrate_filter_sql_formats(logger: logging.Logger) -> None:
    """Best-effort one-time migration from legacy semicolon filters to JSON v2."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, filter_sql
                FROM pipelines_destination_table_sync
                WHERE filter_sql IS NOT NULL
                """
            )
            for sync_id, filter_sql in cursor.fetchall():
                converted = _legacy_filter_to_v2_json(filter_sql)
                if converted and converted != filter_sql:
                    cursor.execute(
                        """
                        UPDATE pipelines_destination_table_sync
                        SET filter_sql = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (converted, sync_id),
                    )
                elif converted is None:
                    cursor.execute(
                        """
                        UPDATE pipelines_destination_table_sync
                        SET is_error = TRUE,
                            error_message = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (_LEGACY_FILTER_MIGRATION_ERROR, sync_id),
                    )

            cursor.execute(
                """
                SELECT id, status, filter_sql
                FROM queue_backfill_data
                WHERE filter_sql IS NOT NULL
                """
            )
            for job_id, status, filter_sql in cursor.fetchall():
                converted = _legacy_filter_to_v2_json(filter_sql)
                if converted and converted != filter_sql:
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET filter_sql = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (converted, job_id),
                    )
                elif converted is None:
                    if status in ("PENDING", "EXECUTING"):
                        cursor.execute(
                            """
                            UPDATE queue_backfill_data
                            SET status = 'FAILED',
                                is_error = TRUE,
                                error_message = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (_LEGACY_FILTER_MIGRATION_ERROR, job_id),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE queue_backfill_data
                            SET is_error = TRUE,
                                error_message = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (_LEGACY_FILTER_MIGRATION_ERROR, job_id),
                        )

            conn.commit()
    except Exception as exc:
        logger.error("Filter migration failed: %s", exc)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)


_LOG_FORMAT_ALIASES = {
    "text": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "json": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

_DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging() -> None:
    """Configure logging based on environment and config."""
    config = get_config()

    # Check for DEBUG environment variable
    debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    level = logging.DEBUG if debug else getattr(logging, config.logging.level.upper())

    # LOG_FORMAT may be a human-readable label ("text", "json") from Docker env vars
    # rather than a real Python %-style format string — map it to an actual format.
    raw_format = config.logging.format
    log_format = (
        _LOG_FORMAT_ALIASES.get(raw_format.lower(), raw_format)
        if raw_format
        else _DEFAULT_LOG_FORMAT
    )

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Reduce noise from some libraries
    logging.getLogger("jpype").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    """Main entry point."""
    # Use 'spawn' start method to avoid fork-related connection pool corruption.
    # fork() can inherit parent's DB connection file descriptors into children,
    # causing corruption. 'spawn' starts fresh child processes.
    import multiprocessing

    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass  # Already set (e.g., Windows defaults to spawn)

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Initialize connection pool with configurable size
    main_pool_max_conn = int(os.getenv("MAIN_POOL_MAX_CONN", "4"))
    init_connection_pool(min_conn=1, max_conn=main_pool_max_conn)

    config = get_config()

    manager = None
    backfill_manager = None
    shutdown_event = threading.Event()
    worker_threads: dict[str, threading.Thread] = {}

    def _start_worker(name: str, target, critical: bool = True) -> threading.Thread:
        def runner():
            mark_worker(name, "running", critical=critical)
            try:
                target()
                status = "stopped" if shutdown_event.is_set() else "failed"
                message = None if shutdown_event.is_set() else "worker exited unexpectedly"
                mark_worker(name, status, critical=critical, message=message)
            except Exception as exc:
                mark_worker(name, "failed", critical=critical, message=str(exc))
                raise

        thread = threading.Thread(target=runner, daemon=False, name=name)
        thread.start()
        worker_threads[name] = thread
        return thread

    try:
        # Running Migration SQL
        run_migration(logger)
        migrate_filter_sql_formats(logger)

        manager = PipelineManager(register_signals=False)
        backfill_manager = BackfillManager(check_interval=5, batch_size=10000)

        _start_worker(
            "api_server",
            lambda: run_server(config.server.host, config.server.port),
        )
        _start_worker("pipeline_manager", manager.run)
        _start_worker(
            "backfill_manager",
            lambda: (
                backfill_manager._recover_stale_jobs(),
                backfill_manager._monitor_queue(),
            ),
        )

        while not shutdown_event.is_set():
            for name, thread in worker_threads.items():
                if not thread.is_alive():
                    logger.error("Critical worker %s exited unexpectedly", name)
                    shutdown_event.set()
                    break
            time.sleep(1)

    except KeyboardInterrupt:
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        shutdown_event.set()

        if manager:
            manager.shutdown()

        if backfill_manager:
            backfill_manager.stop()

        for thread in worker_threads.values():
            if thread.is_alive():
                thread.join(timeout=5)

        try:
            from core.repository import DataFlowRepository

            DataFlowRepository.shutdown()
        except Exception:
            pass

        close_connection_pool()


if __name__ == "__main__":
    sys.exit(main())
