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
import os
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
from core.engine import run_pipeline
from core.backfill_manager import BackfillManager
from server import run_server


def run_chain_stream_cleanup(stop_event: threading.Event) -> None:
    """
    Background thread: periodically trim chain Redis Streams.

    Removes entries older than CHAIN_STREAM_RETENTION_DAYS from every
    rosetta:chain:* stream so unconsumed data does not accumulate forever.
    """
    logger = logging.getLogger("chain_cleanup")
    try:
        config = get_config()
        if not config.chain.enabled:
            return

        from chain.ingest import ChainIngestManager

        manager = ChainIngestManager()
        interval = config.chain.trim_interval_seconds
        logger.info(
            f"Chain stream cleanup thread started "
            f"(retention={config.chain.stream_retention_days}d, "
            f"interval={interval}s)"
        )

        while not stop_event.is_set():
            try:
                manager.trim_all_streams()
            except Exception as e:
                logger.error(f"Chain cleanup iteration failed: {e}")
            stop_event.wait(interval)

    except Exception as e:
        logger.error(f"Chain cleanup thread exiting: {e}")


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


def setup_logging() -> None:
    """Configure logging based on environment and config."""
    config = get_config()

    # Check for DEBUG environment variable
    debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    level = logging.DEBUG if debug else getattr(logging, config.logging.level.upper())

    logging.basicConfig(
        level=level,
        format=config.logging.format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Reduce noise from some libraries
    logging.getLogger("jpype").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    """Main entry point."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Initialize connection pool with configurable size
    main_pool_max_conn = int(os.getenv("MAIN_POOL_MAX_CONN", "8"))
    init_connection_pool(min_conn=1, max_conn=main_pool_max_conn)

    config = get_config()

    manager = None
    backfill_manager = None
    chain_cleanup_stop = threading.Event()

    try:
        # Running Migration SQL
        run_migration(logger)

        # Start API Server in a separate thread
        server_thread = threading.Thread(
            target=run_server,
            args=(config.server.host, config.server.port),
            daemon=True,
        )
        server_thread.start()

        # Start Pipeline Manager in a separate thread
        manager = PipelineManager(register_signals=False)
        manager_thread = threading.Thread(target=manager.run, daemon=True)
        manager_thread.start()

        # Start Backfill Manager in a separate thread
        backfill_manager = BackfillManager(check_interval=5, batch_size=10000)
        backfill_manager.start()

        # Start chain stream cleanup thread (only active when CHAIN_ENABLED=true)
        chain_cleanup_thread = threading.Thread(
            target=run_chain_stream_cleanup,
            args=(chain_cleanup_stop,),
            daemon=True,
            name="chain_stream_cleanup",
        )
        chain_cleanup_thread.start()

        # Start Catalog Health Worker
        from core.catalog_health_worker import CatalogHealthWorker
        catalog_health_worker = CatalogHealthWorker(check_interval_seconds=60)
        catalog_health_thread = threading.Thread(
            target=catalog_health_worker.run,
            args=(chain_cleanup_stop,),
            daemon=True,
            name="catalog_health_worker",
        )
        catalog_health_thread.start()

        # Keep main thread alive to handle signals and facilitate clean shutdown
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        chain_cleanup_stop.set()

        # If manager exists, try to shutdown gracefully (if not already handled by its own signals)
        # Note: manager.run() handles signals, but if we are here via KeyboardInterrupt caught in main,
        # we might want to ensure manager stops.
        # Since threads are daemon, they will be killed when main exits,
        # but manager.shutdown() is cleaner if possible.
        if manager:
            manager.shutdown()

        if backfill_manager:
            backfill_manager.stop()

        close_connection_pool()


if __name__ == "__main__":
    sys.exit(main())
