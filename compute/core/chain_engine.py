"""
Chain Pipeline Engine — consumes CDC data from Redis Streams (Rosetta Chain).

Unlike PipelineEngine which uses Debezium for CDC, ChainPipelineEngine reads
records from Redis Streams that were ingested via Arrow IPC from a remote
Rosetta instance, then routes them to configured destinations.
"""

import json
import logging
import time
from typing import Any, Optional

import redis

from config.config import get_config
from core.models import Pipeline, DestinationType
from core.repository import (
    PipelineRepository,
    PipelineMetadataRepository,
    DataFlowRepository,
)
from core.exceptions import PipelineException
from core.error_sanitizer import sanitize_for_db, sanitize_for_log
from core.dlq_manager import DLQManager
from core.dlq_recovery import DLQRecoveryWorker
from destinations.base import BaseDestination, CDCRecord
from destinations.snowflake import SnowflakeDestination
from destinations.postgresql import PostgreSQLDestination
from destinations.rosetta import RosettaDestination

logger = logging.getLogger(__name__)


class ChainPipelineEngine:
    """
    Pipeline engine for Rosetta Chain — reads from Redis Streams.

    Instead of Debezium CDC, this engine:
    1. Connects to Redis Streams populated by ChainIngestManager
    2. Reads records in consumer groups (XREADGROUP)
    3. Converts them to CDCRecords
    4. Routes to configured destinations (PostgreSQL, Snowflake, or another Rosetta)
    5. Acknowledges processed records (XACK)
    """

    def __init__(self, pipeline_id: int):
        self._pipeline_id = pipeline_id
        self._pipeline: Optional[Pipeline] = None
        self._destinations: dict[int, BaseDestination] = {}
        self._dlq_manager: Optional[DLQManager] = None
        self._dlq_recovery_worker: Optional[DLQRecoveryWorker] = None
        self._redis: Optional[redis.Redis] = None
        self._is_running = False
        self._logger = logging.getLogger(f"ChainEngine_{pipeline_id}")
        # Track which tables have been registered in rosetta_chain_tables so we
        # avoid a DB write on every batch.  Resets when the engine restarts.
        self._registered_tables: set[str] = set()

    def _load_pipeline(self) -> Pipeline:
        """Load pipeline configuration from database."""
        pipeline = PipelineRepository.get_by_id(
            self._pipeline_id, include_relations=True
        )
        if pipeline is None:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} not found",
                {"pipeline_id": self._pipeline_id},
            )
        return pipeline

    def _create_destination(
        self, destination_type: str, config: Any, source_config: Optional[Any] = None
    ) -> BaseDestination:
        """Create destination instance based on type."""
        destination_type = (
            destination_type.strip() if destination_type else destination_type
        )
        if destination_type.upper() == DestinationType.SNOWFLAKE.value:
            cfg = get_config()
            timeout_config = {
                "connect_timeout": cfg.snowflake.connect_timeout,
                "read_timeout": cfg.snowflake.read_timeout,
                "write_timeout": cfg.snowflake.write_timeout,
                "pool_timeout": cfg.snowflake.pool_timeout,
                "batch_timeout_base": cfg.snowflake.batch_timeout_base,
                "batch_timeout_max": cfg.snowflake.batch_timeout_max,
            }
            return SnowflakeDestination(config, timeout_config=timeout_config)
        elif destination_type.upper() == DestinationType.POSTGRES.value:
            return PostgreSQLDestination(config, source_config=source_config)
        elif destination_type.upper() == DestinationType.ROSETTA.value:
            return RosettaDestination(config)
        else:
            raise PipelineException(
                f"Unsupported destination type: {destination_type}",
                {"destination_type": destination_type},
            )

    def _get_stream_keys(self) -> list[str]:
        """
        Get all Redis Stream keys for this pipeline's chain client or catalog table.
        """
        source_type = getattr(self._pipeline, "source_type", "POSTGRES")

        if source_type == "CATALOG_TABLE":
            # source_id holds the catalog_table.id
            table_id = self._pipeline.source_id
            if not table_id:
                raise PipelineException(
                    "Catalog Table pipeline requires source_id",
                    {"pipeline_id": self._pipeline_id},
                )

            from core.database import DatabaseSession

            with DatabaseSession(autocommit=True) as session:
                session.execute(
                    "SELECT stream_name FROM catalog_tables WHERE id = %(table_id)s",
                    {"table_id": table_id},
                )
                row = session.fetchone()
                if not row:
                    raise PipelineException(f"Catalog table {table_id} not found")
                return [row["stream_name"]]

        # ROSETTA chain client logic
        config = get_config()
        prefix = config.chain.redis_stream_prefix
        chain_client_id = self._pipeline.chain_client_id

        if chain_client_id:
            # Specific client: only read streams for that chain client
            # Key format: {prefix}:{chain_client_id}:{table_name}
            pattern = f"{prefix}:{chain_client_id}:*"
        else:
            # Self-stream mode: no specific client, read ALL ingested chain streams.
            # This lets the local compute consume data it ingested itself via Arrow IPC
            # without needing an explicit chain_client_id.
            pattern = f"{prefix}:*"

        keys = []
        cursor = 0
        while True:
            cursor, batch = self._redis.scan(cursor, match=pattern, count=100)
            keys.extend([k.decode() if isinstance(k, bytes) else k for k in batch])
            if cursor == 0:
                break

        return keys

    def initialize(self) -> None:
        """
        Initialize chain pipeline engine.

        Loads config, creates destinations, connects to Redis.
        """
        self._pipeline = self._load_pipeline()
        config = get_config()

        # Connect to Redis
        self._redis = redis.Redis.from_url(
            config.dlq.redis_url,
            decode_responses=False,
        )

        # Create and initialize destinations
        successful = 0
        failed = 0

        for pd in self._pipeline.destinations:
            if not pd.destination:
                continue

            try:
                dest = self._create_destination(
                    pd.destination.type,
                    pd.destination,
                )

                try:
                    dest.initialize()
                    successful += 1

                    from core.repository import PipelineDestinationRepository

                    if pd.is_error:
                        PipelineDestinationRepository.update_error(pd.id, False)

                except Exception as init_error:
                    self._logger.warning(
                        f"Failed to init destination {pd.destination.name}: "
                        f"{sanitize_for_log(init_error)}"
                    )
                    failed += 1

                    from core.repository import PipelineDestinationRepository

                    db_error_msg = sanitize_for_db(
                        init_error, pd.destination.name, pd.destination.type
                    )
                    PipelineDestinationRepository.update_error(
                        pd.id, True, db_error_msg
                    )

                self._destinations[pd.destination_id] = dest

            except Exception as e:
                self._logger.error(
                    f"Failed to create destination {pd.destination.name}: "
                    f"{sanitize_for_log(e)}"
                )
                failed += 1

        self._logger.info(
            f"Chain pipeline {self._pipeline.name}: "
            f"{successful} dest(s) ready, {failed} failed"
        )

        # Initialize DLQ manager
        self._dlq_manager = DLQManager(
            redis_url=config.dlq.redis_url,
            key_prefix=config.dlq.key_prefix,
            max_stream_length=config.dlq.max_stream_length,
            consumer_group=config.dlq.consumer_group,
        )

    def _ensure_consumer_group(self, stream_key: str, group_name: str) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            self._redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _stream_entry_to_cdc_record(
        self, entry_id: bytes, fields: dict, stream_key: str
    ) -> CDCRecord:
        """
        Convert a Redis Stream entry back to a CDCRecord.

        ChainIngestManager._batch_to_records() stores entries as:
            b"operation"  → CDC op string (c/u/d/r)
            b"table_name" → table name
            b"key"        → JSON-encoded primary key dict
            b"value"      → JSON-encoded dict of ALL column values
            b"schema"     → JSON-encoded schema (usually {})
        """

        def _decode(v):
            return v.decode() if isinstance(v, bytes) else v

        operation = _decode(fields.get(b"operation", b"c"))
        table_name = _decode(fields.get(b"table_name", b""))

        # Key is stored as a JSON string under b"key" (not b"key_json")
        key_raw = _decode(fields.get(b"key", b"{}"))
        try:
            key = json.loads(key_raw)
        except (json.JSONDecodeError, TypeError):
            key = {}

        # All column values are stored as a single JSON blob under b"value"
        value_raw = _decode(fields.get(b"value", b"{}"))
        try:
            value = json.loads(value_raw)
            if not isinstance(value, dict):
                value = {}
        except (json.JSONDecodeError, TypeError):
            value = {}

        timestamp_raw = fields.get(b"timestamp")
        timestamp = int(_decode(timestamp_raw)) if timestamp_raw else None

        record = CDCRecord(
            operation=operation,
            table_name=table_name or self._extract_table_from_key(stream_key),
            key=key,
            value=value,
            timestamp=timestamp,
        )
        self._logger.debug(
            f"Parsed CDCRecord: op={record.operation} table={record.table_name} "
            f"key_cols={list(key.keys())} value_cols={list(value.keys())}"
        )
        return record

    def _extract_table_from_key(self, stream_key: str) -> str:
        """Extract table name from stream key pattern: prefix:chain_id:table_name."""
        parts = stream_key.split(":")
        return parts[-1] if parts else "unknown"

    # ------------------------------------------------------------------
    # Auto-table-creation for ROSETTA chain pipelines
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_pg_type(value) -> str:
        """Infer a PostgreSQL column type from a Python value."""
        if value is None:
            return "TEXT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        return "TEXT"

    def _auto_create_chain_table(
        self,
        dest,
        table_name: str,
        records: list[CDCRecord],
    ) -> bool:
        """
        Auto-create a destination table inferred from CDCRecord column values.

        The table is created via DuckDB's postgres ATTACH (not via a separate
        psycopg2 connection) so DuckDB sees the table immediately in its schema
        cache and subsequent DELETE/INSERT statements don't fail with
        "table not found" errors.

        Falls back to psycopg2 DDL if DuckDB creation fails.

        Returns True on success (created or already exists), False on failure.
        """
        from destinations.postgresql import PostgreSQLDestination

        if not isinstance(dest, PostgreSQLDestination):
            return False

        # Ensure destination connection is healthy before any DDL
        try:
            dest.initialize()
        except Exception as e:
            self._logger.error(
                f"Cannot auto-create chain table '{table_name}': "
                f"destination not ready: {e}"
            )
            return False

        # Check if table already exists — avoid unnecessary DDL
        try:
            existing = dest._get_table_schema(table_name)
            if existing:
                return False  # Table already exists
        except Exception:
            pass  # Proceed to attempt creation

        # Collect all unique columns across the batch
        all_columns: dict[str, str] = {}  # col → pg_type
        pk_columns: list[str] = []

        # Determine primary key columns from the first record that has key info
        for record in records:
            if record.key:
                pk_columns = list(record.key.keys())
                break

        # Infer column types from all record values
        for record in records:
            for col, val in record.value.items():
                if col not in all_columns:
                    all_columns[col] = self._infer_pg_type(val)

        if not all_columns:
            self._logger.warning(
                f"Cannot auto-create chain table '{table_name}': "
                f"no columns found in first batch of records."
            )
            return False

        schema = getattr(dest, "schema", "public")

        # Build column definition list
        col_defs: list[str] = []
        single_pk = pk_columns[0] if len(pk_columns) == 1 else None
        for col, pg_type in all_columns.items():
            if col == single_pk:
                col_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
            else:
                col_defs.append(f'"{col}" {pg_type}')

        if len(pk_columns) > 1:
            pk_str = ", ".join(f'"{c}"' for c in pk_columns)
            col_defs.append(f"PRIMARY KEY ({pk_str})")

        col_def_str = ", ".join(col_defs)

        # ── Primary path: create via DuckDB ATTACH ───────────────────────────
        # This ensures DuckDB's schema cache is consistent — if we create the
        # table with a separate psycopg2 connection DuckDB won't see it and the
        # subsequent DELETE/INSERT via DuckDB will fail with "table not found".
        full_table = f"{dest.duckdb_alias}.{schema}.{table_name}"
        try:
            dest._duckdb_conn.execute(
                f"CREATE TABLE IF NOT EXISTS {full_table} ({col_def_str})"
            )
            self._logger.info(
                f"Auto-created chain table '{schema}.{table_name}' via DuckDB "
                f"with columns {list(all_columns.keys())} "
                f"and primary key {pk_columns or 'none'}."
            )
            return True
        except Exception as duckdb_err:
            self._logger.warning(
                f"DuckDB DDL failed for chain table '{table_name}': {duckdb_err}. "
                f"Falling back to psycopg2."
            )

        # ── Fallback: create via psycopg2 direct connection ──────────────────
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({col_def_str})'
        )
        try:
            with dest._pg_conn.cursor() as cur:
                cur.execute(create_sql)
            # autocommit=True — no explicit commit needed
            self._logger.info(
                f"Auto-created chain table '{schema}.{table_name}' via psycopg2 "
                f"with columns {list(all_columns.keys())} "
                f"and primary key {pk_columns or 'none'}."
            )
            return True
        except Exception as e:
            self._logger.error(f"Failed to auto-create chain table '{table_name}': {e}")
            return False

    def _update_monitoring(
        self,
        pd,
        table_sync,
        table_name: str,
        count: int,
    ) -> None:
        """
        Record data flow count in data_flow_record_monitoring.

        Args:
            pd: PipelineDestination object
            table_sync: PipelineDestinationTableSync object
            table_name: Target table name
            count: Number of records written
        """
        try:
            DataFlowRepository.increment_count(
                pipeline_id=self._pipeline_id,
                pipeline_destination_id=pd.id,
                source_id=self._pipeline.source_id,
                table_sync_id=table_sync.id,
                table_name=table_name,
                count=count,
            )
        except Exception as e:
            self._logger.warning(f"Failed to update data flow monitoring: {e}")

    def _write_to_destinations(self, records: list[CDCRecord]) -> None:
        """
        Route CDC records to all configured destinations.

        On failure, sends records to DLQ for later retry.
        """
        if not records:
            return

        # Group records by table
        by_table: dict[str, list[CDCRecord]] = {}
        for r in records:
            by_table.setdefault(r.table_name, []).append(r)

        is_rosetta_source = (
            getattr(self._pipeline, "source_type", "POSTGRES") == "ROSETTA"
        )

        for table_name, table_records in by_table.items():
            for dest_id, dest in self._destinations.items():
                table_sync = None
                try:
                    # Find table_sync config for this destination/table
                    table_sync = self._find_table_sync(dest_id, table_name)
                    if not table_sync:
                        configured_tables = [
                            ts.table_name
                            for pd in self._pipeline.destinations
                            if pd.destination_id == dest_id
                            for ts in pd.table_syncs
                        ]
                        self._logger.warning(
                            f"No table sync config for table '{table_name}' → dest {dest_id}. "
                            f"Configured tables: {configured_tables or 'none'}. "
                            f"Add this table in the pipeline's table sync configuration."
                        )
                        continue

                    # For ROSETTA chain pipelines, ensure destination table exists
                    # before trying to MERGE; auto-create it from record schema if needed.
                    # IMPORTANT: table creation goes via DuckDB ATTACH, not psycopg2,
                    # so DuckDB's schema cache sees the new table immediately.
                    if is_rosetta_source:
                        created = self._auto_create_chain_table(
                            dest, table_sync.table_name_target, table_records
                        )
                        if created:
                            self._logger.info(
                                f"Chain table '{table_sync.table_name_target}' "
                                f"auto-created for dest {dest_id}."
                            )

                    self._logger.debug(
                        f"Writing {len(table_records)} record(s) for "
                        f"'{table_name}' → dest {dest_id} "
                        f"(target: '{table_sync.table_name_target}')"
                    )
                    written = dest.write_batch(table_records, table_sync)
                    self._logger.info(
                        f"Wrote {len(table_records)} record(s) for "
                        f"'{table_name}' → dest {dest_id} OK."
                    )

                    # Track in data flow monitoring
                    written_count = written if isinstance(written, int) else len(table_records)
                    pd_obj = next(
                        (pd for pd in self._pipeline.destinations if pd.destination_id == dest_id),
                        None,
                    )
                    if pd_obj and written_count > 0:
                        self._update_monitoring(
                            pd=pd_obj,
                            table_sync=table_sync,
                            table_name=table_sync.table_name_target,
                            count=written_count,
                        )

                    # Auto-register the table in rosetta_chain_tables so it
                    # appears in the Data Explorer.  Only done once per table
                    # per engine lifetime to avoid repeated DB writes.
                    if table_name not in self._registered_tables:
                        self._register_chain_table(table_name, table_records)
                except Exception as e:
                    self._logger.error(
                        f"Failed to write {len(table_records)} record(s) for "
                        f"'{table_name}' → dest {dest_id}: {sanitize_for_log(e)}",
                        exc_info=True,
                    )
                    # Send to DLQ for later retry
                    if self._dlq_manager and table_sync:
                        for record in table_records:
                            try:
                                self._dlq_manager.enqueue(
                                    pipeline_id=self._pipeline_id,
                                    source_id=self._pipeline.source_id or 0,
                                    destination_id=dest_id,
                                    table_name=table_name,
                                    table_name_target=table_sync.table_name_target,
                                    cdc_record=record,
                                    table_sync=table_sync,
                                    error_message=str(e),
                                )
                            except Exception as dlq_err:
                                self._logger.error(f"DLQ write failed: {dlq_err}")

    def _find_table_sync(self, dest_id: int, table_name: str):
        """Find PipelineDestinationTableSync for a destination and table.

        For ROSETTA chain source pipelines, if no explicit table_sync config
        exists for the incoming table, an in-memory passthrough table_sync is
        automatically created so data flows without requiring up-front table
        configuration (zero-config chain streaming).
        """
        from core.models import PipelineDestinationTableSync as PDTS

        for pd in self._pipeline.destinations:
            if pd.destination_id == dest_id:
                # Exact match — always preferred
                for ts in pd.table_syncs:
                    if ts.table_name == table_name:
                        return ts

                # No explicit config found — auto-passthrough for ROSETTA source
                source_type = getattr(self._pipeline, "source_type", "POSTGRES")
                if source_type == "ROSETTA":
                    configured = [ts.table_name for ts in pd.table_syncs]
                    self._logger.info(
                        f"No explicit table_sync for '{table_name}' on dest {dest_id}. "
                        f"Auto-passthrough enabled (configured: {configured or 'none'}). "
                        f"Routing '{table_name}' → '{table_name}' in destination."
                    )
                    return PDTS(
                        id=0,
                        pipeline_destination_id=pd.id,
                        table_name=table_name,
                        table_name_target=table_name,
                    )

        return None

    def _register_chain_table(self, table_name: str, records: list[CDCRecord]) -> None:
        """
        Register a chain table in rosetta_chain_tables so it appears in the
        Data Explorer.  Called at most once per table per engine lifetime.

        Infers a minimal column schema from the first record's values so the
        Data Explorer can show column information without requiring the remote
        sender to push a Debezium schema.
        """
        try:
            from chain.schema import ChainSchemaManager

            # Build a minimal schema from the records' value keys
            sample = records[0].value if records else {}
            schema_json: dict = {}
            for col, val in sample.items():
                if isinstance(val, bool):
                    pg_type = "boolean"
                elif isinstance(val, int):
                    pg_type = "bigint"
                elif isinstance(val, float):
                    pg_type = "double precision"
                else:
                    pg_type = "text"
                schema_json[col] = {"type": pg_type}

            mgr = ChainSchemaManager()
            chain_client_id = self._pipeline.chain_client_id
            mgr.upsert_table_schema(
                table_name=table_name,
                schema_json=schema_json,
                chain_client_id=chain_client_id,
                source_chain_id=str(chain_client_id) if chain_client_id else None,
            )
            self._registered_tables.add(table_name)
            self._logger.debug(
                f"Registered chain table '{table_name}' in Data Explorer "
                f"(client {chain_client_id})"
            )
        except Exception as e:
            # Non-fatal — pipeline continues even if registration fails
            self._logger.warning(
                f"Could not register chain table '{table_name}' in Data Explorer: {e}"
            )

    def run(self) -> None:
        """
        Main loop: read from Redis Streams and write to destinations.

        Uses XREADGROUP for consumer-group-based consumption with
        automatic acknowledgement after successful processing.
        """
        if self._pipeline is None:
            self.initialize()

        config = get_config()
        chain_config = config.chain
        group_name = chain_config.consumer_group
        consumer_name = f"pipeline_{self._pipeline_id}"
        batch_size = chain_config.batch_size
        block_ms = chain_config.block_ms

        PipelineMetadataRepository.upsert(self._pipeline_id, "RUNNING")
        self._logger.info(f"Starting chain pipeline {self._pipeline.name}")
        self._is_running = True

        # Start DLQ recovery worker
        if self._dlq_manager:
            dlq_config = config.dlq
            self._dlq_recovery_worker = DLQRecoveryWorker(
                pipeline=self._pipeline,
                destinations=self._destinations,
                dlq_manager=self._dlq_manager,
                check_interval=dlq_config.get("check_interval", 30),
                batch_size=dlq_config.get("batch_size", 100),
                max_retry_count=dlq_config.max_retry_count,
                max_age_days=dlq_config.max_age_days,
            )
            self._dlq_recovery_worker.start()

        try:
            while self._is_running:
                # Discover stream keys
                stream_keys = self._get_stream_keys()
                if not stream_keys:
                    time.sleep(2)
                    continue

                # Ensure consumer groups exist
                for sk in stream_keys:
                    self._ensure_consumer_group(sk, group_name)

                # XREADGROUP from all streams
                streams_dict = {sk: ">" for sk in stream_keys}
                try:
                    results = self._redis.xreadgroup(
                        groupname=group_name,
                        consumername=consumer_name,
                        streams=streams_dict,
                        count=batch_size,
                        block=block_ms,
                    )
                except redis.ConnectionError as e:
                    self._logger.error(f"Redis connection error: {e}")
                    time.sleep(5)
                    continue

                if not results:
                    continue

                # Process each stream's entries
                for stream_key_bytes, entries in results:
                    stream_key = (
                        stream_key_bytes.decode()
                        if isinstance(stream_key_bytes, bytes)
                        else stream_key_bytes
                    )

                    records = []
                    entry_ids = []

                    for entry_id, fields in entries:
                        try:
                            record = self._stream_entry_to_cdc_record(
                                entry_id, fields, stream_key
                            )
                            records.append(record)
                            entry_ids.append(entry_id)
                        except Exception as e:
                            self._logger.error(
                                f"Failed to parse stream entry {entry_id} "
                                f"from {stream_key}: {e}",
                                exc_info=True,
                            )

                    if records:
                        self._logger.debug(
                            f"Processing {len(records)} record(s) from "
                            f"stream '{stream_key}' → "
                            f"tables: {list({r.table_name for r in records})}"
                        )
                        self._write_to_destinations(records)

                        # Acknowledge processed entries then delete them.
                        # NOTE: XACK + XDEL run regardless of whether write_batch
                        # succeeded or sent records to DLQ, so entries are never
                        # re-delivered from the stream.  DLQ recovery handles retries.
                        if entry_ids:
                            try:
                                self._redis.xack(stream_key, group_name, *entry_ids)
                                self._logger.debug(
                                    f"XACK {len(entry_ids)} entries from {stream_key}"
                                )
                            except Exception as e:
                                self._logger.error(f"XACK failed for {stream_key}: {e}")

                            try:
                                self._redis.xdel(stream_key, *entry_ids)
                            except Exception as e:
                                self._logger.warning(
                                    f"XDEL failed for {stream_key} "
                                    f"({len(entry_ids)} entries): {e}"
                                )

        except Exception as e:
            self._logger.error(
                f"Chain pipeline {self._pipeline.name} crashed: "
                f"{sanitize_for_log(e)}"
            )
            db_msg = sanitize_for_db(e, self._pipeline.name, "CHAIN_PIPELINE")
            PipelineMetadataRepository.upsert(self._pipeline_id, "ERROR", db_msg)
            raise
        finally:
            self._is_running = False

    def stop(self) -> None:
        """Stop the chain pipeline engine."""
        self._is_running = False

        # Stop DLQ recovery worker
        if self._dlq_recovery_worker:
            try:
                self._dlq_recovery_worker.stop()
            except Exception as e:
                self._logger.warning(f"Error stopping DLQ recovery: {e}")
            self._dlq_recovery_worker = None

        # Close destinations
        for dest in self._destinations.values():
            try:
                dest.close()
            except Exception as e:
                self._logger.warning(f"Error closing destination: {e}")
        self._destinations.clear()

        # Close DLQ manager
        if self._dlq_manager:
            try:
                self._dlq_manager.close_all()
            except Exception as e:
                self._logger.warning(f"Error closing DLQ manager: {e}")
            self._dlq_manager = None

        # Close Redis
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass
            self._redis = None

        # Update metadata
        if self._pipeline:
            PipelineMetadataRepository.upsert(self._pipeline_id, "PAUSED")

        self._logger.info(f"Chain pipeline {self._pipeline_id} stopped")

    @property
    def is_running(self) -> bool:
        return self._is_running
