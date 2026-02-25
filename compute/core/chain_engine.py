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

        if not chain_client_id:
            raise PipelineException(
                "Chain pipeline requires chain_client_id",
                {"pipeline_id": self._pipeline_id},
            )

        # Pattern must match the key format used by ChainIngestManager.get_stream_key():
        # f"{prefix}:{chain_id}:{table_name}" → e.g. "rosetta:chain:3:tbl_xxx"
        pattern = f"{prefix}:{chain_client_id}:*"
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

        return CDCRecord(
            operation=operation,
            table_name=table_name or self._extract_table_from_key(stream_key),
            key=key,
            value=value,
            timestamp=timestamp,
        )

    def _extract_table_from_key(self, stream_key: str) -> str:
        """Extract table name from stream key pattern: prefix:chain_id:table_name."""
        parts = stream_key.split(":")
        return parts[-1] if parts else "unknown"

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

        for table_name, table_records in by_table.items():
            for dest_id, dest in self._destinations.items():
                try:
                    # Find table_sync config for this destination/table
                    table_sync = self._find_table_sync(dest_id, table_name)
                    if table_sync:
                        dest.write_batch(table_records, table_sync)
                    else:
                        self._logger.warning(
                            f"No table sync config for table '{table_name}' → dest {dest_id}. "
                            f"Add this table in the pipeline's table sync configuration."
                        )
                except Exception as e:
                    self._logger.error(
                        f"Failed to write {len(table_records)} records for "
                        f"{table_name} to dest {dest_id}: {sanitize_for_log(e)}"
                    )
                    # Send to DLQ
                    if self._dlq_manager:
                        for record in table_records:
                            try:
                                self._dlq_manager.add_to_dlq(
                                    source_id=self._pipeline.source_id or 0,
                                    table_name=table_name,
                                    destination_id=dest_id,
                                    record=record,
                                    error_message=str(e),
                                )
                            except Exception as dlq_err:
                                self._logger.error(f"DLQ write failed: {dlq_err}")

    def _find_table_sync(self, dest_id: int, table_name: str):
        """Find PipelineDestinationTableSync for a destination and table."""
        for pd in self._pipeline.destinations:
            if pd.destination_id == dest_id:
                for ts in pd.table_syncs:
                    if ts.table_name == table_name:
                        return ts
        return None

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
                            self._logger.error(f"Failed to parse stream entry: {e}")

                    if records:
                        self._write_to_destinations(records)

                        # Acknowledge processed entries then delete them
                        # XACK removes from PEL; XDEL removes from the stream body
                        # so consumed data does not linger in Redis.
                        if entry_ids:
                            try:
                                self._redis.xack(stream_key, group_name, *entry_ids)
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
