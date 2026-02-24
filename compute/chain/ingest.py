"""
Chain ingestion manager for receiving Arrow IPC data.

Receives Arrow IPC record batches from remote Rosetta instances,
deserializes them, and writes CDC records into Redis Streams.
"""

import json
import logging
import time
from io import BytesIO
from typing import Any, Optional

import pyarrow as pa
import redis

from config.config import get_config

logger = logging.getLogger(__name__)


class ChainIngestManager:
    """
    Manages ingestion of Arrow IPC data into Redis Streams.

    Each table from each chain source gets its own Redis Stream:
        rosetta:chain:{chain_id}:{table_name}
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        stream_prefix: Optional[str] = None,
        max_stream_length: Optional[int] = None,
    ):
        config = get_config()
        self._redis_url = redis_url or config.dlq.redis_url
        self._stream_prefix = stream_prefix or config.chain.redis_stream_prefix
        self._max_stream_length = max_stream_length or config.chain.max_stream_length
        self._retention_days = config.chain.stream_retention_days

        self._redis = redis.Redis.from_url(
            self._redis_url,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )

    def _retention_minid(self) -> str:
        """
        Compute the Redis Stream MINID corresponding to 'now minus retention_days'.

        Redis IDs are '{unix_ms}-{seq}', so using just the timestamp prefix is
        enough for XTRIM MINID to drop everything older than retention_days.
        """
        cutoff_ms = int((time.time() - self._retention_days * 86400) * 1000)
        return str(cutoff_ms)

    def get_stream_key(self, chain_id: str, table_name: str) -> str:
        """Get Redis Stream key for a chain table."""
        return f"{self._stream_prefix}:{chain_id}:{table_name}"

    def ingest_arrow_ipc(
        self,
        body: bytes,
        chain_id: str,
        table_name: str,
        operation_type: str = "c",
    ) -> int:
        """
        Receive Arrow IPC stream data and write to Redis Stream.

        Args:
            body: Raw Arrow IPC stream bytes
            chain_id: Identifier of the sending chain instance
            table_name: Target table name
            operation_type: CDC operation type (c/u/d/r)

        Returns:
            Number of records ingested
        """
        try:
            reader = pa.ipc.open_stream(BytesIO(body))
            schema = reader.schema
            record_count = 0
            stream_key = self.get_stream_key(chain_id, table_name)

            for batch in reader:
                records = self._batch_to_records(
                    batch, schema, table_name, operation_type
                )
                minid = self._retention_minid()
                for record in records:
                    self._redis.xadd(
                        stream_key,
                        record,
                        minid=minid,
                        approximate=True,
                    )
                    record_count += 1

            logger.info(
                f"Ingested {record_count} records for "
                f"chain={chain_id} table={table_name}"
            )
            return record_count

        except pa.ArrowInvalid as e:
            logger.error(f"Invalid Arrow IPC data: {e}")
            raise ValueError(f"Invalid Arrow IPC data: {e}")
        except Exception as e:
            logger.error(f"Failed to ingest Arrow IPC data: {e}")
            raise

    def ingest_json_records(
        self,
        records: list[dict[str, Any]],
        chain_id: str,
        table_name: str,
        operation_type: str = "c",
    ) -> int:
        """
        Ingest records from JSON format into Redis Stream.

        Fallback for when Arrow IPC is not used.
        """
        stream_key = self.get_stream_key(chain_id, table_name)
        count = 0
        minid = self._retention_minid()

        for record in records:
            entry = {
                b"operation": operation_type.encode(),
                b"table_name": table_name.encode(),
                b"key": json.dumps(record.get("key", {})).encode(),
                b"value": json.dumps(record.get("value", {})).encode(),
                b"schema": json.dumps(record.get("schema", {})).encode(),
                b"chain_id": chain_id.encode(),
            }
            self._redis.xadd(
                stream_key,
                entry,
                minid=minid,
                approximate=True,
            )
            count += 1

        return count

    def _batch_to_records(
        self,
        batch: pa.RecordBatch,
        schema: pa.Schema,
        table_name: str,
        default_operation: str,
    ) -> list[dict[bytes, bytes]]:
        """
        Convert an Arrow RecordBatch to Redis Stream entries.

        Looks for special columns __operation and __key_json for CDC metadata.
        All other columns are treated as value data.
        """
        records = []
        num_rows = batch.num_rows

        # Check for special metadata columns
        has_operation = "__operation" in schema.names
        has_key = "__key_json" in schema.names

        # Get value column names (exclude metadata columns)
        value_columns = [
            name for name in schema.names if name not in ("__operation", "__key_json")
        ]

        for i in range(num_rows):
            # Extract operation
            operation = default_operation
            if has_operation:
                op_val = batch.column("__operation")[i].as_py()
                if op_val:
                    operation = str(op_val)

            # Extract key
            key_data = {}
            if has_key:
                key_val = batch.column("__key_json")[i].as_py()
                if key_val:
                    try:
                        key_data = json.loads(key_val)
                    except (json.JSONDecodeError, TypeError):
                        key_data = {"_raw_key": key_val}

            # Extract value
            value_data = {}
            for col_name in value_columns:
                val = batch.column(col_name)[i].as_py()
                value_data[col_name] = val

            entry = {
                b"operation": operation.encode(),
                b"table_name": table_name.encode(),
                b"key": json.dumps(key_data).encode(),
                b"value": json.dumps(value_data, default=str).encode(),
                b"schema": b"{}",
            }
            records.append(entry)

        return records

    def get_stream_length(self, chain_id: str, table_name: str) -> int:
        """Get the number of entries in a chain stream."""
        stream_key = self.get_stream_key(chain_id, table_name)
        try:
            return self._redis.xlen(stream_key)
        except Exception:
            return 0

    def trim_all_streams(self) -> int:
        """
        Trim all chain streams to remove entries older than retention_days.

        Called periodically by a background thread to enforce time-based
        retention on streams that may not receive new writes for a while.

        Returns:
            Total number of entries trimmed across all streams.
        """
        minid = self._retention_minid()
        pattern = f"{self._stream_prefix}:*".encode()
        total_trimmed = 0

        try:
            cursor = 0
            while True:
                cursor, keys = self._redis.scan(cursor, match=pattern, count=200)
                for key in keys:
                    try:
                        before = self._redis.xlen(key)
                        self._redis.xtrim(key, minid=minid, approximate=True)
                        after = self._redis.xlen(key)
                        trimmed = before - after
                        if trimmed > 0:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            logger.debug(
                                f"Trimmed {trimmed} old entries from stream {key_str}"
                            )
                            total_trimmed += trimmed
                    except Exception as e:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        logger.warning(f"Failed to trim stream {key_str}: {e}")
                if cursor == 0:
                    break
        except Exception as e:
            logger.error(f"trim_all_streams failed: {e}")

        if total_trimmed > 0:
            logger.info(
                f"Chain stream cleanup: trimmed {total_trimmed} entries older than "
                f"{self._retention_days} day(s) (minid={minid})"
            )
        return total_trimmed

    def list_streams(self, chain_id: Optional[str] = None) -> list[str]:
        """List all chain streams, optionally filtered by chain_id."""
        pattern = f"{self._stream_prefix}:{chain_id or '*'}:*"
        keys = self._redis.keys(pattern)
        return [k.decode() if isinstance(k, bytes) else k for k in keys]

    def close(self) -> None:
        """Close Redis connection."""
        try:
            self._redis.close()
        except Exception:
            pass
