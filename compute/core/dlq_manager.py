"""
Dead Letter Queue (DLQ) Manager using Redis Streams.

Provides persistent message queuing for CDC records that fail to reach destinations.
Messages are organized by source_id, table_name, and destination_id using separate
Redis Streams with consumer groups for at-least-once delivery.
"""

import json
import logging
import re
import time
import threading
from typing import Optional, Any
from datetime import datetime, timezone, timedelta

import redis

from destinations.base import CDCRecord
from core.models import PipelineDestinationTableSync

logger = logging.getLogger(__name__)

# Retry constants for Redis operations
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 0.5  # seconds


class DLQMessage:
    """
    Dead Letter Queue message wrapper.

    Stores CDC record along with routing metadata for recovery.
    """

    def __init__(
        self,
        pipeline_id: int,
        source_id: int,
        destination_id: int,
        table_name: str,
        table_name_target: str,
        cdc_record: CDCRecord,
        table_sync_config: dict[str, Any],
        retry_count: int = 0,
        first_failed_at: Optional[str] = None,
    ):
        """
        Initialize DLQ message.

        Args:
            pipeline_id: Pipeline ID
            source_id: Source ID
            destination_id: Destination ID
            table_name: Source table name
            table_name_target: Target table name
            cdc_record: CDC record that failed
            table_sync_config: Serialized table sync configuration
            retry_count: Number of retry attempts
            first_failed_at: ISO timestamp when first failure occurred
        """
        self.pipeline_id = pipeline_id
        self.source_id = source_id
        self.destination_id = destination_id
        self.table_name = table_name
        self.table_name_target = table_name_target
        self.cdc_record = cdc_record
        self.table_sync_config = table_sync_config
        self.retry_count = retry_count
        self.first_failed_at = first_failed_at or datetime.now(timezone(timedelta(hours=7))).isoformat()

    def to_dict(self) -> dict[str, str]:
        """Serialize message to dict for Redis Stream XADD."""
        data = {
            "pipeline_id": self.pipeline_id,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "table_name": self.table_name,
            "table_name_target": self.table_name_target,
            "cdc_record": {
                "operation": self.cdc_record.operation,
                "table_name": self.cdc_record.table_name,
                "key": self.cdc_record.key,
                "value": self.cdc_record.value,
                "schema": self.cdc_record.schema,
                "timestamp": self.cdc_record.timestamp,
            },
            "table_sync_config": self.table_sync_config,
            "retry_count": self.retry_count,
            "first_failed_at": self.first_failed_at,
        }
        # Store as single JSON field for atomicity and simplicity
        return {"data": json.dumps(data)}

    @classmethod
    def from_stream_entry(cls, entry_data: dict) -> "DLQMessage":
        """
        Deserialize message from Redis Stream entry data.

        Args:
            entry_data: Dict from XREADGROUP / XRANGE, with 'data' key containing JSON

        Returns:
            Deserialized DLQMessage
        """
        raw = entry_data.get(b"data") or entry_data.get("data")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        obj = json.loads(raw)
        cdc_record = CDCRecord(**obj["cdc_record"])
        return cls(
            pipeline_id=obj["pipeline_id"],
            source_id=obj["source_id"],
            destination_id=obj["destination_id"],
            table_name=obj["table_name"],
            table_name_target=obj["table_name_target"],
            cdc_record=cdc_record,
            table_sync_config=obj["table_sync_config"],
            retry_count=obj["retry_count"],
            first_failed_at=obj["first_failed_at"],
        )

    # Keep bytes serialization for backward compatibility with DLQMessage interface
    def to_bytes(self) -> bytes:
        """Serialize message to bytes (JSON)."""
        data_str = self.to_dict()["data"]
        return data_str.encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "DLQMessage":
        """Deserialize message from bytes."""
        obj = json.loads(data.decode("utf-8"))
        cdc_record = CDCRecord(**obj["cdc_record"])
        return cls(
            pipeline_id=obj["pipeline_id"],
            source_id=obj["source_id"],
            destination_id=obj["destination_id"],
            table_name=obj["table_name"],
            table_name_target=obj["table_name_target"],
            cdc_record=cdc_record,
            table_sync_config=obj["table_sync_config"],
            retry_count=obj["retry_count"],
            first_failed_at=obj["first_failed_at"],
        )


class DLQManager:
    """
    Manages dead letter queues for CDC records using Redis Streams.

    Uses Redis Streams with consumer groups for at-least-once delivery.
    Each queue is a separate stream keyed by: {prefix}:s{source_id}:t{table_name}:d{dest_id}
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "rosetta:dlq",
        max_stream_length: int = 100000,
        consumer_group: str = "dlq_recovery",
    ):
        """
        Initialize DLQ manager with Redis connection.

        Args:
            redis_url: Redis connection URL
            key_prefix: Prefix for all DLQ stream keys
            max_stream_length: Maximum entries per stream (MAXLEN cap)
            consumer_group: Consumer group name for recovery workers
        """
        self._redis = redis.Redis.from_url(
            redis_url,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )
        self._key_prefix = key_prefix
        self._max_stream_length = max_stream_length
        self._consumer_group = consumer_group

        # Track which streams have consumer groups initialized
        self._initialized_groups: set[str] = set()
        self._groups_lock = threading.Lock()

        self._logger = logging.getLogger(f"{__name__}.DLQManager")
        self._logger.info(f"DLQ Manager initialized with Redis (prefix={key_prefix})")

    def _stream_key(self, source_id: int, table_name: str, destination_id: int) -> str:
        """
        Build Redis stream key for specific source/table/destination.

        Format: {prefix}:s{source_id}:t{table_name}:d{destination_id}
        """
        return f"{self._key_prefix}:s{source_id}:t{table_name}:d{destination_id}"

    def _parse_stream_key(self, key: str) -> Optional[tuple[int, str, int]]:
        """
        Parse stream key back to (source_id, table_name, destination_id).

        Returns None if key doesn't match expected pattern.
        """
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        pattern = re.compile(rf"^{re.escape(self._key_prefix)}:s(\d+):t(.+):d(\d+)$")
        match = pattern.match(key)
        if not match:
            return None
        return int(match.group(1)), match.group(2), int(match.group(3))

    def _ensure_consumer_group(self, stream_key: str) -> None:
        """
        Ensure consumer group exists for a stream. Creates both stream and group if needed.

        Thread-safe and idempotent.
        """
        if stream_key in self._initialized_groups:
            return

        with self._groups_lock:
            if stream_key in self._initialized_groups:
                return

            try:
                # MKSTREAM creates the stream if it doesn't exist
                self._redis.xgroup_create(
                    stream_key,
                    self._consumer_group,
                    id="0",
                    mkstream=True,
                )
                self._logger.debug(
                    f"Created consumer group '{self._consumer_group}' for stream {stream_key}"
                )
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    # Consumer group already exists — this is fine
                    pass
                else:
                    raise

            self._initialized_groups.add(stream_key)

    def _retry_operation(self, operation, *args, **kwargs):
        """
        Execute a Redis operation with retry and exponential backoff.

        Retries on ConnectionError and TimeoutError.
        """
        last_error = None
        for attempt in range(_MAX_RETRIES):
            try:
                return operation(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                last_error = e
                wait_time = _RETRY_BACKOFF_BASE * (2**attempt)
                self._logger.warning(
                    f"Redis operation failed (attempt {attempt + 1}/{_MAX_RETRIES}): {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                time.sleep(wait_time)
        raise last_error

    def enqueue(
        self,
        pipeline_id: int,
        source_id: int,
        destination_id: int,
        table_name: str,
        table_name_target: str,
        cdc_record: CDCRecord,
        table_sync: PipelineDestinationTableSync,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Add CDC record to dead letter queue via Redis Stream XADD.

        Args:
            pipeline_id: Pipeline ID
            source_id: Source database ID
            destination_id: Destination database ID
            table_name: Source table name
            table_name_target: Target table name
            cdc_record: CDC record that failed to write
            table_sync: Table sync configuration
            error_message: Optional error message

        Returns:
            True if successfully enqueued
        """
        try:
            # Serialize table_sync to dict for storage
            table_sync_dict = {
                "id": table_sync.id,
                "pipeline_destination_id": table_sync.pipeline_destination_id,
                "table_name": table_sync.table_name,
                "table_name_target": table_sync.table_name_target,
                "filter_sql": table_sync.filter_sql,
                "custom_sql": table_sync.custom_sql,
            }

            message = DLQMessage(
                pipeline_id=pipeline_id,
                source_id=source_id,
                destination_id=destination_id,
                table_name=table_name,
                table_name_target=table_name_target,
                cdc_record=cdc_record,
                table_sync_config=table_sync_dict,
            )

            stream_key = self._stream_key(source_id, table_name, destination_id)
            self._ensure_consumer_group(stream_key)

            # XADD with MAXLEN cap to prevent unbounded growth
            entry_data = message.to_dict()
            self._retry_operation(
                self._redis.xadd,
                stream_key,
                entry_data,
                maxlen=self._max_stream_length,
            )

            self._logger.warning(
                f"Enqueued to DLQ: s{source_id}:t{table_name}:d{destination_id} "
                f"- operation={cdc_record.operation}, key={cdc_record.key}"
                + (f", error={error_message}" if error_message else "")
            )

            return True

        except Exception as e:
            self._logger.error(
                f"Failed to enqueue to DLQ: s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return False

    def dequeue_batch(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        max_messages: int = 100,
        consumer_name: str = "worker-1",
    ) -> list[tuple[str, DLQMessage]]:
        """
        Dequeue a batch of messages using XREADGROUP (consumer group).

        Messages are marked as pending (not yet ACK'd) until acknowledge() is called.

        Args:
            source_id: Source database ID
            destination_id: Destination database ID
            table_name: Table name
            max_messages: Maximum number of messages to dequeue
            consumer_name: Consumer name within the group

        Returns:
            List of (message_id, DLQMessage) tuples (empty if queue is empty)
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)
            self._ensure_consumer_group(stream_key)

            # XREADGROUP: read new messages (>) for this consumer
            result = self._retry_operation(
                self._redis.xreadgroup,
                self._consumer_group,
                consumer_name,
                {stream_key: ">"},
                count=max_messages,
            )

            if not result:
                return []

            messages = []
            for stream_name, entries in result:
                for entry_id, entry_data in entries:
                    try:
                        msg_id = (
                            entry_id.decode("utf-8")
                            if isinstance(entry_id, bytes)
                            else entry_id
                        )
                        msg = DLQMessage.from_stream_entry(entry_data)
                        messages.append((msg_id, msg))
                    except Exception as parse_err:
                        self._logger.error(
                            f"Failed to parse DLQ message {entry_id}: {parse_err}",
                            exc_info=True,
                        )

            if messages:
                self._logger.debug(
                    f"Dequeued {len(messages)} messages from DLQ: "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )

            return messages

        except Exception as e:
            self._logger.error(
                f"Failed to dequeue from DLQ: s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return []

    def acknowledge(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        message_ids: list[str],
    ) -> int:
        """
        Acknowledge successfully processed messages (XACK + XDEL).

        Removes messages from the pending entries list and deletes them from the stream.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
            message_ids: List of stream entry IDs to acknowledge

        Returns:
            Number of messages acknowledged
        """
        if not message_ids:
            return 0

        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)

            # ACK removes from pending list
            ack_count = self._retry_operation(
                self._redis.xack,
                stream_key,
                self._consumer_group,
                *message_ids,
            )

            # DEL removes entries from stream entirely
            self._retry_operation(
                self._redis.xdel,
                stream_key,
                *message_ids,
            )

            self._logger.debug(
                f"Acknowledged {ack_count} messages in DLQ: "
                f"s{source_id}:t{table_name}:d{destination_id}"
            )

            return ack_count

        except Exception as e:
            self._logger.error(
                f"Failed to acknowledge DLQ messages: "
                f"s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return 0

    def claim_stale_messages(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        min_idle_ms: int = 60000,
        max_messages: int = 100,
        consumer_name: str = "worker-1",
    ) -> list[tuple[str, DLQMessage]]:
        """
        Claim stale pending messages from dead consumers via XAUTOCLAIM.

        Reclaims messages that have been pending for longer than min_idle_ms,
        transferring ownership to the specified consumer.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
            min_idle_ms: Minimum idle time in ms before claiming
            max_messages: Maximum messages to claim
            consumer_name: Consumer to claim messages for

        Returns:
            List of (message_id, DLQMessage) tuples
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)
            self._ensure_consumer_group(stream_key)

            # XAUTOCLAIM: claim stale messages
            result = self._retry_operation(
                self._redis.xautoclaim,
                stream_key,
                self._consumer_group,
                consumer_name,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=max_messages,
            )

            if not result or len(result) < 2:
                return []

            # result format: (next_start_id, [(id, data), ...], [deleted_ids])
            entries = result[1]
            messages = []
            for entry_id, entry_data in entries:
                try:
                    if not entry_data:
                        continue
                    msg_id = (
                        entry_id.decode("utf-8")
                        if isinstance(entry_id, bytes)
                        else entry_id
                    )
                    msg = DLQMessage.from_stream_entry(entry_data)
                    messages.append((msg_id, msg))
                except Exception as parse_err:
                    self._logger.error(
                        f"Failed to parse claimed DLQ message {entry_id}: {parse_err}"
                    )

            if messages:
                self._logger.info(
                    f"Claimed {len(messages)} stale messages in DLQ: "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )

            return messages

        except Exception as e:
            self._logger.error(
                f"Failed to claim stale messages: "
                f"s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return []

    def get_queue_size(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> int:
        """
        Get the number of messages in a specific queue via XLEN.

        Returns:
            Number of messages in queue (0 if queue doesn't exist)
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)
            return self._retry_operation(self._redis.xlen, stream_key)
        except Exception as e:
            self._logger.error(f"Failed to get queue size: {e}")
            return 0

    def list_queues(self) -> list[tuple[int, str, int]]:
        """
        List all queue identifiers by scanning Redis keys.

        Returns:
            List of (source_id, table_name, destination_id) tuples
        """
        queues = []
        try:
            pattern = f"{self._key_prefix}:s*"
            cursor = 0
            while True:
                cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
                for key in keys:
                    parsed = self._parse_stream_key(key)
                    if parsed:
                        queues.append(parsed)
                if cursor == 0:
                    break

            return queues

        except Exception as e:
            self._logger.error(f"Failed to list queues: {e}")
            return []

    def has_messages(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> bool:
        """
        Check if queue has any messages (non-destructive via XLEN).
        """
        return self.get_queue_size(source_id, table_name, destination_id) > 0

    def purge_old_messages(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        max_retry_count: int = 10,
        max_age_days: int = 7,
    ) -> int:
        """
        Purge messages that exceed retry limits or age.

        Uses XRANGE to read messages non-destructively, then XDEL + XACK to
        remove only the ones that should be purged.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
            max_retry_count: Maximum retry attempts before purging
            max_age_days: Maximum age in days before purging

        Returns:
            Number of messages purged
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)
            stream_len = self._redis.xlen(stream_key)
            if stream_len == 0:
                return 0

            from datetime import timedelta

            cutoff_date = datetime.now(timezone(timedelta(hours=7))) - timedelta(days=max_age_days)
            ids_to_purge = []

            # Read all entries with XRANGE (non-destructive)
            entries = self._redis.xrange(stream_key, count=10000)

            for entry_id, entry_data in entries:
                try:
                    msg = DLQMessage.from_stream_entry(entry_data)
                    should_purge = False

                    # Check retry count
                    if msg.retry_count >= max_retry_count:
                        should_purge = True
                        self._logger.warning(
                            f"Purging DLQ message due to retry limit: "
                            f"s{source_id}:t{table_name}:d{destination_id} "
                            f"retry_count={msg.retry_count}"
                        )

                    # Check age
                    try:
                        first_failed = datetime.fromisoformat(msg.first_failed_at)
                        if first_failed < cutoff_date:
                            should_purge = True
                            self._logger.warning(
                                f"Purging DLQ message due to age: "
                                f"s{source_id}:t{table_name}:d{destination_id} "
                                f"age={datetime.now(timezone(timedelta(hours=7))) - first_failed}"
                            )
                    except Exception:
                        pass

                    if should_purge:
                        msg_id = (
                            entry_id.decode("utf-8")
                            if isinstance(entry_id, bytes)
                            else entry_id
                        )
                        ids_to_purge.append(msg_id)

                except Exception as parse_err:
                    self._logger.error(
                        f"Failed to parse message for purge: {parse_err}"
                    )

            if ids_to_purge:
                # ACK + DEL the purged messages
                self._redis.xack(stream_key, self._consumer_group, *ids_to_purge)
                self._redis.xdel(stream_key, *ids_to_purge)
                self._logger.info(
                    f"Purged {len(ids_to_purge)} old messages from DLQ: "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )

                # If stream is now empty, delete it
                if self._redis.xlen(stream_key) == 0:
                    self.delete_queue(source_id, table_name, destination_id)

            return len(ids_to_purge)

        except Exception as e:
            self._logger.error(
                f"Failed to purge old messages: s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return 0

    def delete_queue(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> bool:
        """
        Delete a queue by removing the Redis stream key entirely.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID

        Returns:
            True if queue was deleted successfully
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)

            deleted = self._retry_operation(self._redis.delete, stream_key)

            # Remove from initialized groups cache
            with self._groups_lock:
                self._initialized_groups.discard(stream_key)

            if deleted:
                self._logger.info(
                    f"Deleted DLQ queue: s{source_id}:t{table_name}:d{destination_id}"
                )

            return bool(deleted)

        except Exception as e:
            self._logger.error(
                f"Failed to delete queue s{source_id}:t{table_name}:d{destination_id}: {e}",
                exc_info=True,
            )
            return False

    def update_message_retry(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        old_message_id: str,
        message: "DLQMessage",
    ) -> Optional[str]:
        """
        Update a message's retry count by adding a new entry and deleting the old one.

        Redis Streams entries are immutable, so we add a new entry with updated
        retry_count and remove the old one.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
            old_message_id: ID of the old stream entry to replace
            message: DLQMessage with updated retry_count

        Returns:
            New message ID if successful, None otherwise
        """
        try:
            stream_key = self._stream_key(source_id, table_name, destination_id)

            # Add updated message
            entry_data = message.to_dict()
            new_id = self._redis.xadd(
                stream_key,
                entry_data,
                maxlen=self._max_stream_length,
            )

            # ACK and delete old entry
            self._redis.xack(stream_key, self._consumer_group, old_message_id)
            self._redis.xdel(stream_key, old_message_id)

            new_id_str = new_id.decode("utf-8") if isinstance(new_id, bytes) else new_id
            return new_id_str

        except Exception as e:
            self._logger.error(
                f"Failed to update message retry: "
                f"s{source_id}:t{table_name}:d{destination_id} - {e}",
                exc_info=True,
            )
            return None

    def close_all(self) -> None:
        """Close Redis connection and cleanup resources."""
        try:
            self._redis.close()
            with self._groups_lock:
                self._initialized_groups.clear()
            self._logger.info("Closed DLQ Redis connection")
        except Exception as e:
            self._logger.warning(f"Error closing DLQ Redis connection: {e}")

    def ping(self) -> bool:
        """Check if Redis connection is alive."""
        try:
            return self._redis.ping()
        except Exception:
            return False
