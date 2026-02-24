"""
Unit tests for DLQ Manager (Redis Streams backend).

Uses fakeredis to mock Redis for testing without a real Redis instance.
"""

import json
import time
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

# Try to import fakeredis - will be used to mock Redis
try:
    import fakeredis
except ImportError:
    pytest.skip("fakeredis not installed", allow_module_level=True)

import sys
import os

# Add compute to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.dlq_manager import DLQManager, DLQMessage
from destinations.base import CDCRecord


# ─── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis():
    """Create a fakeredis server instance."""
    server = fakeredis.FakeServer()
    return server


@pytest.fixture
def dlq_manager(fake_redis):
    """Create a DLQManager with fakeredis backend."""
    manager = DLQManager(
        redis_url="redis://localhost:6379/0",
        key_prefix="test:dlq",
        max_stream_length=1000,
        consumer_group="test_recovery",
    )
    # Replace the real Redis client with fakeredis
    manager._redis = fakeredis.FakeRedis(server=fake_redis, decode_responses=False)
    return manager


@pytest.fixture
def sample_cdc_record():
    """Create a sample CDC record."""
    return CDCRecord(
        operation="u",
        table_name="users",
        key={"id": 1},
        value={"id": 1, "name": "Alice", "email": "alice@example.com"},
        timestamp=1678901234567,
    )


@pytest.fixture
def sample_table_sync():
    """Create a mock table sync object."""

    class TableSyncMock:
        def __init__(self):
            self.id = 1
            self.table_name = "users"
            self.table_name_target = "USERS"
            self.filter_sql = None
            self.custom_sql = None

    return TableSyncMock()


# ─── DLQMessage Tests ───────────────────────────────────────────────────────


class TestDLQMessage:
    def test_serialization_roundtrip_bytes(self, sample_cdc_record):
        """Test that DLQMessage can be serialized to bytes and back."""
        msg = DLQMessage(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync_config={"id": 1, "table_name": "users"},
            retry_count=3,
        )

        # Serialize and deserialize
        data = msg.to_bytes()
        restored = DLQMessage.from_bytes(data)

        assert restored.pipeline_id == 1
        assert restored.source_id == 1
        assert restored.destination_id == 2
        assert restored.table_name == "users"
        assert restored.table_name_target == "USERS"
        assert restored.retry_count == 3
        assert restored.cdc_record.operation == "u"
        assert restored.cdc_record.key == {"id": 1}

    def test_serialization_roundtrip_stream(self, sample_cdc_record):
        """Test that DLQMessage can be serialized for Redis Streams and back."""
        msg = DLQMessage(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync_config={"id": 1},
        )

        # Serialize to stream dict
        stream_data = msg.to_dict()
        assert "data" in stream_data

        # Simulate what Redis returns (bytes keys)
        redis_data = {b"data": stream_data["data"].encode("utf-8")}
        restored = DLQMessage.from_stream_entry(redis_data)

        assert restored.pipeline_id == 1
        assert restored.cdc_record.table_name == "users"

    def test_first_failed_at_auto_set(self, sample_cdc_record):
        """Test that first_failed_at is automatically set."""
        msg = DLQMessage(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync_config={},
        )
        assert msg.first_failed_at is not None
        # Should be a valid ISO format
        datetime.fromisoformat(msg.first_failed_at)


# ─── DLQManager Tests ───────────────────────────────────────────────────────


class TestDLQManagerEnqueue:
    def test_enqueue_creates_stream(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that enqueue creates a Redis Stream entry."""
        result = dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        assert result is True

        # Verify stream exists and has 1 entry
        stream_key = dlq_manager._stream_key(1, "users", 2)
        assert dlq_manager._redis.xlen(stream_key) == 1

    def test_enqueue_multiple_messages(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that multiple messages can be enqueued."""
        for i in range(5):
            dlq_manager.enqueue(
                pipeline_id=1,
                source_id=1,
                destination_id=2,
                table_name="users",
                table_name_target="USERS",
                cdc_record=sample_cdc_record,
                table_sync=sample_table_sync,
            )

        stream_key = dlq_manager._stream_key(1, "users", 2)
        assert dlq_manager._redis.xlen(stream_key) == 5


class TestDLQManagerDequeue:
    def test_dequeue_batch_reads_messages(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that dequeue_batch returns messages from the stream."""
        # Enqueue 3 messages
        for _ in range(3):
            dlq_manager.enqueue(
                pipeline_id=1,
                source_id=1,
                destination_id=2,
                table_name="users",
                table_name_target="USERS",
                cdc_record=sample_cdc_record,
                table_sync=sample_table_sync,
            )

        # Dequeue
        messages = dlq_manager.dequeue_batch(
            source_id=1,
            table_name="users",
            destination_id=2,
            max_messages=10,
        )

        assert len(messages) == 3
        for msg_id, msg in messages:
            assert isinstance(msg_id, str)
            assert isinstance(msg, DLQMessage)
            assert msg.pipeline_id == 1
            assert msg.cdc_record.operation == "u"

    def test_dequeue_empty_queue(self, dlq_manager):
        """Test that dequeue_batch returns empty list for non-existent queue."""
        messages = dlq_manager.dequeue_batch(
            source_id=99,
            table_name="nonexistent",
            destination_id=99,
            max_messages=10,
        )
        assert messages == []

    def test_dequeue_respects_max_messages(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that dequeue_batch respects the max_messages limit."""
        for _ in range(10):
            dlq_manager.enqueue(
                pipeline_id=1,
                source_id=1,
                destination_id=2,
                table_name="users",
                table_name_target="USERS",
                cdc_record=sample_cdc_record,
                table_sync=sample_table_sync,
            )

        messages = dlq_manager.dequeue_batch(
            source_id=1,
            table_name="users",
            destination_id=2,
            max_messages=3,
        )

        assert len(messages) == 3


class TestDLQManagerAcknowledge:
    def test_acknowledge_removes_from_stream(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that acknowledged messages are removed from the stream."""
        # Enqueue
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        # Dequeue
        messages = dlq_manager.dequeue_batch(
            source_id=1, table_name="users", destination_id=2
        )
        assert len(messages) == 1
        msg_id = messages[0][0]

        # Acknowledge
        acked = dlq_manager.acknowledge(
            source_id=1,
            table_name="users",
            destination_id=2,
            message_ids=[msg_id],
        )
        assert acked == 1

        # Stream should be empty now
        stream_key = dlq_manager._stream_key(1, "users", 2)
        assert dlq_manager._redis.xlen(stream_key) == 0


class TestDLQManagerQueueOps:
    def test_has_messages_nondestructive(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that has_messages doesn't consume messages (non-destructive)."""
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        # Check multiple times — should always be True
        assert dlq_manager.has_messages(1, "users", 2) is True
        assert dlq_manager.has_messages(1, "users", 2) is True
        assert dlq_manager.has_messages(1, "users", 2) is True

        # Stream should still have 1 message
        stream_key = dlq_manager._stream_key(1, "users", 2)
        assert dlq_manager._redis.xlen(stream_key) == 1

    def test_has_messages_empty_queue(self, dlq_manager):
        """Test has_messages on non-existent queue."""
        assert dlq_manager.has_messages(99, "nope", 99) is False

    def test_get_queue_size_accurate(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that get_queue_size returns accurate count."""
        assert dlq_manager.get_queue_size(1, "users", 2) == 0

        for _ in range(7):
            dlq_manager.enqueue(
                pipeline_id=1,
                source_id=1,
                destination_id=2,
                table_name="users",
                table_name_target="USERS",
                cdc_record=sample_cdc_record,
                table_sync=sample_table_sync,
            )

        assert dlq_manager.get_queue_size(1, "users", 2) == 7

    def test_list_queues_finds_all_streams(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that list_queues discovers all DLQ streams."""
        # Create queues for different source/table/dest combos
        combos = [
            (1, "users", 2),
            (1, "orders", 2),
            (3, "products", 4),
        ]

        for source_id, table_name, dest_id in combos:
            dlq_manager.enqueue(
                pipeline_id=1,
                source_id=source_id,
                destination_id=dest_id,
                table_name=table_name,
                table_name_target=table_name.upper(),
                cdc_record=sample_cdc_record,
                table_sync=sample_table_sync,
            )

        queues = dlq_manager.list_queues()
        assert len(queues) == 3
        assert set(queues) == set(combos)

    def test_delete_queue_removes_stream(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that delete_queue removes the Redis stream entirely."""
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        assert dlq_manager.has_messages(1, "users", 2) is True

        deleted = dlq_manager.delete_queue(1, "users", 2)
        assert deleted is True

        assert dlq_manager.has_messages(1, "users", 2) is False
        assert dlq_manager.get_queue_size(1, "users", 2) == 0


class TestDLQManagerPurge:
    def test_purge_by_retry_count(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that messages exceeding retry count are purged."""
        # Enqueue a normal message
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        # Manually add a high-retry message directly to the stream
        stream_key = dlq_manager._stream_key(1, "users", 2)
        high_retry_msg = DLQMessage(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync_config={"id": 1},
            retry_count=15,  # Exceeds default max of 10
        )
        dlq_manager._redis.xadd(stream_key, high_retry_msg.to_dict())

        assert dlq_manager.get_queue_size(1, "users", 2) == 2

        purged = dlq_manager.purge_old_messages(
            source_id=1,
            table_name="users",
            destination_id=2,
            max_retry_count=10,
        )

        assert purged == 1
        assert dlq_manager.get_queue_size(1, "users", 2) == 1

    def test_purge_by_age(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that messages exceeding max age are purged."""
        stream_key = dlq_manager._stream_key(1, "users", 2)
        dlq_manager._ensure_consumer_group(stream_key)

        # Add an old message (8 days ago)
        old_date = (datetime.now(timezone(timedelta(hours=7))) - timedelta(days=8)).isoformat()
        old_msg = DLQMessage(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync_config={"id": 1},
            first_failed_at=old_date,
        )
        dlq_manager._redis.xadd(stream_key, old_msg.to_dict())

        # Add a recent message
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        assert dlq_manager.get_queue_size(1, "users", 2) == 2

        purged = dlq_manager.purge_old_messages(
            source_id=1,
            table_name="users",
            destination_id=2,
            max_age_days=7,
        )

        assert purged == 1
        assert dlq_manager.get_queue_size(1, "users", 2) == 1


class TestDLQManagerUpdateRetry:
    def test_update_message_retry(
        self, dlq_manager, sample_cdc_record, sample_table_sync
    ):
        """Test that update_message_retry creates new entry and removes old."""
        # Enqueue
        dlq_manager.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=sample_cdc_record,
            table_sync=sample_table_sync,
        )

        # Dequeue
        messages = dlq_manager.dequeue_batch(
            source_id=1, table_name="users", destination_id=2
        )
        old_id, msg = messages[0]
        assert msg.retry_count == 0

        # Update retry
        msg.retry_count = 1
        new_id = dlq_manager.update_message_retry(
            source_id=1,
            table_name="users",
            destination_id=2,
            old_message_id=old_id,
            message=msg,
        )

        assert new_id is not None
        assert new_id != old_id

        # Stream should still have exactly 1 message (new one)
        stream_key = dlq_manager._stream_key(1, "users", 2)
        assert dlq_manager._redis.xlen(stream_key) == 1


class TestDLQManagerStreamKey:
    def test_stream_key_format(self, dlq_manager):
        """Test stream key generation format."""
        key = dlq_manager._stream_key(1, "users", 2)
        assert key == "test:dlq:s1:tusers:d2"

    def test_parse_stream_key_valid(self, dlq_manager):
        """Test parsing a valid stream key."""
        result = dlq_manager._parse_stream_key("test:dlq:s1:tusers:d2")
        assert result == (1, "users", 2)

    def test_parse_stream_key_with_dots(self, dlq_manager):
        """Test parsing stream key with dots in table name."""
        result = dlq_manager._parse_stream_key("test:dlq:s1:tpublic.users:d2")
        assert result == (1, "public.users", 2)

    def test_parse_stream_key_invalid(self, dlq_manager):
        """Test parsing an invalid stream key returns None."""
        result = dlq_manager._parse_stream_key("invalid:key")
        assert result is None

    def test_parse_stream_key_bytes(self, dlq_manager):
        """Test parsing bytes stream key."""
        result = dlq_manager._parse_stream_key(b"test:dlq:s5:torders:d10")
        assert result == (5, "orders", 10)


class TestDLQManagerConnection:
    def test_ping(self, dlq_manager):
        """Test ping returns True for healthy connection."""
        assert dlq_manager.ping() is True

    def test_close_all(self, dlq_manager):
        """Test close_all cleans up state."""
        # Add something to initialized groups
        dlq_manager._initialized_groups.add("test:stream")

        dlq_manager.close_all()

        assert len(dlq_manager._initialized_groups) == 0
