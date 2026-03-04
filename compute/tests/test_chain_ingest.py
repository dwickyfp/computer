"""
Unit tests for compute/chain/ingest.py (ChainIngestManager).

Uses fakeredis so no real Redis instance is required.
"""

import json
import os
import sys
from io import BytesIO
from unittest.mock import patch, MagicMock

import pytest
import pyarrow as pa
import fakeredis

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(fake_redis_client=None):
    """Create a ChainIngestManager with fakeredis injected."""
    from chain.ingest import ChainIngestManager

    manager = ChainIngestManager.__new__(ChainIngestManager)
    manager._redis_url = "redis://localhost:6379/0"
    manager._stream_prefix = "rosetta:chain"
    manager._max_stream_length = 100_000
    manager._retention_days = 7
    manager._redis = fake_redis_client or fakeredis.FakeRedis(decode_responses=False)
    return manager


def _arrow_ipc_bytes(data: dict) -> bytes:
    """Build a minimal Arrow IPC stream from a dict of column→list."""
    arrays = {k: pa.array(v) for k, v in data.items()}
    batch = pa.record_batch(arrays)
    buf = BytesIO()
    with pa.ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    return buf.getvalue()


# ===========================================================================
# TestGetStreamKey
# ===========================================================================


class TestGetStreamKey:
    def test_format(self):
        mgr = _make_manager()
        key = mgr.get_stream_key("client-1", "orders")
        assert key == "rosetta:chain:client-1:orders"

    def test_custom_prefix_respected(self):
        mgr = _make_manager()
        mgr._stream_prefix = "custom:prefix"
        key = mgr.get_stream_key("c1", "t1")
        assert key == "custom:prefix:c1:t1"

    def test_special_chars_in_table_name(self):
        mgr = _make_manager()
        key = mgr.get_stream_key("id-123", "public.orders")
        assert "id-123" in key
        assert "public.orders" in key


# ===========================================================================
# TestIngestArrowIpc
# ===========================================================================


class TestIngestArrowIpc:
    def test_valid_ipc_returns_record_count(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        count = mgr.ingest_arrow_ipc(body, "c1", "orders")
        assert count == 3

    def test_records_written_to_correct_stream(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [10]})
        mgr.ingest_arrow_ipc(body, "c1", "customers")
        stream_key = mgr.get_stream_key("c1", "customers").encode()
        entries = fake_redis.xrange(stream_key)
        assert len(entries) == 1

    def test_invalid_ipc_raises_value_error(self):
        mgr = _make_manager()
        with pytest.raises(ValueError, match="Invalid Arrow IPC"):
            mgr.ingest_arrow_ipc(b"not-arrow-ipc-data", "c1", "orders")

    def test_empty_table_returns_zero(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": []})
        count = mgr.ingest_arrow_ipc(body, "c1", "empty_table")
        assert count == 0

    def test_operation_type_written_to_stream(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1]})
        mgr.ingest_arrow_ipc(body, "c1", "orders", operation_type="u")
        stream_key = mgr.get_stream_key("c1", "orders").encode()
        entries = fake_redis.xrange(stream_key)
        assert len(entries) == 1
        entry_data = entries[0][1]
        assert entry_data[b"operation"] == b"u"

    def test_multiple_batches_accumulate(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body1 = _arrow_ipc_bytes({"id": [1, 2]})
        body2 = _arrow_ipc_bytes({"id": [3, 4, 5]})
        count1 = mgr.ingest_arrow_ipc(body1, "c1", "t1")
        count2 = mgr.ingest_arrow_ipc(body2, "c1", "t1")
        assert count1 == 2
        assert count2 == 3
        stream_key = mgr.get_stream_key("c1", "t1").encode()
        assert fake_redis.xlen(stream_key) == 5


# ===========================================================================
# TestIngestJsonRecords
# ===========================================================================


class TestIngestJsonRecords:
    def test_records_written_to_stream(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        records = [
            {"key": {"id": 1}, "value": {"name": "Alice"}},
            {"key": {"id": 2}, "value": {"name": "Bob"}},
        ]
        count = mgr.ingest_json_records(records, "c1", "users")
        assert count == 2
        stream_key = mgr.get_stream_key("c1", "users").encode()
        assert fake_redis.xlen(stream_key) == 2

    def test_entry_has_expected_fields(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        records = [{"key": {"pk": 99}, "value": {"x": "hello"}}]
        mgr.ingest_json_records(records, "c2", "tbl")
        stream_key = mgr.get_stream_key("c2", "tbl").encode()
        entries = fake_redis.xrange(stream_key)
        assert len(entries) == 1
        fields = entries[0][1]
        assert b"operation" in fields
        assert b"table_name" in fields
        assert b"key" in fields
        assert b"value" in fields

    def test_empty_records_returns_zero(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        count = mgr.ingest_json_records([], "c1", "t1")
        assert count == 0

    def test_table_name_encoded_in_entry(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        mgr.ingest_json_records([{"key": {}, "value": {}}], "c1", "my_table")
        stream_key = mgr.get_stream_key("c1", "my_table").encode()
        entries = fake_redis.xrange(stream_key)
        assert entries[0][1][b"table_name"] == b"my_table"


# ===========================================================================
# TestGetStreamLength
# ===========================================================================


class TestGetStreamLength:
    def test_empty_stream_returns_zero(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        assert mgr.get_stream_length("c1", "no-such-table") == 0

    def test_after_ingest_returns_correct_length(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1, 2, 3, 4, 5]})
        mgr.ingest_arrow_ipc(body, "c1", "t1")
        assert mgr.get_stream_length("c1", "t1") == 5


# ===========================================================================
# TestListStreams
# ===========================================================================


class TestListStreams:
    def test_lists_streams_for_chain_id(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1]})
        mgr.ingest_arrow_ipc(body, "client-X", "orders")
        mgr.ingest_arrow_ipc(body, "client-X", "customers")
        streams = mgr.list_streams("client-X")
        assert len(streams) == 2
        assert any("orders" in s for s in streams)
        assert any("customers" in s for s in streams)

    def test_list_all_streams(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1]})
        mgr.ingest_arrow_ipc(body, "c1", "t1")
        mgr.ingest_arrow_ipc(body, "c2", "t2")
        all_streams = mgr.list_streams()
        assert len(all_streams) >= 2


# ===========================================================================
# TestTrimAllStreams
# ===========================================================================


class TestTrimAllStreams:
    def test_trim_on_empty_returns_zero(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        result = mgr.trim_all_streams()
        assert result == 0

    def test_trim_does_not_raise(self):
        fake_redis = fakeredis.FakeRedis(decode_responses=False)
        mgr = _make_manager(fake_redis)
        body = _arrow_ipc_bytes({"id": [1, 2]})
        mgr.ingest_arrow_ipc(body, "c1", "t1")
        # Should not raise even if nothing to trim
        mgr.trim_all_streams()


# ===========================================================================
# TestClose
# ===========================================================================


class TestClose:
    def test_close_does_not_raise(self):
        mgr = _make_manager()
        mgr.close()  # Should not raise
