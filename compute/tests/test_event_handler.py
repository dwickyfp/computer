"""
Unit tests for compute/core/event_handler.py (CDCEventHandler).

pydbzengine / JPype are stubbed out in conftest.pytest_configure so
CDCEventHandler can be imported and instantiated without a JVM.
"""

import json
import os
import sys
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from destinations.base import CDCRecord


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for ORM model instances
# ---------------------------------------------------------------------------


def _make_table_sync(
    table_name: str = "public.orders",
    table_name_target: str = "orders",
    custom_sql: str = None,
    enabled: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        table_name=table_name,
        table_name_target=table_name_target,
        custom_sql=custom_sql,
        enabled=enabled,
    )


def _make_pipeline_destination(
    id: int = 1,
    destination_id: int = 10,
    table_syncs=None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        destination_id=destination_id,
        table_syncs=table_syncs or [],
    )


def _make_pipeline(
    name: str = "test-pipeline",
    destinations=None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        destinations=destinations or [],
    )


def _make_handler(pipeline=None, destinations=None, dlq_manager=None):
    """Instantiate CDCEventHandler with minimal mocked dependencies."""
    from core.event_handler import CDCEventHandler

    if pipeline is None:
        pipeline = _make_pipeline()
    if destinations is None:
        destinations = {}
    shutdown = threading.Event()
    return CDCEventHandler(
        pipeline=pipeline,
        destinations=destinations,
        dlq_manager=dlq_manager,
        shutdown_event=shutdown,
    )


# ===========================================================================
# TestParseDestinationToTableName
# ===========================================================================


class TestParseDestinationToTableName:
    def test_three_part_topic_returns_last_part(self):
        h = _make_handler()
        result = h._parse_destination_to_table_name("prefix.public.orders")
        assert result == "orders"

    def test_four_plus_parts_returns_last(self):
        h = _make_handler()
        result = h._parse_destination_to_table_name("a.b.c.d")
        assert result == "d"

    def test_empty_string_returns_empty(self):
        h = _make_handler()
        assert h._parse_destination_to_table_name("") == ""

    def test_none_returns_empty(self):
        h = _make_handler()
        assert h._parse_destination_to_table_name(None) == ""

    def test_single_part_returns_empty(self):
        h = _make_handler()
        # Only 1 part → < 3 → skipped
        assert h._parse_destination_to_table_name("heartbeat") == ""

    def test_two_parts_returns_empty(self):
        h = _make_handler()
        assert h._parse_destination_to_table_name("schema.table") == ""

    def test_exactly_three_parts(self):
        h = _make_handler()
        result = h._parse_destination_to_table_name("debezium.public.users")
        assert result == "users"

    def test_topic_with_hyphens(self):
        h = _make_handler()
        result = h._parse_destination_to_table_name(
            "rosetta_my-pipeline.public.tbl_sales_stream_company"
        )
        assert result == "tbl_sales_stream_company"

    def test_java_string_coerced(self):
        """Java strings (JPype) are str()-coerced to Python strings."""
        h = _make_handler()

        # Simulate a JPype Java string by using a custom __str__
        class JavaString:
            def __str__(self):
                return "prefix.schema.my_table"

        result = h._parse_destination_to_table_name(JavaString())
        assert result == "my_table"


# ===========================================================================
# TestBuildRoutingTable
# ===========================================================================


class TestBuildRoutingTable:
    def test_empty_destinations_empty_routing_table(self):
        pipeline = _make_pipeline(destinations=[])
        h = _make_handler(pipeline=pipeline, destinations={})
        assert h._routing_table == {}

    def test_missing_destination_object_skipped(self):
        """If destination_id is not in destinations dict → skip silently."""
        ts = _make_table_sync("public.orders", "orders")
        pd = _make_pipeline_destination(destination_id=99, table_syncs=[ts])
        pipeline = _make_pipeline(destinations=[pd])
        h = _make_handler(pipeline=pipeline, destinations={})  # No dest 99
        assert "public.orders" not in h._routing_table

    def test_single_destination_and_table_registered(self):
        dest_mock = MagicMock()
        ts = _make_table_sync("public.orders", "orders")
        pd = _make_pipeline_destination(destination_id=10, table_syncs=[ts])
        pipeline = _make_pipeline(destinations=[pd])
        h = _make_handler(pipeline=pipeline, destinations={10: dest_mock})
        assert "public.orders" in h._routing_table
        assert len(h._routing_table["public.orders"]) == 1

    def test_multiple_tables_registered(self):
        dest_mock = MagicMock()
        ts1 = _make_table_sync("public.orders", "orders")
        ts2 = _make_table_sync("public.customers", "customers")
        pd = _make_pipeline_destination(destination_id=10, table_syncs=[ts1, ts2])
        pipeline = _make_pipeline(destinations=[pd])
        h = _make_handler(pipeline=pipeline, destinations={10: dest_mock})
        assert "public.orders" in h._routing_table
        assert "public.customers" in h._routing_table

    def test_multiple_destinations_for_same_table(self):
        """Two pipeline destinations pointing to same table → 2 routing entries."""
        dest1 = MagicMock()
        dest2 = MagicMock()
        ts1 = _make_table_sync("public.orders", "orders")
        ts2 = _make_table_sync("public.orders", "orders_copy")
        pd1 = _make_pipeline_destination(id=1, destination_id=10, table_syncs=[ts1])
        pd2 = _make_pipeline_destination(id=2, destination_id=20, table_syncs=[ts2])
        pipeline = _make_pipeline(destinations=[pd1, pd2])
        h = _make_handler(pipeline=pipeline, destinations={10: dest1, 20: dest2})
        assert len(h._routing_table["public.orders"]) == 2

    def test_routing_info_has_correct_destination(self):
        from core.event_handler import RoutingInfo

        dest_mock = MagicMock()
        ts = _make_table_sync("public.orders", "orders")
        pd = _make_pipeline_destination(destination_id=10, table_syncs=[ts])
        pipeline = _make_pipeline(destinations=[pd])
        h = _make_handler(pipeline=pipeline, destinations={10: dest_mock})
        routing_list = h._routing_table["public.orders"]
        assert routing_list[0].destination is dest_mock


# ===========================================================================
# TestParseRecord
# ===========================================================================


class TestParseRecord:
    def _make_event(
        self,
        destination: str = "prefix.public.orders",
        op: str = "c",
        key_data: dict = None,
        after_data: dict = None,
        before_data: dict = None,
    ) -> MagicMock:
        """Build a mock ChangeEvent imitating Debezium JSON output."""
        key_data = key_data or {"payload": {"id": 1}}
        payload = {"op": op}
        if op in ("c", "u", "r"):
            payload["after"] = after_data or {"id": 1, "name": "test"}
        elif op == "d":
            payload["before"] = before_data or {"id": 1, "name": "test"}

        value_data = {"payload": payload}

        event = MagicMock()
        event.destination.return_value = destination
        event.key.return_value = json.dumps(key_data)
        event.value.return_value = json.dumps(value_data)
        return event

    def test_create_event_returns_cdc_record(self):
        h = _make_handler()
        event = self._make_event(op="c")
        record = h._parse_record(event)
        assert record is not None
        assert isinstance(record, CDCRecord)

    def test_create_op_extracted(self):
        h = _make_handler()
        event = self._make_event(op="c")
        record = h._parse_record(event)
        assert record.operation == "c"

    def test_update_op_extracted(self):
        h = _make_handler()
        event = self._make_event(op="u")
        record = h._parse_record(event)
        assert record.operation == "u"

    def test_delete_op_uses_before(self):
        h = _make_handler()
        event = self._make_event(op="d", before_data={"id": 5, "name": "gone"})
        record = h._parse_record(event)
        assert record.operation == "d"
        assert record.value.get("id") == 5

    def test_heartbeat_returns_none(self):
        h = _make_handler()
        event = self._make_event(op="m")
        result = h._parse_record(event)
        assert result is None

    def test_table_name_extracted(self):
        h = _make_handler()
        event = self._make_event(destination="myprefix.public.orders")
        record = h._parse_record(event)
        assert record.table_name == "orders"

    def test_invalid_destination_returns_none(self):
        """Destinations with < 3 parts → table_name '' → record skipped."""
        h = _make_handler()
        event = self._make_event(destination="heartbeat")
        result = h._parse_record(event)
        assert result is None

    def test_none_value_returns_empty_payload(self):
        """event.value() returning None → defaults to empty dict."""
        h = _make_handler()
        event = MagicMock()
        event.destination.return_value = "a.b.c"
        event.key.return_value = json.dumps({"id": 1})
        event.value.return_value = None
        # Should not raise; may return None (no op) or a record with empty value
        result = h._parse_record(event)
        # Either None (no recognizable op) or CDCRecord — both are valid
        assert result is None or isinstance(result, CDCRecord)


# ===========================================================================
# TestCDCEventHandlerInit
# ===========================================================================


class TestCDCEventHandlerInit:
    def test_shutdown_event_auto_created(self):
        from core.event_handler import CDCEventHandler

        pipeline = _make_pipeline()
        h = CDCEventHandler(pipeline=pipeline, destinations={})
        assert h._shutdown_event is not None

    def test_custom_shutdown_event_used(self):
        from core.event_handler import CDCEventHandler

        event = threading.Event()
        pipeline = _make_pipeline()
        h = CDCEventHandler(pipeline=pipeline, destinations={}, shutdown_event=event)
        assert h._shutdown_event is event

    def test_dlq_manager_none_by_default(self):
        from core.event_handler import CDCEventHandler

        pipeline = _make_pipeline()
        h = CDCEventHandler(pipeline=pipeline, destinations={})
        assert h._dlq_manager is None
