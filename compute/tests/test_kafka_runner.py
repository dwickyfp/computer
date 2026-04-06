"""
Unit tests for KafkaSourceRunner normalization.
"""

import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import Source
from sources.kafka_runner import KafkaSourceRunner


class _FakeMessage:
    def __init__(self, topic: str, key: bytes, value: bytes):
        self._topic = topic
        self._key = key
        self._value = value

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return None


def _make_runner() -> KafkaSourceRunner:
    return _make_runner_with_format("PLAIN_JSON")


def _make_runner_with_format(message_format: str) -> KafkaSourceRunner:
    source = Source.from_dict(
        {
            "id": 1,
            "name": "kafka-source",
            "type": "KAFKA",
            "config": {
                "bootstrap_servers": "kafka:9092",
                "topic_prefix": "dbserver1.inventory",
                "group_id": "compute-consumer",
                "auto_offset_reset": "earliest",
                "format": message_format,
            },
        }
    )
    return KafkaSourceRunner(source)


def test_message_to_record_preserves_native_types_schema_and_timestamp():
    runner = _make_runner()
    msg = _FakeMessage(
        topic="dbserver1.inventory.orders",
        key=b'{"id": 7}',
        value=(
            b'{"id":7,"active":true,"amount":12.5,"tags":["vip"],'
            b'"meta":{"region":"apac"},"rosetta_timestamp":1700000000000,'
            b'"rosetta_operation":"c"}'
        ),
    )

    record = runner._message_to_record(msg)

    assert record is not None
    assert record.table_name == "orders"
    assert record.key == {"id": 7}
    assert record.value["id"] == 7
    assert record.value["active"] is True
    assert record.value["amount"] == 12.5
    assert record.value["tags"] == ["vip"]
    assert record.value["meta"] == {"region": "apac"}
    assert record.schema is None
    assert record.timestamp == 1700000000000
    assert "rosetta_timestamp" not in record.value
    assert "rosetta_operation" not in record.value


def test_message_to_record_uses_before_payload_for_deletes():
    runner = _make_runner()
    msg = _FakeMessage(
        topic="dbserver1.inventory.orders",
        key=b'{"id": 11}',
        value=(
            b'{"id":11,"active":false,"rosetta_timestamp":1700000000001,'
            b'"rosetta_operation":"d"}'
        ),
    )

    record = runner._message_to_record(msg)

    assert record is not None
    assert record.operation == "d"
    assert record.value == {"id": 11, "active": False}


def test_message_to_record_supports_legacy_debezium_json_format():
    runner = _make_runner_with_format("DEBEZIUM_JSON")
    msg = _FakeMessage(
        topic="dbserver1.inventory.orders",
        key=b'{"id": 7}',
        value=(
            b'{"schema":{"type":"struct"},"payload":{"op":"c","ts_ms":1700000000000,'
            b'"after":{"id":7,"active":true,"amount":12.5,"tags":["vip"],'
            b'"meta":{"region":"apac"}}}}'
        ),
    )

    record = runner._message_to_record(msg)

    assert record is not None
    assert record.schema == {"type": "struct"}
    assert record.operation == "c"
    assert record.value["meta"] == {"region": "apac"}


def test_consumer_config_uses_system_group_id_when_source_has_none():
    runner = _make_runner()
    runner._source.config.pop("group_id", None)

    config = runner._consumer_config()

    assert config["group.id"] == "rosetta-kafka-source-1"
    assert config["enable.auto.commit"] is False


class _FakeConsumer:
    def __init__(self, config, messages):
        self.config = config
        self._messages = list(messages)
        self.subscribed = []
        self.commits = 0
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, asynchronous=False):
        self.commits += 1
        self.last_commit_async = asynchronous

    def close(self):
        self.closed = True


def test_run_commits_offsets_after_successful_routing():
    runner = _make_runner()
    msg = _FakeMessage(
        topic="dbserver1.inventory.orders",
        key=b'{"id": 1}',
        value=b'{"id":1,"rosetta_timestamp":1700000000000,"rosetta_operation":"u"}',
    )
    fake_consumer_holder = {}

    def _consumer_factory(config):
        consumer = _FakeConsumer(config, [msg, None])
        fake_consumer_holder["consumer"] = consumer
        return consumer

    stop_event = SimpleNamespace(is_set=lambda: state["stopped"], set=lambda: state.__setitem__("stopped", True))
    state = {"stopped": False}

    class _Router:
        def route_batches(self, records_by_table):
            assert list(records_by_table) == ["orders"]
            state["stopped"] = True

    with patch.dict(
        "sys.modules",
        {"confluent_kafka": SimpleNamespace(Consumer=_consumer_factory)},
    ):
        runner.run("pipe", ["orders"], _Router(), stop_event)

    consumer = fake_consumer_holder["consumer"]
    assert consumer.commits == 1
    assert consumer.last_commit_async is False


def test_run_does_not_commit_offsets_when_routing_fails():
    runner = _make_runner()
    msg = _FakeMessage(
        topic="dbserver1.inventory.orders",
        key=b'{"id": 1}',
        value=b'{"id":1,"rosetta_timestamp":1700000000000,"rosetta_operation":"u"}',
    )
    fake_consumer_holder = {}

    def _consumer_factory(config):
        consumer = _FakeConsumer(config, [msg, None])
        fake_consumer_holder["consumer"] = consumer
        return consumer

    stop_event = SimpleNamespace(is_set=lambda: False)

    class _Router:
        def route_batches(self, records_by_table):
            raise RuntimeError("boom")

    with patch.dict(
        "sys.modules",
        {"confluent_kafka": SimpleNamespace(Consumer=_consumer_factory)},
    ):
        try:
            runner.run("pipe", ["orders"], _Router(), stop_event)
        except RuntimeError as exc:
            assert str(exc) == "boom"
        else:
            raise AssertionError("expected routing error")

    consumer = fake_consumer_holder["consumer"]
    assert consumer.commits == 0
