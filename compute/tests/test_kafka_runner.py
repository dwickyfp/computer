"""
Unit tests for KafkaSourceRunner normalization.
"""

import os
import sys

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
