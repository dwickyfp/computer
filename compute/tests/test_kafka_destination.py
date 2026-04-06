"""
Unit tests for KafkaDestination payload serialization.
"""

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import Destination
from core.exceptions import DestinationException
from destinations.kafka import KafkaDestination
from destinations.base import CDCRecord


def _make_destination(message_format: str) -> KafkaDestination:
    destination = Destination.from_dict(
        {
            "id": 9,
            "name": "kafka-destination",
            "type": "KAFKA",
            "config": {
                "bootstrap_servers": "kafka:9092",
                "topic_prefix": "rosetta.public",
                "format": message_format,
            },
        }
    )
    return KafkaDestination(destination)


def test_record_value_plain_json_adds_rosetta_metadata():
    destination = _make_destination("PLAIN_JSON")
    record = CDCRecord(
        operation="u",
        table_name="orders",
        key={"id": 1},
        value={"id": 1, "status": "paid", "amount": 15.5},
        timestamp=1700000000000,
    )

    payload = json.loads(destination._record_value(record).decode("utf-8"))

    assert payload == {
        "id": 1,
        "status": "paid",
        "amount": 15.5,
        "rosetta_timestamp": 1700000000000,
        "rosetta_operation": "u",
    }


def test_record_value_legacy_debezium_json_still_supported():
    destination = _make_destination("DEBEZIUM_JSON")
    record = CDCRecord(
        operation="d",
        table_name="orders",
        key={"id": 1},
        value={"id": 1, "status": "cancelled"},
        schema={"type": "struct"},
        timestamp=1700000000001,
    )

    payload = json.loads(destination._record_value(record).decode("utf-8"))

    assert payload["schema"] == {"type": "struct"}
    assert payload["payload"]["op"] == "d"
    assert payload["payload"]["before"] == {"id": 1, "status": "cancelled"}
    assert payload["payload"]["after"] is None


class _FakeProducer:
    def __init__(self, config, *, delivery_error=None, remaining=0):
        self.config = config
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.calls = []

    def produce(self, topic, key, value, on_delivery):
        self.calls.append((topic, key, value))
        self._callback = on_delivery

    def poll(self, timeout):
        if hasattr(self, "_callback"):
            callback = self._callback
            del self._callback
            callback(self.delivery_error, SimpleNamespace(topic=lambda: "topic"))

    def flush(self, timeout=None):
        self.poll(0)
        return self.remaining


def test_write_batch_counts_only_delivered_records():
    destination = _make_destination("PLAIN_JSON")
    producer_holder = {}

    def _producer_factory(config):
        producer = _FakeProducer(config)
        producer_holder["producer"] = producer
        return producer

    record = CDCRecord(
        operation="u",
        table_name="orders",
        key={"id": 1},
        value={"id": 1},
        timestamp=1700000000000,
    )
    table_sync = SimpleNamespace(table_name_target="orders")

    with patch.dict(
        "sys.modules",
        {"confluent_kafka": SimpleNamespace(Producer=_producer_factory)},
    ):
        written = destination.write_batch([record], table_sync)

    assert written == 1
    assert producer_holder["producer"].config["acks"] == "all"
    assert producer_holder["producer"].config["enable.idempotence"] is True


def test_write_batch_raises_when_delivery_fails():
    destination = _make_destination("PLAIN_JSON")

    def _producer_factory(config):
        return _FakeProducer(config, delivery_error=RuntimeError("delivery failed"))

    record = CDCRecord(
        operation="u",
        table_name="orders",
        key={"id": 1},
        value={"id": 1},
        timestamp=1700000000000,
    )
    table_sync = SimpleNamespace(table_name_target="orders")

    with patch.dict(
        "sys.modules",
        {"confluent_kafka": SimpleNamespace(Producer=_producer_factory)},
    ), pytest.raises(DestinationException):
        destination.write_batch([record], table_sync)
