"""
Unit tests for KafkaDestination payload serialization.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.models import Destination
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
