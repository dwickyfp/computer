"""
Unit tests for Kafka schema inference and tracking.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kafka_schema import classify_schema_change, infer_kafka_schema
from core.kafka_schema_tracker import KafkaSchemaTracker


def test_infer_kafka_schema_builds_source_schema_columns():
    schema = infer_kafka_schema(
        {
            "id": 7,
            "is_active": True,
            "amount": 12.5,
            "payload": {"region": "apac"},
            "tags": ["vip"],
            "notes": None,
        },
        {"id": 7},
    )

    assert schema["id"]["column_name"] == "id"
    assert schema["id"]["real_data_type"] == "BIGINT"
    assert schema["id"]["is_primary_key"] is True
    assert schema["is_active"]["real_data_type"] == "BOOLEAN"
    assert schema["amount"]["real_data_type"] == "DOUBLE PRECISION"
    assert schema["payload"]["real_data_type"] == "JSONB"
    assert schema["tags"]["real_data_type"] == "ARRAY"
    assert schema["notes"]["real_data_type"] == "UNKNOWN"


def test_classify_schema_change_detects_add_drop_and_type_changes():
    old_schema = {
        "id": {"column_name": "id", "real_data_type": "BIGINT"},
        "name": {"column_name": "name", "real_data_type": "TEXT"},
    }
    assert (
        classify_schema_change(
            old_schema,
            {
                **old_schema,
                "amount": {"column_name": "amount", "real_data_type": "DOUBLE PRECISION"},
            },
        )
        == "NEW COLUMN"
    )
    assert (
        classify_schema_change(
            old_schema,
            {"id": {"column_name": "id", "real_data_type": "BIGINT"}},
        )
        == "DROP COLUMN"
    )
    assert (
        classify_schema_change(
            old_schema,
            {
                "id": {"column_name": "id", "real_data_type": "TEXT"},
                "name": {"column_name": "name", "real_data_type": "TEXT"},
            },
        )
        == "CHANGES TYPE"
    )


def test_schema_tracker_only_persists_new_fingerprints(monkeypatch):
    calls = []

    def _sync(source_id, table_name, schema_table):
        calls.append((source_id, table_name, schema_table))
        return {"updated": True, "schema_table": schema_table, "change_type": "INITIAL_LOAD"}

    monkeypatch.setattr(
        "core.kafka_schema_tracker.TableMetadataRepository.sync_inferred_schema",
        staticmethod(_sync),
    )

    tracker = KafkaSchemaTracker(source_id=4)
    tracker.track_record("orders", {"id": 1, "amount": 10.5}, {"id": 1})
    tracker.track_record("orders", {"id": 2, "amount": 11.5}, {"id": 2})
    tracker.track_record("orders", {"id": 2, "amount": 11.5, "active": True}, {"id": 2})

    assert len(calls) == 2
    assert calls[0][1] == "orders"
    assert "active" in calls[1][2]


def test_schema_tracker_retries_when_table_metadata_is_missing(monkeypatch):
    calls = []

    def _sync(source_id, table_name, schema_table):
        calls.append((source_id, table_name, schema_table))
        return {"updated": False, "reason": "missing_table", "schema_table": schema_table}

    monkeypatch.setattr(
        "core.kafka_schema_tracker.TableMetadataRepository.sync_inferred_schema",
        staticmethod(_sync),
    )

    tracker = KafkaSchemaTracker(source_id=9)
    tracker.track_record("orders", {"id": 1}, {"id": 1})
    tracker.track_record("orders", {"id": 1}, {"id": 1})

    assert len(calls) == 2
