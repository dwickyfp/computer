"""
Kafka schema inference helpers.
"""

from __future__ import annotations

import json
from typing import Any

KAFKA_METADATA_FIELDS = frozenset({"rosetta_timestamp", "rosetta_operation"})


def normalize_schema_definition(schema_data: Any) -> dict[str, dict[str, Any]]:
    """Normalize schema payloads into a dict keyed by column name."""
    if isinstance(schema_data, dict):
        normalized: dict[str, dict[str, Any]] = {}
        for key, value in schema_data.items():
            if not isinstance(value, dict):
                normalized[str(key)] = {
                    "column_name": str(key),
                    "data_type": "UNKNOWN",
                    "real_data_type": "UNKNOWN",
                    "is_nullable": "YES",
                    "is_primary_key": False,
                    "has_default": False,
                    "default_value": None,
                }
                continue
            column_name = str(value.get("column_name") or key)
            normalized[column_name] = dict(value, column_name=column_name)
        return normalized

    if isinstance(schema_data, list):
        normalized = {}
        for column in schema_data:
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("column_name") or "")
            if not column_name:
                continue
            normalized[column_name] = dict(column, column_name=column_name)
        return normalized

    return {}


def infer_value_type(value: Any) -> str:
    """Infer a PostgreSQL-like logical type from a JSON value."""
    if value is None:
        return "UNKNOWN"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    if isinstance(value, dict):
        return "JSONB"
    if isinstance(value, list):
        return "ARRAY"
    return "TEXT"


def infer_kafka_schema(
    value: dict[str, Any], key: dict[str, Any] | None = None
) -> dict[str, dict[str, Any]]:
    """Infer a schema object from a plain JSON Kafka record."""
    if not isinstance(value, dict):
        return {}

    key_fields = set(key.keys()) if isinstance(key, dict) else set()
    schema: dict[str, dict[str, Any]] = {}
    for column_name in sorted(value.keys()):
        if column_name in KAFKA_METADATA_FIELDS:
            continue
        data_type = infer_value_type(value[column_name])
        schema[column_name] = {
            "column_name": column_name,
            "data_type": data_type,
            "real_data_type": data_type,
            "is_nullable": "YES",
            "is_primary_key": column_name in key_fields,
            "has_default": False,
            "default_value": None,
        }
    return schema


def schema_hash(schema_data: Any) -> str:
    """Create a stable hash key for schema comparison."""
    normalized = normalize_schema_definition(schema_data)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def classify_schema_change(old_schema: Any, new_schema: Any) -> str:
    """Classify schema changes for history records."""
    old_columns = normalize_schema_definition(old_schema)
    new_columns = normalize_schema_definition(new_schema)

    if not old_columns:
        return "INITIAL_LOAD"
    if set(new_columns) != set(old_columns):
        if set(new_columns) - set(old_columns):
            return "NEW COLUMN"
        if set(old_columns) - set(new_columns):
            return "DROP COLUMN"

    for column_name, new_column in new_columns.items():
        old_column = old_columns.get(column_name)
        if not old_column:
            continue
        if (
            old_column.get("real_data_type") or old_column.get("data_type")
        ) != (new_column.get("real_data_type") or new_column.get("data_type")):
            return "CHANGES TYPE"
        if bool(old_column.get("is_primary_key")) != bool(
            new_column.get("is_primary_key")
        ):
            return "CHANGES TYPE"
    return "UNKNOWN"
