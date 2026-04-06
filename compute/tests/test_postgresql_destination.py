"""
Focused tests for PostgreSQL destination delete semantics.
"""

from datetime import date, datetime, time
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.exceptions import DestinationException
from core.models import Destination
from destinations.base import CDCRecord
from destinations.postgresql import PostgreSQLDestination


class _FakeDuckDBConnection:
    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql):
        self.executed.append(str(sql))
        return self


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.execute_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.execute_calls += 1

    def fetchall(self):
        return list(self._rows)


class _FakePGConnection:
    def __init__(self, rows):
        self._cursor = _FakeCursor(rows)

    def cursor(self):
        return self._cursor


def _make_destination() -> PostgreSQLDestination:
    destination = Destination.from_dict(
        {
            "id": 1,
            "name": "pg-destination",
            "type": "POSTGRES",
            "config": {
                "host": "localhost",
                "port": 5432,
                "database": "target",
                "user": "postgres",
                "password": "secret",
                "schema": "public",
            },
        }
    )
    dest = PostgreSQLDestination(destination)
    dest._is_initialized = True
    dest._duckdb_conn = _FakeDuckDBConnection()
    return dest


def _make_table_sync(filter_sql=None, custom_sql=None):
    return SimpleNamespace(
        table_name="orders",
        table_name_target="orders",
        filter_sql=filter_sql,
        custom_sql=custom_sql,
        primary_key_column_target=None,
    )


def _make_delete_record():
    return CDCRecord(
        operation="d",
        table_name="orders",
        key={"id": 1},
        value={"id": 1, "status": "cancelled"},
        timestamp=1700000000000,
    )


def test_filter_sql_delete_uses_delete_path_not_merge():
    destination = _make_destination()
    table_sync = _make_table_sync(
        filter_sql='{"version":2,"groups":[{"conditions":[{"column":"status","operator":"=","value":"cancelled"}],"intraLogic":"AND"}],"interLogic":[]}'
    )

    with patch.object(
        destination, "_get_table_schema", return_value={"id": {"type": "integer"}}
    ), patch.object(destination, "_insert_batch_to_duckdb"), patch.object(
        destination, "_apply_filters_in_duckdb"
    ) as apply_filter, patch.object(
        destination, "_get_duckdb_columns", return_value=["id", "status"]
    ), patch.object(
        destination, "_count_duckdb_rows", return_value=1
    ), patch.object(
        destination, "_resolve_key_columns", return_value=["id"]
    ), patch.object(
        destination, "_delete_duckdb_table_from_postgres", return_value=1
    ) as delete_from_postgres, patch.object(
        destination, "_merge_into_postgres"
    ) as merge_into_postgres:
        written = destination.write_batch([_make_delete_record()], table_sync)

    assert written == 1
    apply_filter.assert_called_once()
    delete_from_postgres.assert_called_once()
    merge_into_postgres.assert_not_called()


def test_custom_sql_delete_fails_closed():
    destination = _make_destination()
    table_sync = _make_table_sync(custom_sql="SELECT * FROM orders")

    with patch.object(
        destination, "_get_table_schema", return_value={"id": {"type": "integer"}}
    ), patch.object(destination, "_insert_batch_to_duckdb"), patch.object(
        destination, "_get_duckdb_columns", return_value=["id"]
    ), patch.object(
        destination, "_count_duckdb_rows", return_value=1
    ):
        with pytest.raises(DestinationException) as exc_info:
            destination.write_batch([_make_delete_record()], table_sync)

    assert "DELETE events are not supported with custom_sql pipelines" in str(
        exc_info.value
    )


def test_insert_batch_uses_target_schema_fallback_for_plain_json_logical_types():
    destination = _make_destination()
    captured = {}

    record = CDCRecord(
        operation="u",
        table_name="orders",
        key={"id": 1},
        value={
            "id": 1,
            "transaction_date": 1,
            "created_at": 1_000_000,
        },
        timestamp=1700000000000,
    )

    def _capture_arrow_table(arrays):
        captured.update(arrays)
        return SimpleNamespace()

    with patch("destinations.postgresql.pa.table", side_effect=_capture_arrow_table):
        destination._insert_batch_to_duckdb(
            [record],
            "orders",
            target_schema={
                "id": {"type": "integer"},
                "transaction_date": {"type": "date"},
                "created_at": {"type": "timestamp with time zone"},
            },
        )

    assert captured["id"] == [1]
    assert captured["transaction_date"] == [date(1970, 1, 2)]
    assert captured["created_at"] == [datetime(1970, 1, 1, 0, 0, 1)]


def test_convert_debezium_value_parses_iso_time_string_for_time_columns():
    destination = _make_destination()

    without_tz = destination._convert_debezium_value(
        "02:31:58Z",
        "support_call_time",
        {"type": "time without time zone"},
    )
    with_tz = destination._convert_debezium_value(
        "02:31:58Z",
        "support_call_time",
        {"type": "time with time zone"},
    )

    assert without_tz == time(2, 31, 58)
    assert with_tz == "02:31:58+00:00"


def test_insert_batch_applies_target_schema_after_debezium_schema_coercion():
    destination = _make_destination()
    captured = {}

    record = CDCRecord(
        operation="u",
        table_name="orders",
        key={"id": 1},
        value={"support_call_time": "02:31:58Z"},
        schema={
            "type": "struct",
            "fields": [
                {
                    "field": "after",
                    "fields": [
                        {
                            "field": "support_call_time",
                            "type": "string",
                            "name": "io.debezium.time.ZonedTime",
                        }
                    ],
                }
            ],
        },
        timestamp=1700000000000,
    )

    def _capture_arrow_table(arrays):
        captured.update(arrays)
        return SimpleNamespace()

    with patch("destinations.postgresql.pa.table", side_effect=_capture_arrow_table):
        destination._insert_batch_to_duckdb(
            [record],
            "orders",
            target_schema={
                "support_call_time": {"type": "time without time zone"},
            },
        )

    assert captured["support_call_time"] == [time(2, 31, 58)]


def test_insert_batch_uses_record_key_when_delete_payload_is_empty():
    destination = _make_destination()
    captured = {}

    record = CDCRecord(
        operation="d",
        table_name="orders",
        key={"id": 7},
        value={},
        timestamp=1700000000000,
    )

    def _capture_arrow_table(arrays):
        captured.update(arrays)
        return SimpleNamespace()

    with patch("destinations.postgresql.pa.table", side_effect=_capture_arrow_table):
        destination._insert_batch_to_duckdb(
            [record],
            "orders",
            target_schema={"id": {"type": "integer"}},
        )

    assert captured["id"] == [7]


def test_get_target_primary_key_uses_cache_after_first_lookup():
    destination = _make_destination()
    destination._pg_conn = _FakePGConnection(rows=[("id",)])

    first = destination._get_target_primary_key("orders")
    second = destination._get_target_primary_key("orders")

    assert first == ["id"]
    assert second == ["id"]
    assert destination._pg_conn._cursor.execute_calls == 1
