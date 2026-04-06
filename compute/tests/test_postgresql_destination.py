"""
Focused tests for PostgreSQL destination delete semantics.
"""

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
