"""
Unit tests for compute/core/models.py.

Tests all dataclass models: CDCRecord, Source, Destination, Pipeline, etc.
No database connections required — pure in-memory construction and property checks.
"""

import sys
import os
import pytest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from destinations.base import CDCRecord
from core.models import (
    Source,
    Destination,
    Pipeline,
    PipelineStatus,
    DestinationType,
    SourceType,
    MetadataStatus,
    BackfillStatus,
)


# ===========================================================================
# TestCDCRecord
# ===========================================================================


class TestCDCRecord:
    def test_insert_operation_is_insert(self):
        record = CDCRecord(
            operation="c", table_name="t", key={"id": 1}, value={"id": 1}
        )
        assert record.is_insert is True
        assert record.is_update is False
        assert record.is_delete is False

    def test_snapshot_operation_is_insert(self):
        record = CDCRecord(
            operation="r", table_name="t", key={"id": 1}, value={"id": 1}
        )
        assert record.is_insert is True

    def test_update_operation_is_update(self):
        record = CDCRecord(
            operation="u", table_name="t", key={"id": 1}, value={"id": 1}
        )
        assert record.is_update is True
        assert record.is_insert is False
        assert record.is_delete is False

    def test_delete_operation_is_delete(self):
        record = CDCRecord(
            operation="d", table_name="t", key={"id": 1}, value={"id": 1}
        )
        assert record.is_delete is True
        assert record.is_insert is False
        assert record.is_update is False

    def test_optional_fields_default_none(self):
        record = CDCRecord(operation="c", table_name="t", key={}, value={})
        assert record.schema is None
        assert record.timestamp is None

    def test_all_fields_set(self):
        schema = {"columns": [{"name": "id", "type": "int4"}]}
        record = CDCRecord(
            operation="u",
            table_name="public.orders",
            key={"id": 42},
            value={"id": 42, "status": "shipped"},
            schema=schema,
            timestamp=1700000000000,
        )
        assert record.table_name == "public.orders"
        assert record.key == {"id": 42}
        assert record.schema == schema
        assert record.timestamp == 1700000000000


# ===========================================================================
# TestSource
# ===========================================================================


class TestSourceModel:
    def _base_dict(self, **overrides):
        base = dict(
            id=1,
            name="prod-source",
            type="POSTGRES",
            config={
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "username": "repl_user",
                "publication_name": "dbz_pub",
                "replication_name": "dbz_slot",
            },
            pg_host="db.example.com",
            pg_port=5432,
            pg_database="mydb",
            pg_username="repl_user",
            pg_password="secret",
            publication_name="dbz_pub",
            replication_name="dbz_slot",
            is_publication_enabled=True,
            is_replication_enabled=True,
            total_tables=5,
        )
        base.update(overrides)
        return base

    def test_from_dict_all_fields(self):
        src = Source.from_dict(self._base_dict())
        assert src.id == 1
        assert src.name == "prod-source"
        assert src.pg_host == "db.example.com"
        assert src.pg_port == 5432
        assert src.pg_database == "mydb"
        assert src.pg_username == "repl_user"
        assert src.pg_password == "secret"
        assert src.publication_name == "dbz_pub"
        assert src.replication_name == "dbz_slot"
        assert src.is_publication_enabled is True
        assert src.total_tables == 5
        assert src.is_postgres is True

    def test_kafka_source_type_properties(self):
        src = Source.from_dict(
            self._base_dict(
                type="KAFKA",
                config={
                    "bootstrap_servers": "kafka:9092",
                    "topic_prefix": "dbserver1.inventory",
                    "group_id": "compute-consumer",
                    "format": "PLAIN_JSON",
                },
                pg_host=None,
                pg_port=None,
                pg_database=None,
                pg_username=None,
                publication_name=None,
                replication_name=None,
            )
        )
        assert src.is_kafka is True
        assert src.source_type == "KAFKA"

    def test_from_dict_optional_default_false(self):
        """Optional bool fields default to False when absent."""
        d = self._base_dict()
        d.pop("is_publication_enabled")
        d.pop("is_replication_enabled")
        src = Source.from_dict(d)
        assert src.is_publication_enabled is False
        assert src.is_replication_enabled is False

    def test_from_dict_password_none_allowed(self):
        d = self._base_dict(pg_password=None)
        src = Source.from_dict(d)
        assert src.pg_password is None


# ===========================================================================
# TestDestination
# ===========================================================================


class TestDestinationModel:
    def _base_dict(self, **overrides):
        base = dict(
            id=1,
            name="prod-dest",
            type="POSTGRES",
            config={"host": "localhost", "port": 5435},
        )
        base.update(overrides)
        return base

    def test_from_dict_type_postgres(self):
        dest = Destination.from_dict(self._base_dict(type="POSTGRES"))
        assert dest.is_postgres is True
        assert dest.is_snowflake is False
        assert dest.is_kafka is False

    def test_from_dict_type_snowflake(self):
        dest = Destination.from_dict(self._base_dict(type="SNOWFLAKE"))
        assert dest.is_snowflake is True
        assert dest.is_postgres is False

    def test_from_dict_type_kafka(self):
        dest = Destination.from_dict(
            self._base_dict(
                type="KAFKA",
                config={
                    "bootstrap_servers": "kafka:9092",
                    "topic_prefix": "dbserver1.inventory",
                },
            )
        )
        assert dest.is_kafka is True

    def test_type_check_case_insensitive(self):
        """Type checks should be uppercase-normalised."""
        dest = Destination.from_dict(self._base_dict(type="postgres"))
        assert dest.is_postgres is True

    def test_config_defaults_to_empty_dict(self):
        d = self._base_dict()
        d.pop("config", None)
        dest = Destination.from_dict(d)
        assert dest.config == {}

    # ===========================================================================
    # TestPipelineModel
    # ===========================================================================


class TestPipelineModel:
    def _base_dict(self, **overrides):
        base = dict(
            id=1,
            name="test-pipeline",
            source_id=1,
            status="START",
        )
        base.update(overrides)
        return base

    def test_from_dict_basic(self):
        p = Pipeline.from_dict(self._base_dict())
        assert p.id == 1
        assert p.name == "test-pipeline"
        assert p.source_id == 1
        assert p.status == "START"

    def test_from_dict_source_id_nullable(self):
        p = Pipeline.from_dict(self._base_dict(source_id=None))
        assert p.source_id is None

    def test_destinations_default_empty(self):
        p = Pipeline.from_dict(self._base_dict())
        assert p.destinations == []


# ===========================================================================
# TestEnums
# ===========================================================================


class TestEnums:
    def test_pipeline_status_values(self):
        assert PipelineStatus.START.value == "START"
        assert PipelineStatus.PAUSE.value == "PAUSE"
        assert PipelineStatus.REFRESH.value == "REFRESH"

    def test_destination_type_values(self):
        assert DestinationType.SNOWFLAKE.value == "SNOWFLAKE"
        assert DestinationType.POSTGRES.value == "POSTGRES"
        assert DestinationType.KAFKA.value == "KAFKA"

    def test_source_type_values(self):
        assert SourceType.POSTGRES.value == "POSTGRES"
        assert SourceType.KAFKA.value == "KAFKA"

    def test_metadata_status_values(self):
        assert MetadataStatus.RUNNING.value == "RUNNING"
        assert MetadataStatus.PAUSED.value == "PAUSED"
        assert MetadataStatus.ERROR.value == "ERROR"

    def test_backfill_status_values(self):
        assert BackfillStatus.PENDING.value == "PENDING"
        assert BackfillStatus.EXECUTING.value == "EXECUTING"
        assert BackfillStatus.COMPLETED.value == "COMPLETED"
        assert BackfillStatus.FAILED.value == "FAILED"
        assert BackfillStatus.CANCELLED.value == "CANCELLED"
