"""
Unit tests for Pydantic schema validators.

Exercises field_validator logic in SourceCreate, PipelineCreate,
BackfillJobCreate, and TagCreate without touching the database or HTTP.
"""

import pytest
from pydantic import ValidationError


# =============================================================================
# SourceCreate
# =============================================================================


class TestSourceCreateValidator:
    _base = dict(
        type="POSTGRES",
        config={
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "username": "user",
            "password": "pass",
            "publication_name": "dbz_pub",
            "replication_name": "dbz_slot",
        },
    )

    def _make(self, **kwargs):
        from app.domain.schemas.source import SourceCreate

        return SourceCreate(**{**self._base, **kwargs})

    def test_valid_name_accepted(self):
        obj = self._make(name="prod-db_01")
        assert obj.name == "prod-db_01"

    def test_name_lowercased(self):
        obj = self._make(name="UPPER-CASE")
        assert obj.name == "upper-case"

    def test_name_with_spaces_rejected(self):
        with pytest.raises(ValidationError, match="alphanumeric"):
            self._make(name="invalid name")

    def test_name_with_at_sign_rejected(self):
        with pytest.raises(ValidationError, match="alphanumeric"):
            self._make(name="bad@name")

    def test_name_with_dot_rejected(self):
        with pytest.raises(ValidationError, match="alphanumeric"):
            self._make(name="bad.name")

    def test_postgres_config_defaults_port(self):
        obj = self._make(name="postgres-source", config={**self._base["config"], "port": None})
        assert obj.config["port"] == 5432

    def test_kafka_source_requires_bootstrap_group_and_prefix(self):
        from app.domain.schemas.source import SourceCreate

        obj = SourceCreate(
            name="kafka-source",
            type="KAFKA",
            config={
                "bootstrap_servers": "kafka:9092",
                "topic_prefix": "dbserver1.inventory",
            },
        )
        assert obj.config["format"] == "PLAIN_JSON"
        assert "group_id" not in obj.config


# =============================================================================
# PipelineCreate
# =============================================================================


class TestPipelineCreateValidator:
    _base = dict(name="my-pipeline", source_id=1)

    def _make(self, **kwargs):
        from app.domain.schemas.pipeline import PipelineCreate

        return PipelineCreate(**{**self._base, **kwargs})

    def test_postgres_with_source_id_accepted(self):
        obj = self._make()
        assert obj.source_id == 1

    def test_source_id_required(self):
        with pytest.raises(ValidationError):
            self._make(source_id=None)

    def test_name_lowercased(self):
        obj = self._make(name="UPPER-CASE")
        assert obj.name == "upper-case"

    def test_name_with_spaces_rejected(self):
        with pytest.raises(ValidationError, match="alphanumeric"):
            self._make(name="bad name")

    def test_name_with_dot_rejected(self):
        with pytest.raises(ValidationError, match="alphanumeric"):
            self._make(name="bad.name")


# =============================================================================
# BackfillJobCreate
# =============================================================================


class TestBackfillJobCreateValidator:
    _base = dict(table_name="public.orders")

    def _make(self, **kwargs):
        from app.domain.schemas.backfill import BackfillJobCreate

        return BackfillJobCreate(**{**self._base, **kwargs})

    def test_no_filters_creates_none_sql(self):
        obj = self._make(filters=[])
        assert obj.get_filter_sql() is None

    def test_single_filter_generates_where_clause(self):
        obj = self._make(
            filters=[{"column": "status", "operator": "=", "value": "active"}]
        )
        sql = obj.get_filter_sql()
        assert sql is not None
        assert '"version": 2' in sql
        assert '"status"' in sql
        assert '"active"' in sql

    def test_multiple_filters_generate_json_v2(self):
        obj = self._make(
            filters=[
                {"column": "status", "operator": "=", "value": "active"},
                {"column": "amount", "operator": ">", "value": "100"},
            ]
        )
        sql = obj.get_filter_sql()
        assert '"conditions"' in sql
        assert '"amount"' in sql

    def test_six_filters_rejected(self):
        with pytest.raises(ValidationError):
            self._make(
                filters=[
                    {"column": f"col{i}", "operator": "=", "value": str(i)}
                    for i in range(6)
                ]
            )

    def test_five_filters_accepted(self):
        obj = self._make(
            filters=[
                {"column": f"col{i}", "operator": "=", "value": str(i)}
                for i in range(5)
            ]
        )
        assert len(obj.filters) == 5

    def test_none_filters_creates_none_sql(self):
        obj = self._make(filters=None)
        assert obj.get_filter_sql() is None


# =============================================================================
# TagCreate
# =============================================================================


class TestTagCreateValidator:
    def _make(self, tag: str):
        from app.domain.schemas.tag import TagCreate

        return TagCreate(tag=tag)

    def test_alphanumeric_tag_accepted(self):
        obj = self._make("backend123")
        assert obj.tag == "backend123"

    def test_hyphen_allowed(self):
        obj = self._make("back-end")
        assert obj.tag == "back-end"

    def test_underscore_allowed(self):
        obj = self._make("back_end")
        assert obj.tag == "back_end"

    def test_space_allowed(self):
        obj = self._make("back end")
        assert obj.tag == "back end"

    def test_leading_trailing_whitespace_stripped(self):
        obj = self._make("  myTag  ")
        assert obj.tag == "myTag"

    def test_at_sign_rejected(self):
        with pytest.raises(ValidationError):
            self._make("bad@tag")

    def test_dot_rejected(self):
        with pytest.raises(ValidationError):
            self._make("bad.tag")

    def test_slash_rejected(self):
        with pytest.raises(ValidationError):
            self._make("bad/tag")

    def test_min_length_one_enforced(self):
        """Empty string is rejected by field constraint."""
        with pytest.raises(ValidationError):
            self._make("")
