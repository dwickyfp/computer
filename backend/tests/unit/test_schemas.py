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
        pg_host="localhost",
        pg_port=5432,
        pg_database="mydb",
        pg_username="user",
        pg_password="pass",
        publication_name="dbz_pub",
        replication_name="dbz_slot",
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

    def test_publication_name_with_underscore_allowed(self):
        obj = self._make(publication_name="dbz_publication")
        assert obj.publication_name == "dbz_publication"

    def test_publication_name_with_hyphen_rejected(self):
        with pytest.raises(ValidationError, match="Publication name"):
            self._make(publication_name="bad-pub")

    def test_publication_name_with_space_rejected(self):
        with pytest.raises(ValidationError, match="Publication name"):
            self._make(publication_name="bad pub")

    def test_pg_port_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            self._make(pg_port=99999)

    def test_pg_port_zero_rejected(self):
        with pytest.raises(ValidationError):
            self._make(pg_port=0)


# =============================================================================
# PipelineCreate
# =============================================================================


class TestPipelineCreateValidator:
    _base = dict(name="my-pipeline", source_type="POSTGRES", source_id=1)

    def _make(self, **kwargs):
        from app.domain.schemas.pipeline import PipelineCreate

        return PipelineCreate(**{**self._base, **kwargs})

    def test_postgres_with_source_id_accepted(self):
        obj = self._make()
        assert obj.source_id == 1

    def test_postgres_without_source_id_rejected(self):
        with pytest.raises(ValidationError, match="source_id is required"):
            self._make(source_id=None)

    def test_rosetta_without_source_id_accepted(self):
        from app.domain.schemas.pipeline import PipelineCreate

        obj = PipelineCreate(name="rosetta-pipe", source_type="ROSETTA")
        assert obj.source_id is None

    def test_catalog_table_without_catalog_table_id_rejected(self):
        with pytest.raises(ValidationError, match="catalog_table_id is required"):
            self._make(
                source_type="CATALOG_TABLE", source_id=None, catalog_table_id=None
            )

    def test_catalog_table_with_catalog_table_id_accepted(self):
        obj = self._make(
            source_type="CATALOG_TABLE",
            source_id=None,
            catalog_table_id=5,
        )
        assert obj.catalog_table_id == 5

    def test_invalid_source_type_rejected(self):
        with pytest.raises(ValidationError, match="source_type must be one of"):
            self._make(source_type="KAFKA")

    def test_source_type_uppercased(self):
        from app.domain.schemas.pipeline import PipelineCreate

        obj = PipelineCreate(name="test-pipe", source_type="postgres", source_id=1)
        assert obj.source_type == "POSTGRES"

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
        assert "status" in sql
        assert "active" in sql

    def test_multiple_filters_joined_with_semicolon(self):
        """Clauses are joined with ';' (not AND)."""
        obj = self._make(
            filters=[
                {"column": "status", "operator": "=", "value": "active"},
                {"column": "amount", "operator": ">", "value": "100"},
            ]
        )
        sql = obj.get_filter_sql()
        assert ";" in sql

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
