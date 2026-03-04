"""
Unit tests for compute/core/schema_validator.py.

Pure unit tests — no database connections required.
Tests focus on _normalize_pg_type, _PG_TYPE_COMPAT, SchemaValidationResult,
and SchemaIssue without touching DB-dependent functions.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.schema_validator import (
    _normalize_pg_type,
    _PG_TYPE_COMPAT,
    SchemaValidationResult,
    SchemaIssue,
)


# ===========================================================================
# TestNormalizePgType
# ===========================================================================


class TestNormalizePgType:
    def test_strips_whitespace_and_lowercases(self):
        assert _normalize_pg_type("  INTEGER ") == "integer"

    def test_character_varying_unchanged(self):
        assert _normalize_pg_type("character varying") == "character varying"

    def test_int4_alias_to_integer(self):
        assert _normalize_pg_type("int4") == "integer"

    def test_int8_alias_to_bigint(self):
        assert _normalize_pg_type("int8") == "bigint"

    def test_int2_alias_to_smallint(self):
        assert _normalize_pg_type("int2") == "smallint"

    def test_float4_alias_to_real(self):
        assert _normalize_pg_type("float4") == "real"

    def test_float8_alias_to_double_precision(self):
        assert _normalize_pg_type("float8") == "double precision"

    def test_bool_alias_to_boolean(self):
        assert _normalize_pg_type("bool") == "boolean"

    def test_timestamptz_alias(self):
        assert _normalize_pg_type("timestamptz") == "timestamp with time zone"

    def test_timetz_alias(self):
        assert _normalize_pg_type("timetz") == "time with time zone"

    def test_strips_array_brackets(self):
        assert _normalize_pg_type("integer[]") == "integer"

    def test_strips_precision_varchar(self):
        assert _normalize_pg_type("varchar(255)") == "varchar"

    def test_strips_precision_numeric(self):
        assert _normalize_pg_type("numeric(10,2)") == "numeric"

    def test_text_unchanged(self):
        assert _normalize_pg_type("text") == "text"

    def test_json_unchanged(self):
        assert _normalize_pg_type("json") == "json"

    def test_jsonb_unchanged(self):
        assert _normalize_pg_type("jsonb") == "jsonb"

    def test_uuid_unchanged(self):
        assert _normalize_pg_type("uuid") == "uuid"


# ===========================================================================
# TestPgTypeCompatibilityMap
# ===========================================================================


class TestPgTypeCompatibilityMap:
    def test_integer_compatible_with_bigint(self):
        assert "bigint" in _PG_TYPE_COMPAT["integer"]

    def test_integer_compatible_with_text(self):
        assert "text" in _PG_TYPE_COMPAT["integer"]

    def test_integer_not_in_text_compat(self):
        """text → integer is NOT allowed (widening only)."""
        assert "integer" not in _PG_TYPE_COMPAT.get("text", set())

    def test_smallint_compatible_with_integer(self):
        assert "integer" in _PG_TYPE_COMPAT["smallint"]

    def test_bigint_not_compatible_with_integer(self):
        """bigint → integer is NOT in compat (would lose precision)."""
        assert "integer" not in _PG_TYPE_COMPAT["bigint"]

    def test_json_compatible_with_jsonb(self):
        assert "jsonb" in _PG_TYPE_COMPAT["json"]

    def test_jsonb_compatible_with_json(self):
        assert "json" in _PG_TYPE_COMPAT["jsonb"]

    def test_uuid_compatible_with_text(self):
        assert "text" in _PG_TYPE_COMPAT["uuid"]

    def test_boolean_compatible_with_integer(self):
        assert "integer" in _PG_TYPE_COMPAT["boolean"]

    def test_date_compatible_with_timestamp(self):
        assert "timestamp without time zone" in _PG_TYPE_COMPAT["date"]

    def test_bytea_compatible_with_text(self):
        assert "text" in _PG_TYPE_COMPAT["bytea"]

    def test_inet_compatible_with_cidr(self):
        assert "cidr" in _PG_TYPE_COMPAT["inet"]

    def test_all_types_compatible_with_themselves(self):
        """Every type should be in its own compat set."""
        for src_type, compat_set in _PG_TYPE_COMPAT.items():
            assert (
                src_type in compat_set
            ), f"{src_type!r} should be compatible with itself"


# ===========================================================================
# TestSchemaValidationResult
# ===========================================================================


class TestSchemaValidationResult:
    def test_default_is_compatible(self):
        result = SchemaValidationResult()
        assert result.is_compatible is True

    def test_default_empty_issues(self):
        result = SchemaValidationResult()
        assert result.issues == []

    def test_default_counters(self):
        result = SchemaValidationResult()
        assert result.tables_checked == 0
        assert result.tables_skipped == 0

    def test_add_error_sets_incompatible(self):
        result = SchemaValidationResult()
        result.add_error("orders", "id", "Type mismatch: integer vs text")
        assert result.is_compatible is False

    def test_add_error_appends_issue(self):
        result = SchemaValidationResult()
        result.add_error("orders", "id", "Type mismatch")
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.severity == "ERROR"
        assert issue.table_name == "orders"
        assert issue.column_name == "id"
        assert "mismatch" in issue.message.lower()

    def test_add_warning_does_not_set_incompatible(self):
        result = SchemaValidationResult()
        result.add_warning("orders", "note", "Missing optional column")
        assert result.is_compatible is True

    def test_add_warning_appends_issue(self):
        result = SchemaValidationResult()
        result.add_warning("customers", None, "Extra column will be ignored")
        assert len(result.issues) == 1
        assert result.issues[0].severity == "WARNING"

    def test_multiple_errors_accumulate(self):
        result = SchemaValidationResult()
        result.add_error("t1", "c1", "err1")
        result.add_error("t2", "c2", "err2")
        assert len(result.issues) == 2
        assert result.is_compatible is False

    def test_mixed_errors_and_warnings(self):
        result = SchemaValidationResult()
        result.add_warning("t1", "c1", "warn")
        result.add_error("t2", "c2", "error")
        assert len(result.issues) == 2
        assert result.is_compatible is False

    def test_tables_checked_incremented_manually(self):
        result = SchemaValidationResult()
        result.tables_checked += 3
        assert result.tables_checked == 3


# ===========================================================================
# TestSchemaIssue
# ===========================================================================


class TestSchemaIssue:
    def test_fields_assigned(self):
        issue = SchemaIssue(
            table_name="users",
            column_name="email",
            severity="WARNING",
            message="Column type widened",
        )
        assert issue.table_name == "users"
        assert issue.column_name == "email"
        assert issue.severity == "WARNING"
        assert issue.message == "Column type widened"

    def test_column_name_optional_none(self):
        issue = SchemaIssue(
            table_name="orders",
            column_name=None,
            severity="ERROR",
            message="Table not found",
        )
        assert issue.column_name is None
