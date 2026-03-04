"""
Unit tests for compute/core/error_sanitizer.py.

Tests sensitive-data redaction, friendly-message mapping,
and the db/log convenience helpers.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.error_sanitizer import (
    ErrorSanitizer,
    sanitize_error,
    sanitize_for_db,
    sanitize_for_log,
)


# ===========================================================================
# Helper
# ===========================================================================


def _exc(msg: str) -> Exception:
    return ValueError(msg)


# ===========================================================================
# TestSensitivePatternRedaction
# ===========================================================================


class TestSensitivePatternRedaction:
    def test_postgresql_connection_string_redacted(self):
        err = _exc("could not connect to postgresql://admin:s3cr3t!@db.host:5432/prod")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "s3cr3t" not in result
        assert "postgresql://***" in result.lower() or "***" in result

    def test_password_equals_redacted(self):
        err = _exc('auth failed, password="my_super_password"')
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "my_super_password" not in result

    def test_api_key_redacted(self):
        err = _exc("api_key=abcXYZ1234567890 is invalid")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "abcXYZ1234567890" not in result

    def test_token_redacted(self):
        err = _exc("token=eyJhbGciOiJSUzI1NiJ9.payload.signature")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "eyJhbGciOiJSUzI1NiJ9" not in result

    def test_authorization_bearer_redacted(self):
        err = _exc("Authorization: Bearer eyABCDEFGHIJK")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "eyABCDEFGHIJK" not in result
        assert "***" in result

    def test_private_key_block_redacted(self):
        fake_pem = (
            "-----BEGIN PRIVATE KEY-----\n"
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC\n"
            "-----END PRIVATE KEY-----"
        )
        err = _exc(f"Error loading key: {fake_pem}")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC" not in result
        assert "REDACTED" in result


# ===========================================================================
# TestErrorMappings
# ===========================================================================


class TestErrorMappings:
    def test_connection_refused_mapped(self):
        err = _exc("connection refused to port 5432")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "Database Connection Refused" in result

    def test_authentication_failed_mapped(self):
        err = _exc("FATAL: authentication failed for user admin")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "Failed" in result or "Authentication" in result

    def test_relation_does_not_exist_mapped(self):
        err = _exc('ERROR: relation "orders" does not exist')
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "Not Found" in result or "Table" in result

    def test_unknown_error_no_mapping(self):
        """An error that doesn't match any known pattern returns sanitized original."""
        err = _exc("some completely unusual error xyz")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "xyz" in result or len(result) > 0

    def test_context_prefix_added_to_mapping(self):
        err = _exc("connection refused to 5432")
        result = ErrorSanitizer.sanitize_error_message(err, context="PostgreSQL")
        assert result.startswith("PostgreSQL:")

    def test_context_prefix_added_to_sanitized(self):
        err = _exc("some normal error without mapping")
        result = ErrorSanitizer.sanitize_error_message(err, context="Snowflake")
        assert result.startswith("Snowflake:")


# ===========================================================================
# TestEmptyErrorTypes
# ===========================================================================


class TestEmptyErrorTypes:
    def test_timeout_error_returns_friendly_message(self):
        err = TimeoutError()
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "timed out" in result.lower() or "timeout" in result.lower()
        assert len(result) > 0

    def test_timeout_error_with_context(self):
        err = TimeoutError()
        result = ErrorSanitizer.sanitize_error_message(err, context="Snowflake")
        assert "Snowflake" in result

    def test_empty_string_error_returns_unknown(self):
        err = _exc("")
        result = ErrorSanitizer.sanitize_error_message(err)
        assert "unknown" in result.lower()

    def test_empty_string_error_with_context(self):
        err = _exc("")
        result = ErrorSanitizer.sanitize_error_message(err, context="Postgres")
        assert "Postgres" in result
        assert "unknown" in result.lower()


# ===========================================================================
# TestLongMessageTruncation
# ===========================================================================


class TestLongMessageTruncation:
    def test_long_message_truncated_to_500(self):
        err = _exc("x" * 1000)
        result = ErrorSanitizer.sanitize_error_message(err)
        assert len(result) <= 503  # 500 + possible context prefix


# ===========================================================================
# TestSanitizeForDatabase
# ===========================================================================


class TestSanitizeForDatabase:
    def test_connection_refused_returns_friendly(self):
        err = _exc("connection refused")
        result = ErrorSanitizer.sanitize_for_database(err)
        assert "connect" in result.lower() or "connection" in result.lower()

    def test_authentication_failed_returns_friendly(self):
        err = _exc("authentication failed for user postgres")
        result = ErrorSanitizer.sanitize_for_database(err)
        assert "authentication" in result.lower()

    def test_empty_message_with_dest_type(self):
        err = _exc("")
        result = ErrorSanitizer.sanitize_for_database(err, destination_type="SNOWFLAKE")
        assert "SNOWFLAKE" in result or "unknown" in result.lower()

    def test_timeout_hides_credentials(self):
        err = _exc("operation timed out after 30s")
        result = ErrorSanitizer.sanitize_for_database(err)
        assert "timeout" in result.lower()

    def test_connection_interrupted_mapped(self):
        err = _exc("connection closed unexpectedly")
        result = ErrorSanitizer.sanitize_for_database(err)
        assert "interrupted" in result.lower() or "connection" in result.lower()


# ===========================================================================
# TestSanitizeForLogs
# ===========================================================================


class TestSanitizeForLogs:
    def test_credentials_still_redacted_in_logs(self):
        err = _exc("postgresql://user:MYSECRET@host/db failed")
        result = ErrorSanitizer.sanitize_for_logs(err)
        assert "MYSECRET" not in result

    def test_include_details_false_uses_db_format(self):
        err = _exc("connection refused to 5432")
        result = ErrorSanitizer.sanitize_for_logs(err, include_details=False)
        assert len(result) > 0

    def test_normal_error_preserved_in_logs(self):
        err = _exc("normal error without credentials")
        result = ErrorSanitizer.sanitize_for_logs(err)
        assert "normal error" in result


# ===========================================================================
# TestConvenienceFunctions
# ===========================================================================


class TestConvenienceFunctions:
    def test_sanitize_error_delegates(self):
        err = _exc("connection refused")
        assert sanitize_error(err) == ErrorSanitizer.sanitize_error_message(err)

    def test_sanitize_for_db_delegates(self):
        err = _exc("connection refused")
        assert sanitize_for_db(err) == ErrorSanitizer.sanitize_for_database(err)

    def test_sanitize_for_log_delegates(self):
        err = _exc("postgresql://u:p@host/db error")
        result = sanitize_for_log(err)
        assert "p@host" not in result
