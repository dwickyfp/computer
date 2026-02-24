"""
Error message sanitizer for security.

Removes sensitive information (passwords, connection strings, credentials)
from error messages before storing in database or showing to users.
"""

import re
from typing import Optional


class ErrorSanitizer:
    """Sanitize error messages to remove sensitive information."""

    # Patterns to detect and sanitize
    SENSITIVE_PATTERNS = [
        # Connection strings with passwords
        (r"postgresql://[^:]+:([^@]+)@", r"postgresql://***:***@"),
        (r"mysql://[^:]+:([^@]+)@", r"mysql://***:***@"),
        (r"mongodb://[^:]+:([^@]+)@", r"mongodb://***:***@"),
        # Password in various formats
        (r"password['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"password=***"),
        (r"pwd['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"pwd=***"),
        (r"passwd['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"passwd=***"),
        # API keys and tokens
        (r"api[_-]?key['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"api_key=***"),
        (r"token['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"token=***"),
        (r"secret['\"]?\s*[:=]\s*['\"]?([^'\";\s,}]+)", r"secret=***"),
        # Private keys
        (
            r"-----BEGIN (RSA |)PRIVATE KEY-----.*?-----END (RSA |)PRIVATE KEY-----",
            r"-----BEGIN PRIVATE KEY----- [REDACTED] -----END PRIVATE KEY-----",
        ),
        # Authorization headers
        (r"Authorization:\s*Bearer\s+([^\s]+)", r"Authorization: Bearer ***"),
        (r"Authorization:\s*Basic\s+([^\s]+)", r"Authorization: Basic ***"),
    ]

    # Error type to user-friendly message mapping
    ERROR_MAPPINGS = {
        # Connection errors
        "connection refused": "Database Connection Refused",
        "connection timed out": "Database Connection Timeout",
        "connection reset": "Database Connection Reset",
        "connection closed": "Database Connection Closed",
        "could not connect": "Unable to Connect to Database",
        "no route to host": "Database Server Unreachable",
        "host is unreachable": "Database Server Unreachable",
        "network is unreachable": "Network Connection Failed",
        # Authentication errors
        "authentication failed": "Database Authentication Failed",
        "access denied": "Database Access Denied",
        "permission denied": "Database Permission Denied",
        "invalid username": "Database Authentication Failed",
        "invalid password": "Database Authentication Failed",
        "incorrect password": "Database Authentication Failed",
        "login failed": "Database Login Failed",
        # SSL/TLS errors
        "ssl error": "SSL Connection Error",
        "certificate": "SSL Certificate Error",
        "tls": "TLS Connection Error",
        # PostgreSQL specific
        "psycopg2": "PostgreSQL Connection Error",
        # Schema errors
        "relation does not exist": "Table Not Found",
        "table does not exist": "Table Not Found",
        "column does not exist": "Column Not Found",
        "schema does not exist": "Schema Not Found",
        # General errors
        "timeout": "Operation Timeout",
        "out of memory": "System Resource Error",
        "disk full": "Storage Error",
    }

    @classmethod
    def sanitize_error_message(
        cls, error: Exception, context: Optional[str] = None
    ) -> str:
        """
        Sanitize error message for safe storage and display.

        Args:
            error: The exception object
            context: Optional context (e.g., "PostgreSQL", "WAL Monitor")

        Returns:
            Sanitized, user-friendly error message
        """
        # Get original error message
        original_str = str(error).strip()

        # Handle empty error messages
        if not original_str:
            if context:
                return f"{context}: Unknown error occurred"
            return "Unknown error occurred"

        error_msg = original_str.lower()

        # First, try to match with user-friendly mappings
        for pattern, friendly_msg in cls.ERROR_MAPPINGS.items():
            if pattern in error_msg:
                # Add context if provided
                if context:
                    return f"{context}: {friendly_msg}"
                return friendly_msg

        # If no specific mapping, sanitize sensitive data
        sanitized = original_str

        for pattern, replacement in cls.SENSITIVE_PATTERNS:
            sanitized = re.sub(
                pattern, replacement, sanitized, flags=re.IGNORECASE | re.DOTALL
            )

        # Truncate very long messages
        if len(sanitized) > 500:
            sanitized = sanitized[:497] + "..."

        # Add generic prefix if context provided
        if context:
            return f"{context}: {sanitized}"

        return sanitized

    @classmethod
    def sanitize_for_database(
        cls,
        error: Exception,
        context: Optional[str] = None,
    ) -> str:
        """
        Sanitize error message specifically for database storage.

        This is more aggressive and user-friendly than log sanitization.

        Args:
            error: The exception object
            context: Optional context (e.g., "Source PostgreSQL", "WAL Monitor")

        Returns:
            User-friendly sanitized message for database storage
        """
        error_msg = str(error).strip().lower()

        # Handle empty error messages early
        if not error_msg:
            if context:
                return f"{context}: Unknown error occurred"
            return "Unknown error occurred"

        # Try to categorize the error
        if any(
            x in error_msg
            for x in ["connection refused", "could not connect", "connection timed out"]
        ):
            return "Unable to connect to database"

        if any(
            x in error_msg
            for x in [
                "authentication failed",
                "access denied",
                "permission denied",
                "login failed",
            ]
        ):
            return "Database authentication failed"

        if any(
            x in error_msg
            for x in ["connection closed", "connection reset", "broken pipe"]
        ):
            return "Database connection was interrupted"

        if any(x in error_msg for x in ["timeout", "timed out"]):
            return "Database operation timeout"

        if any(
            x in error_msg for x in ["table does not exist", "relation does not exist"]
        ):
            return "Target table not found"

        if any(x in error_msg for x in ["column does not exist"]):
            return "Column mismatch in database"

        if any(x in error_msg for x in ["ssl", "certificate", "tls"]):
            return "SSL/TLS connection error"

        # Default: sanitize sensitive info but keep some detail
        sanitized = cls.sanitize_error_message(error, context)

        # Further simplify for database
        if "password" in sanitized.lower() or "credential" in sanitized.lower():
            return "Database authentication configuration error"

        return sanitized

    @classmethod
    def sanitize_for_logs(
        cls,
        error: Exception,
        include_details: bool = True,
    ) -> str:
        """
        Sanitize error message for logging.

        Logs can have more detail than database/UI, but still hide credentials.

        Args:
            error: The exception object
            include_details: Whether to include detailed error info

        Returns:
            Sanitized message for logging
        """
        if not include_details:
            return cls.sanitize_for_database(error)

        # For logs, sanitize credentials but keep error structure
        original_str = str(error)
        sanitized = original_str

        for pattern, replacement in cls.SENSITIVE_PATTERNS:
            sanitized = re.sub(
                pattern, replacement, sanitized, flags=re.IGNORECASE | re.DOTALL
            )

        return sanitized


# Convenience functions
def sanitize_error(error: Exception, context: Optional[str] = None) -> str:
    """Sanitize error message for safe display."""
    return ErrorSanitizer.sanitize_error_message(error, context)


def sanitize_for_db(
    error: Exception,
    context: Optional[str] = None,
) -> str:
    """Sanitize error message for database storage."""
    return ErrorSanitizer.sanitize_for_database(error, context)


def sanitize_for_log(error: Exception, include_details: bool = True) -> str:
    """Sanitize error message for logging."""
    return ErrorSanitizer.sanitize_for_logs(error, include_details)
