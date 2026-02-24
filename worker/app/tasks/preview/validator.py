"""
SQL validation for preview queries.

Blocks dangerous SQL operations to ensure preview safety.
"""

import re

from app.core.exceptions import ValidationError

import structlog

logger = structlog.get_logger(__name__)

# Forbidden SQL keywords that could modify data or schema
FORBIDDEN_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "EXEC",
    "EXECUTE",
    "CALL",
    "INTO",
    "MERGE",
    "REPLACE",
    "COPY",
    "EXPORT",
]

# DuckDB-specific dangerous functions that could read/write external files
FORBIDDEN_FUNCTIONS = [
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_json",
    "read_json_auto",
    "read_text",
    "read_blob",
    "write_csv",
    "write_parquet",
    "copy_to",
    "copy_from",
    "http_get",
    "http_post",
]

# Pattern to match forbidden keywords as standalone words
_FORBIDDEN_PATTERN = re.compile(
    r"\b(" + "|".join(FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

_FORBIDDEN_FUNC_PATTERN = re.compile(
    r"\b(" + "|".join(FORBIDDEN_FUNCTIONS) + r")\s*\(",
    re.IGNORECASE,
)


def validate_preview_sql(sql: str) -> None:
    """
    Validate SQL for preview execution.

    Raises ValidationError if SQL contains forbidden keywords.

    Args:
        sql: SQL string to validate

    Raises:
        ValidationError: If SQL contains forbidden operations
    """
    if not sql or not sql.strip():
        return

    # Remove string literals and comments before checking
    cleaned = re.sub(r"'[^']*'", "", sql)  # Remove single-quoted strings
    cleaned = re.sub(r'"[^"]*"', "", cleaned)  # Remove double-quoted strings
    cleaned = re.sub(r"--[^\n]*", "", cleaned)  # Remove line comments
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)  # Remove block comments

    match = _FORBIDDEN_PATTERN.search(cleaned)
    if match:
        raise ValidationError(
            f"SQL contains forbidden keyword: {match.group(1).upper()}. "
            f"Preview only supports SELECT queries."
        )

    func_match = _FORBIDDEN_FUNC_PATTERN.search(cleaned)
    if func_match:
        raise ValidationError(
            f"SQL contains forbidden function: {func_match.group(1)}. "
            f"File access and HTTP functions are not allowed in preview."
        )
