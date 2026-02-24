"""
Result serialization for preview data.

Handles conversion of DuckDB/Arrow results to JSON-safe formats.
Optimized for speed: orjson handles datetime, bytes, Decimal natively
when used downstream — but we still normalize for the JSON dict layer.
"""

import base64
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def serialize_preview_result(
    columns: list[str],
    column_types: list[str],
    data: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Serialize preview results to JSON-safe format.

    Args:
        columns: Column names
        column_types: Column type labels (number, text, date, boolean)
        data: List of row dicts

    Returns:
        Serialized result dict ready for JSON encoding
    """
    serialized_data = [
        {k: _serialize_value(v) for k, v in row.items()}
        for row in data
    ]

    return {
        "columns": columns,
        "column_types": column_types,
        "data": serialized_data,
        "error": None,
    }


def serialize_error(error: str) -> dict[str, Any]:
    """Create error response dict."""
    return {
        "columns": [],
        "column_types": [],
        "data": [],
        "error": error,
    }


def extract_column_types(schema) -> list[str]:
    """
    Extract column type labels from an Arrow schema.

    Args:
        schema: PyArrow schema

    Returns:
        List of type labels: 'number', 'text', 'date', 'boolean'
    """
    column_types = []
    for field in schema:
        dtype = str(field.type).lower()
        if any(t in dtype for t in ["int", "float", "decimal", "double"]):
            column_types.append("number")
        elif "bool" in dtype:
            column_types.append("boolean")
        elif any(t in dtype for t in ["date", "time", "timestamp"]):
            column_types.append("date")
        else:
            column_types.append("text")
    return column_types


def _serialize_value(v: Any) -> Any:
    """Serialize a single value to JSON-safe format."""
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(v).decode("utf-8")
    if isinstance(v, Decimal):
        return float(v)
    return v
