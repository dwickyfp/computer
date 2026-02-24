"""
Data profiling module for preview results (D7).

Computes per-column statistics from Arrow tables:
- null_count, null_percent
- distinct_count
- min, max, mean (for numeric columns)
- top_values (most frequent values)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def profile_arrow_table(arrow_table) -> list[dict[str, Any]]:
    """
    Compute profiling statistics for each column in an Arrow table.

    Args:
        arrow_table: PyArrow Table from DuckDB query result

    Returns:
        List of dicts, one per column, with profiling stats:
        [
            {
                "column": "col_name",
                "type": "int64",
                "total_count": 100,
                "null_count": 5,
                "null_percent": 5.0,
                "distinct_count": 42,
                "min": 1,
                "max": 100,
                "mean": 50.5,
                "top_values": [{"value": "foo", "count": 10}, ...]
            },
            ...
        ]
    """
    try:
        import pyarrow.compute as pc
    except ImportError:
        logger.warning("PyArrow compute not available — profiling skipped")
        return []

    profiles: list[dict[str, Any]] = []
    total_rows = arrow_table.num_rows

    if total_rows == 0:
        # Return basic metadata for empty tables
        for col_name in arrow_table.column_names:
            col = arrow_table.column(col_name)
            profiles.append({
                "column": col_name,
                "type": str(col.type),
                "total_count": 0,
                "null_count": 0,
                "null_percent": 0.0,
                "distinct_count": 0,
            })
        return profiles

    for col_name in arrow_table.column_names:
        col = arrow_table.column(col_name)
        col_type = str(col.type)

        profile: dict[str, Any] = {
            "column": col_name,
            "type": col_type,
            "total_count": total_rows,
            "null_count": 0,
            "null_percent": 0.0,
            "distinct_count": 0,
        }

        try:
            # Null statistics
            null_count = pc.sum(pc.is_null(col)).as_py() or 0
            profile["null_count"] = null_count
            profile["null_percent"] = round(
                (null_count / total_rows) * 100, 2
            ) if total_rows > 0 else 0.0

            # Distinct count
            try:
                unique_values = pc.unique(col)
                profile["distinct_count"] = len(unique_values)
            except Exception:
                profile["distinct_count"] = None

            # Numeric stats (min, max, mean)
            if _is_numeric_type(col_type):
                try:
                    non_null = pc.drop_null(col)
                    if len(non_null) > 0:
                        min_val = pc.min(non_null).as_py()
                        max_val = pc.max(non_null).as_py()
                        mean_val = pc.mean(non_null).as_py()
                        profile["min"] = _safe_scalar(min_val)
                        profile["max"] = _safe_scalar(max_val)
                        profile["mean"] = round(mean_val, 4) if mean_val is not None else None
                except Exception:
                    pass

            # Top values (value frequencies) — limited to top 5
            try:
                top_values = _compute_top_values(col, limit=5)
                if top_values:
                    profile["top_values"] = top_values
            except Exception:
                pass

        except Exception as e:
            logger.debug(f"Profiling error for column {col_name}: {e}")

        profiles.append(profile)

    return profiles


def _is_numeric_type(type_str: str) -> bool:
    """Check if Arrow type string represents a numeric type."""
    numeric_types = {
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float", "float16", "float32", "float64", "double",
        "decimal128", "decimal256",
    }
    # Strip precision info (e.g., "decimal128(10, 2)" → "decimal128")
    base_type = type_str.split("(")[0].strip()
    return base_type in numeric_types


def _safe_scalar(value: Any) -> Any:
    """Convert Arrow scalar to JSON-safe Python type."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    # Handle Decimal, date, etc.
    try:
        return float(value)
    except (ValueError, TypeError):
        return str(value)


def _compute_top_values(col, limit: int = 5) -> list[dict[str, Any]]:
    """Compute top N most frequent values for a column."""
    import pyarrow.compute as pc

    # Use value_counts for frequency analysis
    try:
        value_counts = pc.value_counts(col)
    except Exception:
        return []

    if len(value_counts) == 0:
        return []

    # Extract values and counts
    entries: list[tuple[Any, int]] = []
    for item in value_counts:
        # value_counts returns StructArray with 'values' and 'counts'
        val = item["values"].as_py()
        count = item["counts"].as_py()
        entries.append((val, count))

    # Sort by count descending, take top N
    entries.sort(key=lambda x: x[1], reverse=True)
    entries = entries[:limit]

    return [
        {"value": _safe_scalar(v), "count": c}
        for v, c in entries
    ]
