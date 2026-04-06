"""
Shared JSON v2 filter compiler.
"""

from __future__ import annotations

import json
import re

_ALLOWED_OPERATORS = frozenset(
    {
        "=",
        "!=",
        "<>",
        ">",
        "<",
        ">=",
        "<=",
        "LIKE",
        "ILIKE",
        "NOT LIKE",
        "NOT ILIKE",
        "IN",
        "NOT IN",
        "BETWEEN",
        "IS NULL",
        "IS NOT NULL",
    }
)

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


def _raise(error_cls, message: str):
    raise error_cls(message)


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def quote_filter_value(value, *, error_cls=ValueError) -> str:
    if value is None:
        _raise(error_cls, "Filter value cannot be null for this operator")
    try:
        float(value)
        return str(value)
    except (ValueError, TypeError):
        return f"'{escape_sql_string(str(value))}'"


def build_single_clause(
    column: str,
    operator: str,
    value=None,
    value2=None,
    *,
    error_cls=ValueError,
) -> str:
    op_upper = str(operator or "").upper().strip()
    if op_upper not in _ALLOWED_OPERATORS:
        _raise(error_cls, f"Rejected disallowed operator '{operator}' in filter SQL")

    if not _IDENTIFIER_PATTERN.match(str(column or "")):
        _raise(error_cls, f"Rejected invalid column name '{column}' in filter SQL")

    safe_column = f'"{column}"'

    if op_upper in ("IS NULL", "IS NOT NULL"):
        return f"{safe_column} {op_upper}"

    if value in (None, ""):
        _raise(error_cls, f"Filter value is required for operator '{operator}'")

    if op_upper == "BETWEEN":
        if value2 in (None, ""):
            _raise(error_cls, "BETWEEN requires both value and value2")
        return (
            f"{safe_column} BETWEEN "
            f"{quote_filter_value(value, error_cls=error_cls)} "
            f"AND {quote_filter_value(value2, error_cls=error_cls)}"
        )

    if op_upper in ("LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE"):
        safe_value = escape_sql_string(str(value))
        return f"{safe_column} {op_upper} '%{safe_value}%'"

    if op_upper in ("IN", "NOT IN"):
        values = [v.strip() for v in str(value).split(",") if v.strip()]
        if not values:
            _raise(error_cls, f"{op_upper} requires at least one value")
        quoted = [quote_filter_value(v, error_cls=error_cls) for v in values]
        return f"{safe_column} {op_upper} ({', '.join(quoted)})"

    return f"{safe_column} {operator} {quote_filter_value(value, error_cls=error_cls)}"


def build_where_clause_from_filter_sql(filter_sql: str | None, *, error_cls=ValueError) -> str:
    if not filter_sql:
        return ""

    try:
        parsed = json.loads(filter_sql)
    except (json.JSONDecodeError, TypeError) as exc:
        _raise(error_cls, "filter_sql must be valid JSON v2")

    if not isinstance(parsed, dict) or parsed.get("version") != 2:
        _raise(error_cls, "filter_sql must use version 2 JSON format")

    return build_where_clause_v2(parsed, error_cls=error_cls)


def build_where_clause_v2(parsed: dict, *, error_cls=ValueError) -> str:
    groups = parsed.get("groups", [])
    inter_logic = parsed.get("interLogic", [])
    if not groups:
        return ""

    group_clauses: list[str] = []
    for group in groups:
        conditions = group.get("conditions", [])
        intra_logic = str(group.get("intraLogic", "AND")).upper().strip()
        if intra_logic not in {"AND", "OR"}:
            _raise(error_cls, f"Unsupported intraLogic '{intra_logic}'")

        clauses: list[str] = []
        for cond in conditions:
            clause = build_single_clause(
                cond.get("column", ""),
                cond.get("operator", ""),
                cond.get("value"),
                cond.get("value2"),
                error_cls=error_cls,
            )
            if clause:
                clauses.append(clause)

        if not clauses:
            continue
        if len(clauses) == 1:
            group_clauses.append(clauses[0])
        else:
            group_clauses.append(f"({f' {intra_logic} '.join(clauses)})")

    if not group_clauses:
        return ""

    result = group_clauses[0]
    for i in range(1, len(group_clauses)):
        logic = str(inter_logic[i - 1] if i - 1 < len(inter_logic) else "AND").upper().strip()
        if logic not in {"AND", "OR"}:
            _raise(error_cls, f"Unsupported interLogic '{logic}'")
        result = f"{result} {logic} {group_clauses[i]}"
    return result
