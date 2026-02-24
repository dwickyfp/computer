"""
SQL Lineage Parser for Worker.

Uses sqlglot to parse custom SQL and extract column-level lineage.
"""

from datetime import datetime, timezone
from typing import Any

try:
    import sqlglot
    from sqlglot import exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

import structlog

logger = structlog.get_logger(__name__)


class LineageParser:
    """Parse SQL to extract column-level lineage information."""

    def __init__(self, source_table: str, source_columns: list[str] | None = None):
        self.source_table = source_table
        self.source_columns = source_columns or []

    def parse(self, sql: str | None) -> dict[str, Any]:
        """
        Parse SQL and extract lineage metadata.

        Args:
            sql: Custom SQL query or None for SELECT *

        Returns:
            Lineage metadata dict
        """
        if not sql or sql.strip() == "":
            return self._create_direct_lineage()

        # Check if it's a simple SELECT * FROM table
        normalized = " ".join(sql.strip().upper().split())
        if normalized == f"SELECT * FROM {self.source_table.upper()}":
            return self._create_direct_lineage()

        if not SQLGLOT_AVAILABLE:
            return self._create_error_lineage("sqlglot not installed")

        try:
            return self._parse_sql(sql)
        except Exception as e:
            logger.warning(
                "SQL parsing failed",
                error=str(e),
                sql=sql[:200] if sql else None,
            )
            return self._create_error_lineage(str(e))

    def _create_direct_lineage(self) -> dict[str, Any]:
        """Create lineage for direct pass-through (SELECT *)."""
        column_lineage = {}
        for col in self.source_columns:
            column_lineage[col] = {
                "sources": [f"{self.source_table}.{col}"],
                "transform": "direct",
            }

        return {
            "version": 1,
            "source_tables": [{"table": self.source_table, "type": "source"}],
            "source_columns": [
                f"{self.source_table}.{col}" for col in self.source_columns
            ],
            "output_columns": self.source_columns.copy(),
            "column_lineage": column_lineage,
            "referenced_tables": [self.source_table],
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _parse_sql(self, sql: str) -> dict[str, Any]:
        """Parse SQL using sqlglot."""
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        tables = self._extract_tables(parsed)
        output_columns = self._extract_output_columns(parsed)

        column_lineage = {}
        source_columns_set = set()

        for col_name, col_expr in output_columns.items():
            sources, transform = self._analyze_column_expression(col_expr, tables)
            column_lineage[col_name] = {"sources": sources, "transform": transform}
            source_columns_set.update(sources)

        return {
            "version": 1,
            "source_tables": [
                {
                    "table": t,
                    "type": "source" if t == self.source_table else "join",
                }
                for t in tables
            ],
            "source_columns": sorted(list(source_columns_set)),
            "output_columns": list(output_columns.keys()),
            "column_lineage": column_lineage,
            "referenced_tables": tables,
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _extract_tables(self, parsed: "exp.Expression") -> list[str]:
        """Extract all table references from SQL."""
        tables = []
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table.db:
                table_name = f"{table.db}.{table_name}"
            if table_name not in tables:
                tables.append(table_name)
        return tables

    def _extract_output_columns(
        self, parsed: "exp.Expression"
    ) -> dict[str, "exp.Expression"]:
        """Extract output column names and their expressions."""
        columns = {}

        select = parsed.find(exp.Select)
        if not select:
            return columns

        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                for col in self.source_columns:
                    columns[col] = exp.Column(this=exp.Identifier(this=col))
            elif isinstance(expr, exp.Alias):
                columns[expr.alias] = expr.this
            elif isinstance(expr, exp.Column):
                columns[expr.name] = expr
            else:
                columns[expr.sql()] = expr

        return columns

    def _analyze_column_expression(
        self, expr: "exp.Expression", tables: list[str]
    ) -> tuple[list[str], str]:
        """Analyze column expression to find source columns and transformation type."""
        sources = []
        transform = "direct"

        for col in expr.find_all(exp.Column):
            table = col.table or (tables[0] if tables else self.source_table)
            sources.append(f"{table}.{col.name}")

        if expr.find(exp.Sum):
            transform = "SUM"
        elif expr.find(exp.Count):
            transform = "COUNT"
        elif expr.find(exp.Avg):
            transform = "AVG"
        elif expr.find(exp.Max):
            transform = "MAX"
        elif expr.find(exp.Min):
            transform = "MIN"
        elif expr.find(exp.Concat) or expr.find(exp.DPipe):
            transform = "CONCAT"
        elif expr.find(exp.Case):
            transform = "CASE"
        elif len(list(expr.find_all(exp.Column))) > 1:
            transform = "expression"

        return sources if sources else [f"{self.source_table}.*"], transform

    def _create_error_lineage(self, error: str) -> dict[str, Any]:
        """Create lineage metadata indicating parse failure."""
        return {
            "version": 1,
            "error": error,
            "source_tables": [{"table": self.source_table, "type": "source"}],
            "source_columns": [],
            "output_columns": [],
            "column_lineage": {},
            "referenced_tables": [self.source_table],
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }


def parse_lineage(
    sql: str | None,
    source_table: str,
    source_columns: list[str] | None = None,
) -> dict[str, Any]:
    """
    Convenience function to parse SQL lineage.

    Args:
        sql: Custom SQL query
        source_table: Primary source table name
        source_columns: List of source column names

    Returns:
        Lineage metadata dict
    """
    parser = LineageParser(source_table, source_columns)
    return parser.parse(sql)
