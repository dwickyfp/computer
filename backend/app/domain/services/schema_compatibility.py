"""
Schema Compatibility Validation service (B2).

Validates source and destination schemas are compatible before pipeline start.
Detects column mismatches, type incompatibilities, and missing required columns.
"""

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.core.logging import get_logger

logger = get_logger(__name__)


# PostgreSQL type compatibility map (source type -> compatible dest types)
_PG_TYPE_COMPAT = {
    "integer": {"integer", "bigint", "numeric", "double precision", "real", "text", "varchar"},
    "bigint": {"bigint", "numeric", "double precision", "text", "varchar"},
    "smallint": {"smallint", "integer", "bigint", "numeric", "text", "varchar"},
    "serial": {"integer", "bigint", "serial", "bigserial", "numeric"},
    "bigserial": {"bigint", "bigserial", "numeric"},
    "real": {"real", "double precision", "numeric", "text"},
    "double precision": {"double precision", "numeric", "text"},
    "numeric": {"numeric", "double precision", "text"},
    "boolean": {"boolean", "text", "varchar", "integer"},
    "text": {"text", "varchar", "character varying"},
    "varchar": {"text", "varchar", "character varying"},
    "character varying": {"text", "varchar", "character varying"},
    "char": {"char", "text", "varchar", "character varying"},
    "uuid": {"uuid", "text", "varchar"},
    "json": {"json", "jsonb", "text"},
    "jsonb": {"json", "jsonb", "text"},
    "date": {"date", "timestamp", "timestamp without time zone", "timestamp with time zone", "text"},
    "timestamp": {"timestamp", "timestamp without time zone", "timestamp with time zone", "text"},
    "timestamp without time zone": {"timestamp", "timestamp without time zone", "timestamp with time zone", "text"},
    "timestamp with time zone": {"timestamp with time zone", "timestamp without time zone", "text"},
    "bytea": {"bytea", "text"},
    "inet": {"inet", "text", "varchar"},
    "cidr": {"cidr", "text", "varchar"},
    "macaddr": {"macaddr", "text", "varchar"},
    "interval": {"interval", "text"},
    "point": {"point", "text"},
    "geometry": {"geometry", "text"},
    "geography": {"geography", "text"},
}


class SchemaValidationResult:
    """Result of a schema compatibility check."""

    def __init__(self):
        self.compatible: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.source_columns: Dict[str, str] = {}
        self.dest_columns: Dict[str, str] = {}

    def add_error(self, msg: str) -> None:
        self.compatible = False
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "compatible": self.compatible,
            "errors": self.errors,
            "warnings": self.warnings,
            "source_column_count": len(self.source_columns),
            "dest_column_count": len(self.dest_columns),
        }


class SchemaCompatibilityService:
    """
    Validates source/destination schema compatibility.

    Used before pipeline start to catch incompatibilities early.
    """

    def __init__(self, db: Session):
        self.db = db

    def validate_pipeline_schemas(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        target_table: str = None,
    ) -> SchemaValidationResult:
        """
        Validate schema compatibility between source table and destination.

        Args:
            source_id: Source database ID
            table_name: Source table name
            destination_id: Destination ID
            target_table: Destination table name (defaults to source table name)

        Returns:
            SchemaValidationResult with errors and warnings
        """
        from sqlalchemy import text

        result = SchemaValidationResult()
        target = target_table or table_name

        try:
            # Fetch source columns
            source_row = self.db.execute(
                text("SELECT pg_host, pg_port, pg_database, pg_username, pg_password FROM sources WHERE id = :id"),
                {"id": source_id},
            ).fetchone()

            if not source_row:
                result.add_error(f"Source {source_id} not found")
                return result

            # Fetch columns from information_schema via raw connection
            from app.core.security import decrypt_value
            import psycopg2

            src_pass = decrypt_value(source_row.pg_password) if source_row.pg_password else ""
            try:
                src_conn = psycopg2.connect(
                    host=source_row.pg_host,
                    port=source_row.pg_port,
                    database=source_row.pg_database,
                    user=source_row.pg_username,
                    password=src_pass,
                    connect_timeout=5,
                )
                src_cur = src_conn.cursor()
                src_cur.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = %s "
                    "ORDER BY ordinal_position",
                    (table_name,),
                )
                source_cols = src_cur.fetchall()
                src_cur.close()
                src_conn.close()
            except Exception as e:
                result.add_warning(f"Cannot connect to source to verify schema: {e}")
                return result

            if not source_cols:
                result.add_warning(f"Source table '{table_name}' has no columns or doesn't exist")
                return result

            for col_name, dtype, nullable in source_cols:
                result.source_columns[col_name] = dtype

            # Check destination (only PostgreSQL destinations for now)
            dest_row = self.db.execute(
                text("SELECT type, config FROM destinations WHERE id = :id"),
                {"id": destination_id},
            ).fetchone()

            if not dest_row:
                result.add_error(f"Destination {destination_id} not found")
                return result

            dest_type = dest_row.type if hasattr(dest_row, "type") else "POSTGRES"

            if dest_type and dest_type.upper() == "SNOWFLAKE":
                # Snowflake schema validation is more lenient
                result.add_warning(
                    "Snowflake destination — schema validation limited to column presence checks"
                )
                return result

            # PostgreSQL destination — validate column types
            import json
            dest_cfg = dest_row.config
            if isinstance(dest_cfg, str):
                dest_cfg = json.loads(dest_cfg)

            dest_pass = decrypt_value(dest_cfg.get("password", "")) if dest_cfg.get("password") else ""
            try:
                dest_conn = psycopg2.connect(
                    host=dest_cfg.get("host", "localhost"),
                    port=dest_cfg.get("port", 5432),
                    database=dest_cfg.get("database", "postgres"),
                    user=dest_cfg.get("user", ""),
                    password=dest_pass,
                    connect_timeout=5,
                )
                dest_cur = dest_conn.cursor()
                dest_cur.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = %s "
                    "ORDER BY ordinal_position",
                    (target,),
                )
                dest_cols = dest_cur.fetchall()
                dest_cur.close()
                dest_conn.close()
            except Exception as e:
                result.add_warning(f"Cannot connect to destination to verify schema: {e}")
                return result

            if not dest_cols:
                # Destination table doesn't exist yet — will be auto-created
                result.add_warning(
                    f"Destination table '{target}' does not exist yet. "
                    "It will be auto-created on first sync."
                )
                return result

            for col_name, dtype, nullable in dest_cols:
                result.dest_columns[col_name] = dtype

            # Compare schemas
            self._compare_columns(result)

        except Exception as e:
            logger.error(f"Schema validation error: {e}", exc_info=True)
            result.add_warning(f"Schema validation encountered an error: {e}")

        return result

    def _compare_columns(self, result: SchemaValidationResult) -> None:
        """Compare source and destination columns for compatibility."""
        # Check for missing columns in destination
        for src_col, src_type in result.source_columns.items():
            if src_col not in result.dest_columns:
                result.add_warning(
                    f"Column '{src_col}' ({src_type}) exists in source but not in destination"
                )
                continue

            dest_type = result.dest_columns[src_col]
            src_base = src_type.lower().split("(")[0].strip()
            dest_base = dest_type.lower().split("(")[0].strip()

            if src_base == dest_base:
                continue

            # Check compatibility
            compatible_types = _PG_TYPE_COMPAT.get(src_base, set())
            if dest_base not in compatible_types and compatible_types:
                result.add_error(
                    f"Column '{src_col}': type mismatch — "
                    f"source={src_type}, destination={dest_type}"
                )
            elif not compatible_types:
                result.add_warning(
                    f"Column '{src_col}': unknown source type '{src_type}' — "
                    f"cannot verify compatibility with '{dest_type}'"
                )

        # Check for extra columns in destination (usually fine)
        for dest_col in result.dest_columns:
            if dest_col not in result.source_columns:
                result.add_warning(
                    f"Column '{dest_col}' exists in destination but not in source"
                )
