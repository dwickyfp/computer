"""
Schema compatibility validator for compute engine.

Validates source vs destination table schemas before pipeline start.
Catches type mismatches, missing columns, and incompatible types early
to prevent CDC failures at runtime.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from core.models import Pipeline, DestinationType
from core.security import decrypt_value

logger = logging.getLogger(__name__)


# PostgreSQL type compatibility map
# Maps source types to compatible destination types
_PG_TYPE_COMPAT: dict[str, set[str]] = {
    "smallint": {"smallint", "integer", "bigint", "numeric", "real", "double precision", "text", "varchar", "character varying"},
    "integer": {"integer", "bigint", "numeric", "real", "double precision", "text", "varchar", "character varying"},
    "bigint": {"bigint", "numeric", "double precision", "text", "varchar", "character varying"},
    "numeric": {"numeric", "double precision", "text", "varchar", "character varying"},
    "real": {"real", "double precision", "numeric", "text", "varchar", "character varying"},
    "double precision": {"double precision", "numeric", "text", "varchar", "character varying"},
    "boolean": {"boolean", "text", "varchar", "character varying", "integer", "smallint"},
    "text": {"text", "varchar", "character varying"},
    "character varying": {"text", "varchar", "character varying"},
    "varchar": {"text", "varchar", "character varying"},
    "char": {"char", "character", "text", "varchar", "character varying"},
    "character": {"char", "character", "text", "varchar", "character varying"},
    "date": {"date", "timestamp", "timestamp without time zone", "timestamp with time zone", "text", "varchar", "character varying"},
    "timestamp without time zone": {"timestamp without time zone", "timestamp with time zone", "text", "varchar", "character varying"},
    "timestamp with time zone": {"timestamp with time zone", "timestamp without time zone", "text", "varchar", "character varying"},
    "time without time zone": {"time without time zone", "time with time zone", "text", "varchar", "character varying"},
    "time with time zone": {"time with time zone", "time without time zone", "text", "varchar", "character varying"},
    "uuid": {"uuid", "text", "varchar", "character varying"},
    "json": {"json", "jsonb", "text", "varchar", "character varying"},
    "jsonb": {"json", "jsonb", "text", "varchar", "character varying"},
    "bytea": {"bytea", "text"},
    "inet": {"inet", "cidr", "text", "varchar", "character varying"},
    "cidr": {"inet", "cidr", "text", "varchar", "character varying"},
    "macaddr": {"macaddr", "text", "varchar", "character varying"},
    "interval": {"interval", "text", "varchar", "character varying"},
}


@dataclass
class SchemaIssue:
    """A single schema compatibility issue."""
    table_name: str
    column_name: Optional[str]
    severity: str  # "ERROR" or "WARNING"
    message: str


@dataclass
class SchemaValidationResult:
    """Result of schema validation for a pipeline."""
    is_compatible: bool = True
    issues: list[SchemaIssue] = field(default_factory=list)
    tables_checked: int = 0
    tables_skipped: int = 0

    def add_error(self, table_name: str, column_name: Optional[str], message: str):
        self.issues.append(SchemaIssue(table_name, column_name, "ERROR", message))
        self.is_compatible = False

    def add_warning(self, table_name: str, column_name: Optional[str], message: str):
        self.issues.append(SchemaIssue(table_name, column_name, "WARNING", message))


def _normalize_pg_type(raw_type: str) -> str:
    """Normalize PostgreSQL type names for comparison."""
    t = raw_type.lower().strip()
    # Remove array brackets
    t = t.replace("[]", "")
    # Normalize common aliases
    t = t.replace("int4", "integer")
    t = t.replace("int8", "bigint")
    t = t.replace("int2", "smallint")
    t = t.replace("float4", "real")
    t = t.replace("float8", "double precision")
    t = t.replace("bool", "boolean")
    t = t.replace("timestamptz", "timestamp with time zone")
    t = t.replace("timetz", "time with time zone")
    # Strip precision from varchar(N), numeric(P,S), etc.
    if "(" in t:
        t = t[:t.index("(")].strip()
    return t


def _get_source_columns(
    host: str, port: int, database: str, user: str, password: str,
    table_name: str,
) -> dict[str, str]:
    """
    Get column name -> data type map from source PostgreSQL database.

    Returns empty dict on connection failure.
    """
    schema_name = "public"
    tbl = table_name
    if "." in table_name:
        parts = table_name.split(".", 1)
        schema_name = parts[0]
        tbl = parts[1]

    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=database,
            user=user, password=password,
            connect_timeout=10,
        )
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema_name, tbl),
                )
                rows = cur.fetchall()
                return {row["column_name"]: row["data_type"] for row in rows}
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to get source columns for {table_name}: {e}")
        return {}


def _get_dest_columns(
    dest_config: dict, table_name: str,
) -> dict[str, str]:
    """
    Get column name -> data type map from destination PostgreSQL database.

    Returns empty dict on connection failure or for non-PostgreSQL destinations.
    """
    schema_name = "public"
    tbl = table_name
    if "." in table_name:
        parts = table_name.split(".", 1)
        schema_name = parts[0]
        tbl = parts[1]

    try:
        host = dest_config.get("host", "localhost")
        port = dest_config.get("port", 5432)
        database = dest_config.get("database", "postgres")
        user = dest_config.get("user", "")
        password = decrypt_value(dest_config.get("password", "")) if dest_config.get("password") else ""

        conn = psycopg2.connect(
            host=host, port=port, dbname=database,
            user=user, password=password,
            connect_timeout=10,
        )
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema_name, tbl),
                )
                rows = cur.fetchall()
                return {row["column_name"]: row["data_type"] for row in rows}
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to get destination columns for {table_name}: {e}")
        return {}


def _check_type_compatible(source_type: str, dest_type: str) -> bool:
    """Check if source type is compatible with destination type."""
    src_norm = _normalize_pg_type(source_type)
    dst_norm = _normalize_pg_type(dest_type)

    # Exact match
    if src_norm == dst_norm:
        return True

    # Check compatibility map
    compat_set = _PG_TYPE_COMPAT.get(src_norm, set())
    if dst_norm in compat_set:
        return True

    # text/varchar can accept anything
    if dst_norm in ("text", "varchar", "character varying"):
        return True

    return False


def validate_pipeline_schemas(
    pipeline: Pipeline,
    table_names: list[str],
) -> SchemaValidationResult:
    """
    Validate schema compatibility between source and all destinations.

    Only validates PostgreSQL destinations. Snowflake destinations are
    skipped with a warning (type mapping is complex and handled by
    the Snowpipe Streaming layer).

    Args:
        pipeline: Pipeline with loaded source and destinations
        table_names: List of table names to validate

    Returns:
        SchemaValidationResult with any issues found
    """
    result = SchemaValidationResult()

    if not pipeline.source:
        result.add_error("*", None, "Pipeline has no source configured")
        return result

    source = pipeline.source
    src_password = decrypt_value(source.pg_password) if source.pg_password else ""

    for table_name in table_names:
        result.tables_checked += 1

        # Get source columns
        src_columns = _get_source_columns(
            host=source.pg_host,
            port=source.pg_port,
            database=source.pg_database,
            user=source.pg_username,
            password=src_password,
            table_name=table_name,
        )

        if not src_columns:
            result.add_warning(
                table_name, None,
                f"Could not fetch source schema for table '{table_name}' — skipping validation",
            )
            result.tables_skipped += 1
            continue

        # Validate against each destination
        for pd in pipeline.destinations:
            if not pd.destination:
                continue

            dest = pd.destination

            # Skip Snowflake — type mapping handled at Snowpipe Streaming layer
            if dest.type.upper() == DestinationType.SNOWFLAKE.value:
                result.add_warning(
                    table_name, None,
                    f"Snowflake destination '{dest.name}' skipped — "
                    f"type validation handled by Snowpipe Streaming",
                )
                continue

            # Only PostgreSQL destinations
            if dest.type.upper() != DestinationType.POSTGRES.value:
                continue

            # Get target table name from table_syncs
            target_table = table_name
            for ts in pd.table_syncs:
                if ts.table_name == table_name and ts.table_name_target:
                    target_table = ts.table_name_target
                    break

            dest_columns = _get_dest_columns(dest.config, target_table)

            if not dest_columns:
                # Destination table doesn't exist yet — CDC will create it
                result.add_warning(
                    table_name, None,
                    f"Destination table '{target_table}' in '{dest.name}' "
                    f"not found — will be created during replication",
                )
                continue

            # Compare columns
            for col_name, src_type in src_columns.items():
                if col_name not in dest_columns:
                    result.add_warning(
                        table_name, col_name,
                        f"Column '{col_name}' ({src_type}) exists in source "
                        f"but not in destination '{dest.name}' table '{target_table}'",
                    )
                    continue

                dest_type = dest_columns[col_name]
                if not _check_type_compatible(src_type, dest_type):
                    result.add_error(
                        table_name, col_name,
                        f"Type mismatch on column '{col_name}': "
                        f"source={src_type}, destination={dest_type} "
                        f"in '{dest.name}' table '{target_table}'",
                    )

    return result
