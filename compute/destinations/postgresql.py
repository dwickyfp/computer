"""
PostgreSQL destination using DuckDB for MERGE INTO operations.

Provides CDC data sync to PostgreSQL with filter and custom SQL support.
"""

import logging
import re
from typing import Any, Optional
from contextlib import contextmanager

import duckdb
import psycopg2
import pyarrow as pa

from destinations.base import BaseDestination, CDCRecord
from core.models import Destination, PipelineDestinationTableSync
from core.exceptions import DestinationException
from core.security import decrypt_value
from core.notification import NotificationLogRepository, NotificationLogCreate
from core.error_sanitizer import sanitize_for_db

logger = logging.getLogger(__name__)


class PostgreSQLDestination(BaseDestination):
    """
    PostgreSQL destination using DuckDB for efficient MERGE INTO operations.

    Flow:
    1. Create table if not exists (based on CDC schema)
    2. Filter columns (optional, from filter_sql)
    3. Custom SQL transformation (optional, from custom_sql)
    4. MERGE INTO destination table
    """

    # Required config keys
    REQUIRED_CONFIG = ["host", "port", "database", "user", "password"]

    def __init__(self, config: Destination, source_config: Optional[Any] = None):
        """
        Initialize PostgreSQL destination.

        Args:
            config: Destination configuration from database
            source_config: Optional source configuration for attaching source database to DuckDB
        """
        super().__init__(config)
        self._source_config = source_config
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._pg_conn: Optional[psycopg2.extensions.connection] = None
        self._staging_tables: set[str] = set()  # Track created staging tables for reuse
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate required configuration keys."""
        cfg = self._config.config
        missing = [k for k in self.REQUIRED_CONFIG if k not in cfg]
        if missing:
            raise DestinationException(
                f"Missing required PostgreSQL config: {missing}",
                {"destination_id": self._config.id},
            )

    @property
    def host(self) -> str:
        """Get PostgreSQL host."""
        return self._config.config["host"]

    @property
    def port(self) -> int:
        """Get PostgreSQL port."""
        return int(self._config.config.get("port", 5432))

    @property
    def database(self) -> str:
        """Get PostgreSQL database."""
        return self._config.config["database"]

    @property
    def user(self) -> str:
        """Get PostgreSQL user."""
        return self._config.config["user"]

    @property
    def password(self) -> str:
        """Get PostgreSQL password (decrypted)."""
        return decrypt_value(self._config.config.get("password", ""))

    @property
    def schema(self) -> str:
        """Get target schema."""
        return self._config.config.get("schema", "public")

    @property
    def duckdb_alias(self) -> str:
        """Get DuckDB attach alias name: pg_<destination_name_lowercase>."""
        # Sanitize destination name: lowercase, replace spaces/special chars with underscores
        sanitized = re.sub(r"[^a-z0-9_]", "_", self._config.name.lower())
        return f"pg_{sanitized}"

    @property
    def source_duckdb_alias(self) -> Optional[str]:
        """Get DuckDB attach alias name for source: pg_src_<source_name_lowercase>."""
        if not self._source_config:
            return None
        # Sanitize source name: lowercase, replace spaces/special chars with underscores
        sanitized = re.sub(r"[^a-z0-9_]", "_", self._source_config.name.lower())
        return f"pg_src_{sanitized}"

    def _get_postgres_connection_string(self) -> str:
        """Get PostgreSQL connection string for DuckDB."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def _get_source_connection_string(self) -> Optional[str]:
        """Get source PostgreSQL connection string for DuckDB."""
        if not self._source_config:
            return None

        from core.security import decrypt_value

        # Source model has direct fields, not a config dict
        host = self._source_config.pg_host
        port = self._source_config.pg_port
        database = self._source_config.pg_database
        user = self._source_config.pg_username
        password = decrypt_value(self._source_config.pg_password or "")

        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _check_connection_health(self) -> bool:
        """
        Check if existing PostgreSQL connection is healthy.

        Returns:
            True if connection is healthy and usable
        """
        if not self._pg_conn:
            return False

        try:
            # Check if connection is closed
            if self._pg_conn.closed:
                self._logger.debug("PostgreSQL connection is closed")
                return False

            # Execute simple query to verify connection is alive
            with self._pg_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            self._logger.debug(f"PostgreSQL connection health check failed: {e}")
            return False
        except Exception as e:
            self._logger.warning(f"Unexpected error in connection health check: {e}")
            return False

    def initialize(self, force_reconnect: bool = False) -> None:
        """
        Initialize DuckDB connection with PostgreSQL extension.

        Args:
            force_reconnect: Force reconnection even if already initialized
        """
        # If already initialized and connections are healthy, skip
        if self._is_initialized and not force_reconnect:
            # Check connection health
            if self._check_connection_health():
                return
            else:
                # Connection is stale/closed, need to reconnect
                self._logger.info(
                    f"Detected stale connection for {self._config.name}, reconnecting..."
                )
                self._cleanup_connections()
                self._is_initialized = False

        try:
            # Create in-memory DuckDB connection with performance tuning
            self._duckdb_conn = duckdb.connect(":memory:")
            self._duckdb_conn.execute("SET memory_limit='4GB'")
            self._duckdb_conn.execute("SET threads=4")
            self._duckdb_conn.execute("SET enable_progress_bar=false")

            # Reset staging table tracker on new connection
            self._staging_tables.clear()

            # Install and load PostgreSQL extension
            self._duckdb_conn.execute("INSTALL postgres;")
            self._duckdb_conn.execute("LOAD postgres;")

            # Attach PostgreSQL destination database with dynamic alias name
            conn_str = self._get_postgres_connection_string()
            alias = self.duckdb_alias
            self._duckdb_conn.execute(
                f"""
                ATTACH '{conn_str}' AS {alias} (TYPE postgres, READ_WRITE, SCHEMA 'public');
            """
            )
            self._logger.debug(f"Attached destination as '{alias}'")

            # Attach source PostgreSQL database if source config is provided
            if self._source_config:
                try:
                    source_conn_str = self._get_source_connection_string()
                    source_alias = self.source_duckdb_alias

                    if source_conn_str and source_alias:
                        self._duckdb_conn.execute(
                            f"""
                            ATTACH '{source_conn_str}' AS {source_alias} (TYPE postgres, READ_ONLY, SCHEMA 'public');
                        """
                        )
                        self._logger.info(
                            f"Attached source database as '{source_alias}' (READ_ONLY)"
                        )
                except Exception as source_error:
                    # Log warning but don't fail destination initialization
                    self._logger.warning(
                        f"Failed to attach source database: {source_error}. "
                        f"Source tables will not be available for joins in custom SQL."
                    )

            # Also create direct psycopg2 connection for DDL operations
            self._pg_conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
            )
            self._pg_conn.autocommit = True

            self._is_initialized = True
            self._logger.info(
                f"PostgreSQL destination initialized: {self._config.name}"
            )

        except Exception as e:
            # Sanitize error message to avoid exposing credentials
            sanitized_msg = sanitize_for_db(e, self._config.name, "POSTGRES")
            raise DestinationException(
                sanitized_msg,
                {"destination_id": self._config.id},
            )

    def _get_table_schema(self, table_name: str) -> dict[str, dict]:
        """
        Get column info from target PostgreSQL table.

        Args:
            table_name: Target table name

        Returns:
            Dict mapping column_name -> {
                'type': postgres_type,
                'scale': numeric_scale (for decimals),
                'udt_name': underlying type name (for PostGIS types)
            }
        """
        with self._pg_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type, numeric_scale, udt_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """,
                (self.schema, table_name),
            )

            schema = {}
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1].lower()
                scale = row[2]
                udt_name = row[3].lower() if row[3] else None

                # For USER-DEFINED types (like PostGIS), use udt_name as the type
                if data_type == "user-defined" and udt_name:
                    data_type = udt_name

                schema[col_name] = {
                    "type": data_type,
                    "scale": scale,
                    "udt_name": udt_name,
                }
            return schema

    def _convert_debezium_value(self, value, column_name: str, column_info: dict):
        """
        Convert Debezium-encoded value to proper Python type for PostgreSQL.

        Args:
            value: Raw value from Debezium
            column_name: Column name
            column_info: Column metadata (type, scale)

        Returns:
            Converted value ready for PostgreSQL
        """
        import datetime
        import base64
        from decimal import Decimal

        if value is None:
            return None

        pg_type = column_info.get("type", "text")

        try:
            if pg_type == "date":
                # Debezium sends DATE as days since epoch
                if isinstance(value, int):
                    return datetime.date(1970, 1, 1) + datetime.timedelta(days=value)
                return value

            elif pg_type in (
                "timestamp without time zone",
                "timestamp with time zone",
                "timestamp",
            ):
                # Debezium sends TIMESTAMP as microseconds since epoch
                if isinstance(value, int):
                    return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                        microseconds=value
                    )
                return value

            elif pg_type == "time without time zone":
                # Debezium sends TIME as microseconds since midnight
                if isinstance(value, int):
                    return (
                        datetime.datetime.min + datetime.timedelta(microseconds=value)
                    ).time()
                return value

            elif pg_type in ("time with time zone", "timetz"):
                # Debezium sends TIME WITH TIME ZONE as microseconds since midnight
                # The timezone offset is preserved in the PostgreSQL column metadata
                if isinstance(value, int):
                    return (
                        datetime.datetime.min + datetime.timedelta(microseconds=value)
                    ).time()
                return value

            elif pg_type in ("numeric", "decimal"):
                # Debezium sends NUMERIC/DECIMAL as Base64-encoded big-endian byte array
                if (
                    isinstance(value, str)
                    and not value.replace(".", "").replace("-", "").isdigit()
                ):
                    try:
                        # Decode Base64 to bytes
                        decoded_bytes = base64.b64decode(value)
                        # Convert big-endian bytes to integer
                        int_value = int.from_bytes(
                            decoded_bytes, byteorder="big", signed=True
                        )

                        # Use actual schema scale if available, otherwise heuristic
                        scale = column_info.get("scale")
                        if scale is None:
                            # Fallback heuristic
                            if (
                                "price" in column_name.lower()
                                or "rate" in column_name.lower()
                            ):
                                scale = 4
                            elif (
                                "pct" in column_name.lower()
                                or "percent" in column_name.lower()
                            ):
                                scale = 2
                            else:
                                scale = 2

                        return Decimal(int_value) / Decimal(10**scale)
                    except Exception as e:
                        self._logger.warning(
                            f"Failed to decode Base64 numeric for {column_name}: {e}"
                        )
                        return value
                elif isinstance(value, (int, float)):
                    return Decimal(str(value))
                return value

            elif pg_type in ("integer", "bigint", "smallint"):
                return int(value) if value is not None else None

            elif pg_type in ("real", "double precision"):
                return float(value) if value is not None else None

            elif pg_type == "boolean":
                return bool(value) if value is not None else None

            elif pg_type in ("json", "jsonb"):
                # Convert to JSON string - PostgreSQL can implicitly convert JSON string to jsonb
                import json

                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                elif isinstance(value, str):
                    # Already a JSON string - validate and return
                    try:
                        json.loads(value)  # Validate it's valid JSON
                        return value
                    except:
                        return json.dumps(value)  # Wrap as JSON string
                else:
                    return json.dumps(value)

            elif pg_type == "ARRAY" or "[]" in str(pg_type):
                # Convert to PostgreSQL array literal format: {a,b,c}
                if isinstance(value, list):
                    # Format as PostgreSQL array literal
                    formatted_items = []
                    for item in value:
                        if item is None:
                            formatted_items.append("NULL")
                        elif isinstance(item, str):
                            # Escape quotes and wrap in quotes
                            escaped = item.replace('"', '\\"')
                            formatted_items.append(f'"{escaped}"')
                        else:
                            formatted_items.append(str(item))
                    return "{" + ",".join(formatted_items) + "}"
                elif isinstance(value, str):
                    # Already a PostgreSQL array string
                    if value.startswith("{") and value.endswith("}"):
                        return value
                    return value
                return value

            elif pg_type in ("geometry", "geography", "point", "polygon", "linestring"):
                # Handle PostGIS types - Debezium sends dict with 'wkb' and 'srid'
                if isinstance(value, dict) and "wkb" in value:
                    try:
                        # Value is {'wkb': 'Base64...', 'srid': 4326}
                        wkb_b64 = value["wkb"]
                        if not wkb_b64:
                            return None

                        # Decode Base64 WKB to bytes
                        wkb_bytes = base64.b64decode(wkb_b64)

                        # Convert bytes to Hex string for PostgreSQL
                        # PostgreSQL expects hex string for WKB in generic handling
                        return wkb_bytes.hex()
                    except Exception as e:
                        self._logger.warning(f"Failed to process geometry WKB: {e}")
                        return None
                return value

            elif pg_type == "ARRAY" or "[]" in str(pg_type):
                # Handle array types
                return value

            else:
                # Default: return as-is (text, varchar, etc.)
                return value

        except Exception as e:
            self._logger.warning(
                f"Failed to convert {column_name} ({pg_type}): {e}, using raw value"
            )
            return value

    def _parse_filter_sql(self, filter_sql: str) -> list[str]:
        """
        Parse filter_sql into list of WHERE clauses.

        Supports two formats:
        1. Legacy: "column_1 = '11';column_2>1" (semicolon-separated, all AND)
        2. JSON v2: {"version": 2, "groups": [...], "interLogic": [...]}

        Args:
            filter_sql: Semicolon-separated filter conditions or JSON v2 string

        Returns:
            List of individual filter conditions (for legacy format)
        """
        if not filter_sql:
            return []

        # Try JSON v2 format first
        try:
            import json
            parsed = json.loads(filter_sql)
            if isinstance(parsed, dict) and parsed.get("version") == 2:
                # V2 format - return empty list (use _build_where_clause_v2 instead)
                return []
        except (json.JSONDecodeError, TypeError):
            pass

        # Legacy format: Split by semicolon and strip whitespace
        filters = [f.strip() for f in filter_sql.split(";") if f.strip()]
        return filters

    def _build_where_clause_from_filter_sql(self, filter_sql: str) -> str:
        """
        Build a complete WHERE clause string from filter_sql.

        Supports both legacy semicolon format and JSON v2 format with 
        AND/OR logic, grouping, and IN operator.

        Args:
            filter_sql: Filter SQL string (legacy or JSON v2)

        Returns:
            Complete WHERE clause (without the WHERE keyword), or empty string
        """
        if not filter_sql:
            return ""

        # Try JSON v2 format first
        try:
            import json
            parsed = json.loads(filter_sql)
            if isinstance(parsed, dict) and parsed.get("version") == 2:
                return self._build_where_clause_v2(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

        # Legacy format: semicolon-separated, all AND
        filters = [f.strip() for f in filter_sql.split(";") if f.strip()]
        if not filters:
            return ""
        return " AND ".join([f"({condition})" for condition in filters])

    def _build_where_clause_v2(self, parsed: dict) -> str:
        """
        Build WHERE clause from JSON v2 filter format.

        Supports grouping with AND/OR between groups and within groups,
        and the IN operator.

        Args:
            parsed: Parsed JSON v2 filter dict

        Returns:
            WHERE clause string (without WHERE keyword)
        """
        groups = parsed.get("groups", [])
        inter_logic = parsed.get("interLogic", [])

        if not groups:
            return ""

        group_clauses = []
        for group in groups:
            conditions = group.get("conditions", [])
            intra_logic = group.get("intraLogic", "AND")

            if not conditions:
                continue

            clauses = []
            for cond in conditions:
                column = cond.get("column", "")
                operator = cond.get("operator", "")
                value = cond.get("value", "")
                value2 = cond.get("value2", "")

                if not column:
                    continue

                clause = self._build_single_clause(column, operator, value, value2)
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

        # Join groups with inter-logic operators
        result = group_clauses[0]
        for i in range(1, len(group_clauses)):
            logic = inter_logic[i - 1] if i - 1 < len(inter_logic) else "AND"
            result = f"{result} {logic} {group_clauses[i]}"

        return result

    def _build_single_clause(self, column: str, operator: str, value: str, value2: str = "") -> str:
        """
        Build a single SQL clause from filter components.

        Args:
            column: Column name
            operator: SQL operator
            value: Filter value
            value2: Second value (for BETWEEN)

        Returns:
            SQL clause string
        """
        op_upper = operator.upper().strip()

        if op_upper in ("IS NULL", "IS NOT NULL"):
            return f"{column} {op_upper}"

        if not value and op_upper not in ("IS NULL", "IS NOT NULL"):
            return ""

        if op_upper == "BETWEEN" and value and value2:
            q_val = self._quote_filter_value(value)
            q_val2 = self._quote_filter_value(value2)
            return f"{column} BETWEEN {q_val} AND {q_val2}"

        if op_upper in ("LIKE", "ILIKE"):
            return f"{column} {op_upper} '%{value}%'"

        if op_upper == "IN":
            # Values are comma-separated
            values = [v.strip() for v in value.split(",") if v.strip()]
            quoted = [self._quote_filter_value(v) for v in values]
            return f"{column} IN ({', '.join(quoted)})"

        return f"{column} {operator} {self._quote_filter_value(value)}"

    def _quote_filter_value(self, value: str) -> str:
        """Quote a filter value - numeric values are unquoted, strings are quoted."""
        try:
            float(value)
            return value
        except (ValueError, TypeError):
            return f"'{value}'"

    def _parse_debezium_field_types(
        self, schema: Optional[dict]
    ) -> dict[str, dict]:
        """
        Parse the Debezium envelope schema to extract column type metadata.

        Debezium wraps each CDC event in an envelope with a ``schema`` field
        that describes column types (``after.fields``).  This method extracts
        that info so we can coerce raw values to proper Python types before
        building a PyArrow table — ensuring DuckDB sees correct column types
        for custom SQL aggregations (SUM, AVG, etc.).

        Debezium schema structure::

            {
                "type": "struct",
                "fields": [
                    {"field": "before", ...},
                    {"field": "after", "fields": [
                        {"field": "amount", "type": "bytes",
                         "name": "org.apache.kafka.connect.data.Decimal",
                         "parameters": {"scale": "2", ...}},
                        {"field": "employee_id", "type": "int32"},
                        ...
                    ]},
                    {"field": "op", ...},
                    ...
                ]
            }

        Args:
            schema: Debezium envelope schema dict (from CDCRecord.schema)

        Returns:
            dict mapping column_name → {type, name, parameters}
        """
        if not schema or not isinstance(schema, dict):
            return {}

        try:
            fields = schema.get("fields", [])
            for field in fields:
                if field.get("field") == "after":
                    result = {}
                    for col_field in field.get("fields", []):
                        col_name = col_field.get("field")
                        if col_name:
                            result[col_name] = {
                                "type": col_field.get("type", ""),
                                "name": col_field.get("name", ""),
                                "parameters": col_field.get("parameters", {}),
                            }
                    return result
        except Exception:
            pass
        return {}

    def _coerce_values_for_duckdb(
        self,
        values: list,
        dbz_info: dict,
        col_name: str,
    ) -> list:
        """
        Convert raw Debezium-encoded values to proper Python types so that
        PyArrow infers correct DuckDB column types.

        Handles all major Debezium logical types:

        - ``org.apache.kafka.connect.data.Decimal``  (base64 or string → float)
        - ``io.debezium.time.Date``                  (int days → datetime.date)
        - ``io.debezium.time.MicroTimestamp``         (int μs → datetime)
        - ``io.debezium.time.NanoTimestamp``          (int ns → datetime)
        - ``io.debezium.time.Timestamp``              (int ms → datetime)
        - ``io.debezium.time.MicroTime``              (int μs → time)
        - ``io.debezium.time.Time``                   (int ms → time)
        - Primitive types: int16/int32/int64, float/double, boolean

        Args:
            values: Raw column values from Debezium
            dbz_info: Field type metadata from ``_parse_debezium_field_types``
            col_name: Column name (for logging)

        Returns:
            Coerced column values with proper Python types
        """
        import base64
        import datetime
        from decimal import Decimal

        dbz_type = dbz_info.get("type", "")
        dbz_name = dbz_info.get("name", "")
        params = dbz_info.get("parameters", {})

        # ── DECIMAL / NUMERIC ──
        if dbz_name == "org.apache.kafka.connect.data.Decimal":
            scale = int(params.get("scale", 0))
            result = []
            for v in values:
                if v is None:
                    result.append(None)
                elif isinstance(v, (int, float)):
                    result.append(float(v))
                elif isinstance(v, str):
                    # Try plain numeric string first
                    try:
                        result.append(float(v))
                    except ValueError:
                        # Base64-encoded big-endian byte array
                        try:
                            decoded = base64.b64decode(v)
                            int_val = int.from_bytes(
                                decoded, byteorder="big", signed=True
                            )
                            result.append(
                                float(Decimal(int_val) / Decimal(10**scale))
                            )
                        except Exception:
                            result.append(None)
                else:
                    result.append(v)
            return result

        # ── DATE (days since epoch) ──
        if dbz_name == "io.debezium.time.Date":
            epoch = datetime.date(1970, 1, 1)
            return [
                (epoch + datetime.timedelta(days=v))
                if isinstance(v, int) else v
                for v in values
            ]

        # ── TIMESTAMP (microseconds since epoch) ──
        if dbz_name in (
            "io.debezium.time.MicroTimestamp",
            "io.debezium.time.NanoTimestamp",
        ):
            epoch = datetime.datetime(1970, 1, 1)
            result = []
            for v in values:
                if v is None:
                    result.append(None)
                elif isinstance(v, int):
                    if "Nano" in dbz_name:
                        result.append(
                            epoch + datetime.timedelta(microseconds=v // 1000)
                        )
                    else:
                        result.append(
                            epoch + datetime.timedelta(microseconds=v)
                        )
                else:
                    result.append(v)
            return result

        # ── TIMESTAMP (milliseconds since epoch) ──
        if dbz_name == "io.debezium.time.Timestamp":
            epoch = datetime.datetime(1970, 1, 1)
            return [
                (epoch + datetime.timedelta(milliseconds=v))
                if isinstance(v, int) else v
                for v in values
            ]

        # ── TIME (microseconds/milliseconds since midnight) ──
        if dbz_name in ("io.debezium.time.MicroTime", "io.debezium.time.Time"):
            result = []
            for v in values:
                if v is None:
                    result.append(None)
                elif isinstance(v, int):
                    if "Micro" in dbz_name:
                        td = datetime.timedelta(microseconds=v)
                    else:
                        td = datetime.timedelta(milliseconds=v)
                    result.append((datetime.datetime.min + td).time())
                else:
                    result.append(v)
            return result

        # ── Primitive int types ──
        if dbz_type in ("int16", "int32") and not dbz_name:
            return [int(v) if v is not None else None for v in values]

        if dbz_type == "int64" and not dbz_name:
            return [int(v) if v is not None else None for v in values]

        # ── Primitive float types ──
        if dbz_type in ("float32", "float", "float64", "double"):
            return [float(v) if v is not None else None for v in values]

        # ── Boolean ──
        if dbz_type == "boolean":
            return [bool(v) if v is not None else None for v in values]

        # Default: return as-is (string, bytes, etc.)
        return values

    def _auto_coerce_numeric_column(self, values: list) -> list:
        """
        Fallback auto-detection for columns when no Debezium schema is
        available.  Only coerces if ALL non-None values are numeric strings
        (contain a decimal point to avoid false positives with IDs/codes).

        Args:
            values: Raw column values

        Returns:
            Coerced values (float) or originals if not numeric
        """
        if not values:
            return values

        samples = [v for v in values if v is not None]
        if not samples or not all(isinstance(s, str) for s in samples):
            return values

        # Only coerce strings with a decimal point (avoids IDs, codes)
        has_dot = any("." in s for s in samples)
        if not has_dot:
            return values

        try:
            for s in samples:
                float(s)
            return [float(v) if v is not None else None for v in values]
        except (ValueError, TypeError):
            return values

    def _insert_batch_to_duckdb(
        self,
        records: list[CDCRecord],
        table_name: str,
    ) -> None:
        """
        Insert CDC records into a DuckDB table with proper column types.

        Uses the Debezium schema attached to CDC records to coerce raw values
        (base64 decimals, epoch timestamps, etc.) into proper Python types
        *before* building the PyArrow table.  This ensures DuckDB sees correct
        column types — critical when custom SQL uses aggregations like
        ``SUM(amount)`` or ``CAST(ts AS DATE)``.

        Falls back to auto-detection of numeric strings when Debezium schema
        is not available.

        Args:
            records: CDC records to insert
            table_name: Original source table name (e.g., 'tbl_sales')
        """
        if not records:
            return

        # Sanitize table name for DuckDB
        safe_table_name = table_name.replace(".", "_").replace("-", "_")

        # Drop existing table if exists
        self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {safe_table_name}")

        # Convert records to columnar format
        data = [record.value for record in records]
        columns = list(data[0].keys())

        # Parse Debezium schema for type-aware ingestion
        debezium_types = self._parse_debezium_field_types(records[0].schema)

        arrays = {}
        for col in columns:
            raw_values = [row.get(col) for row in data]
            dbz_info = debezium_types.get(col)

            if dbz_info:
                # Schema-aware coercion (primary path)
                arrays[col] = self._coerce_values_for_duckdb(
                    raw_values, dbz_info, col
                )
            elif not debezium_types:
                # No schema at all — try auto-detecting numeric strings
                arrays[col] = self._auto_coerce_numeric_column(raw_values)
            else:
                # Schema exists but this column isn't in it — keep raw
                arrays[col] = raw_values

        # Create Arrow table — DuckDB's native format (zero-copy)
        arrow_table = pa.table(arrays)

        # Register the Arrow table and create DuckDB table from it
        self._duckdb_conn.execute(
            f"CREATE TABLE {safe_table_name} AS SELECT * FROM arrow_table"
        )

        self._logger.debug(
            f"Inserted {len(records)} records into DuckDB table "
            f"'{safe_table_name}' (PyArrow, "
            f"schema_aware={'yes' if debezium_types else 'auto'})"
        )

    def _apply_filters_in_duckdb(
        self,
        table_name: str,
        filter_sql: Optional[str],
    ) -> None:
        """
        Apply filter SQL directly in DuckDB by deleting non-matching rows.

        Supports both legacy format and JSON v2 format with AND/OR grouping
        and IN operator.

        Args:
            table_name: DuckDB table name
            filter_sql: Filter conditions (legacy semicolon or JSON v2)
        """
        if not filter_sql:
            return

        # Sanitize table name
        safe_table_name = table_name.replace(".", "_").replace("-", "_")

        # Build WHERE clause using the unified method
        where_conditions = self._build_where_clause_from_filter_sql(filter_sql)
        if not where_conditions:
            return

        # Delete rows that DON'T match the filter (keep only matching rows)
        delete_sql = f"""
            DELETE FROM {safe_table_name}
            WHERE NOT ({where_conditions})
        """

        self._logger.debug(f"Applying filter in DuckDB: {delete_sql}")
        self._duckdb_conn.execute(delete_sql)

    def _execute_custom_sql_from_duckdb(
        self,
        table_name: str,
        custom_sql: Optional[str],
    ) -> list[dict]:
        """
        Execute custom SQL on DuckDB table.

        User can directly reference table name in their SQL.
        If table name has dots (schema.table), it's already sanitized to underscores.

        Args:
            table_name: DuckDB table name (e.g., 'tbl_sales')
            custom_sql: User's custom SQL query

        Returns:
            Transformed records as dicts
        """
        # Sanitize table name
        safe_table_name = table_name.replace(".", "_").replace("-", "_")

        if not custom_sql:
            # Return all rows from table
            sql = f"SELECT * FROM {safe_table_name}"
        else:
            # Replace original table name with sanitized version in user's SQL
            # This allows users to write: SELECT * FROM tbl_sales
            # Even if the actual DuckDB table is tbl_sales
            sql = custom_sql.replace(table_name, safe_table_name)

            # Also handle case where table name has schema prefix
            if "." in table_name:
                bare_name = table_name.split(".")[-1]
                sql = sql.replace(bare_name, safe_table_name)

        self._logger.debug(f"Executing custom SQL: {sql}")
        result = self._duckdb_conn.execute(sql).fetchall()

        # Get column names
        result_columns = [desc[0] for desc in self._duckdb_conn.description]

        # Convert to dicts
        transformed = []
        for row in result:
            transformed.append(dict(zip(result_columns, row)))

        return transformed

    def _apply_filters(
        self,
        records: list[CDCRecord],
        filter_sql: Optional[str],
    ) -> list[CDCRecord]:
        """
        Apply filter conditions to records (legacy Python-based filtering).

        DEPRECATED: Use _apply_filters_in_duckdb instead.

        Args:
            records: CDC records to filter
            filter_sql: Filter conditions (semicolon-separated or JSON v2)

        Returns:
            Filtered records
        """
        if not filter_sql:
            return records

        # For v2 format, try to use DuckDB filtering
        try:
            import json
            parsed = json.loads(filter_sql)
            if isinstance(parsed, dict) and parsed.get("version") == 2:
                # V2 format - fall through to DuckDB-based filtering
                # This legacy method doesn't support v2, return all records
                self._logger.warning(
                    "V2 filter format not supported in legacy Python filtering, "
                    "use DuckDB-based filtering instead"
                )
                return records
        except (json.JSONDecodeError, TypeError):
            pass

        filters = self._parse_filter_sql(filter_sql)
        if not filters:
            return records

        filtered = []
        for record in records:
            if self._record_matches_filters(record.value, filters):
                filtered.append(record)

        return filtered

    def _record_matches_filters(self, record: dict, filters: list[str]) -> bool:
        """
        Check if a record matches all filter conditions.

        Supports basic conditions: =, !=, >, <, >=, <=

        Args:
            record: Record data dict
            filters: List of filter conditions

        Returns:
            True if record matches all conditions
        """
        for condition in filters:
            # Parse condition (simple implementation)
            # Supports: column = 'value', column > 1, column >= 1, etc.
            match = re.match(r"(\w+)\s*(=|!=|<>|>|<|>=|<=)\s*(.+)", condition.strip())

            if not match:
                self._logger.warning(f"Could not parse filter condition: {condition}")
                continue

            column, operator, value = match.groups()

            # Get record value
            if column not in record:
                return False

            record_value = record[column]

            # Parse comparison value (remove quotes if string)
            value = value.strip()
            if value.startswith("'") and value.endswith("'"):
                compare_value = value[1:-1]
            else:
                try:
                    compare_value = float(value) if "." in value else int(value)
                except ValueError:
                    compare_value = value

            # Perform comparison
            try:
                if operator == "=":
                    if record_value != compare_value:
                        return False
                elif operator in ("!=", "<>"):
                    if record_value == compare_value:
                        return False
                elif operator == ">":
                    if not (record_value > compare_value):
                        return False
                elif operator == "<":
                    if not (record_value < compare_value):
                        return False
                elif operator == ">=":
                    if not (record_value >= compare_value):
                        return False
                elif operator == "<=":
                    if not (record_value <= compare_value):
                        return False
            except TypeError:
                # Type mismatch in comparison
                return False

        return True

    def _execute_custom_sql(
        self,
        records: list[CDCRecord],
        table_name: str,
        custom_sql: str,
    ) -> list[dict]:
        """
        Execute custom SQL transformation on records (legacy method).

        DEPRECATED: Use _execute_custom_sql_from_duckdb instead.

        Creates a temporary table with records, then executes the custom SQL.
        The custom SQL can reference the table by its original name.

        Args:
            records: CDC records
            table_name: Source table name (for reference in SQL)
            custom_sql: Custom SQL query

        Returns:
            Transformed records as dicts
        """
        if not custom_sql or not records:
            return [r.value for r in records]

        try:
            # Create temporary table with record data
            temp_table = f"_temp_{table_name.replace('.', '_')}"

            # Get columns from first record
            columns = list(records[0].value.keys())

            # Create temp table
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            # Build CREATE TABLE statement
            col_defs = ", ".join([f'"{c}" VARCHAR' for c in columns])
            self._duckdb_conn.execute(f"CREATE TABLE {temp_table} ({col_defs})")

            # Insert records
            for record in records:
                values = [str(record.value.get(c, "")) for c in columns]
                placeholders = ", ".join(["?" for _ in columns])
                self._duckdb_conn.execute(
                    f"INSERT INTO {temp_table} VALUES ({placeholders})", values
                )

            # Replace table name in custom SQL
            # Handle both "table_name" and "schema.table_name" formats
            sql = custom_sql.replace(table_name, temp_table)
            if "." in table_name:
                bare_name = table_name.split(".")[-1]
                sql = sql.replace(bare_name, temp_table)

            # Execute custom SQL
            result = self._duckdb_conn.execute(sql).fetchall()

            # Get column names from result
            result_columns = [desc[0] for desc in self._duckdb_conn.description]

            # Convert to dicts
            transformed = []
            for row in result:
                transformed.append(dict(zip(result_columns, row)))

            # Cleanup temp table
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            return transformed

        except Exception as e:
            self._logger.error(f"Custom SQL execution failed: {e}")
            # Fall back to original records
            return [r.value for r in records]

    def _get_primary_key_columns(self, record: CDCRecord) -> list[str]:
        """
        Get primary key columns from record key.

        Args:
            record: CDC record

        Returns:
            List of primary key column names
        """
        if record.key:
            return list(record.key.keys())
        # Default to first column if no key info
        return list(record.value.keys())[:1]

    def _get_target_primary_key(self, target_table: str) -> list[str]:
        """
        Get primary key columns from the destination PostgreSQL table.

        Queries ``pg_index`` + ``pg_attribute`` to retrieve the actual PK
        columns of the target table.  This is essential when custom SQL
        transforms data into a different schema (e.g., source PK is
        ``transaction_id`` but aggregate target PK is ``report_date``).

        Args:
            target_table: Target table name on the destination database

        Returns:
            List of primary key column names, or empty list if no PK found
        """
        try:
            with self._pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a
                        ON a.attrelid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                      AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                    """,
                    (f"{self.schema}.{target_table}",),
                )
                pk_cols = [row[0] for row in cursor.fetchall()]
                if pk_cols:
                    self._logger.debug(
                        f"Detected target PK for '{target_table}': {pk_cols}"
                    )
                return pk_cols
        except Exception as e:
            self._logger.warning(
                f"Failed to detect PK for target table '{target_table}': {e}"
            )
            return []

    def _merge_into_postgres(
        self,
        records: list[dict],
        target_table: str,
        key_columns: list[str],
    ) -> int:
        """
        Perform MERGE INTO operation on PostgreSQL via DuckDB.

        Args:
            records: Records to merge
            target_table: Target table name
            key_columns: Primary key columns for merge condition

        Returns:
            Number of affected rows
        """
        if not records:
            return 0

        self._logger.info(
            f"Starting MERGE INTO for {len(records)} records to table '{target_table}'"
        )

        try:
            # Get target table schema for type conversion
            table_schema = self._get_table_schema(target_table)
            self._logger.debug(f"Target schema for {target_table}: {table_schema}")

            # Create temporary source table
            temp_source = "_merge_source"
            columns = list(records[0].keys())

            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")

            # Convert first record values using schema mapping
            def convert_record(record, log_first=False):
                converted = []
                for c in columns:
                    raw_value = record.get(c)

                    # Get column info from schema (default to text if not found)
                    col_info = table_schema.get(c, {"type": "text"})
                    pg_type = col_info.get("type", "text")

                    if log_first:
                        self._logger.info(
                            f"  Column '{c}': raw={repr(raw_value)} (type={type(raw_value).__name__}) -> pg_type='{pg_type}' scale={col_info.get('scale')}"
                        )

                    # Pass full column info to converter
                    converted_value = self._convert_debezium_value(
                        raw_value, c, col_info
                    )

                    # Special handling for TIME WITH TIME ZONE: normalize 'Z' to '+00:00'
                    # PostgreSQL doesn't accept 'Z' suffix for timetz, only explicit offsets
                    if pg_type in ("time with time zone", "timetz") and isinstance(
                        converted_value, str
                    ):
                        if converted_value.endswith("Z"):
                            converted_value = converted_value[:-1] + "+00:00"

                    converted.append(converted_value)
                return converted

            # Map PostgreSQL types to DuckDB types for explicit table creation
            def pg_to_duckdb_type(pg_type):
                if pg_type in ("date",):
                    return "DATE"
                if pg_type in (
                    "timestamp",
                    "timestamp without time zone",
                    "timestamp with time zone",
                ):
                    return "TIMESTAMP"
                if pg_type in ("time", "time without time zone"):
                    return "TIME"
                if pg_type in ("time with time zone", "timetz"):
                    return "VARCHAR"  # Store as string to avoid DuckDB cast errors
                if pg_type in ("integer", "int", "serial"):
                    return "INTEGER"
                if pg_type in ("bigint", "bigserial"):
                    return "BIGINT"
                if pg_type in ("smallint",):
                    return "SMALLINT"
                if pg_type in ("boolean", "bool"):
                    return "BOOLEAN"
                if pg_type in ("real", "float4"):
                    return "FLOAT"
                if pg_type in ("double precision", "float8", "numeric", "decimal"):
                    return "DOUBLE"
                # Store complex types as VARCHAR - PostgreSQL will handle implicit conversion
                if pg_type in ("json", "jsonb"):
                    return "json"  # DuckDB has JSON type
                if pg_type == "text[]":
                    return "VARCHAR[]"  # Array of text
                if pg_type == "integer[]":
                    return "INTEGER[]"  # Array of integers
                if "[]" in pg_type:
                    return "VARCHAR[]"
                if pg_type in ("geometry", "geography"):
                    return "VARCHAR"  # Store as hex WKB
                return "VARCHAR"  # Default fallback

            # Create explicit column definitions
            col_defs = []
            for c in columns:
                col_info = table_schema.get(c, {"type": "text"})
                pg_type = col_info.get("type", "text")
                duck_type = pg_to_duckdb_type(pg_type)
                col_defs.append(f'"{c}" {duck_type}')

            col_def_str = ", ".join(col_defs)

            # Use persistent staging table: create once, truncate on reuse
            # Key includes target_table to avoid schema conflicts across tables
            staging_key = f"{temp_source}_{target_table}"
            if staging_key in self._staging_tables:
                try:
                    self._duckdb_conn.execute(f"DELETE FROM {temp_source}")
                except Exception:
                    # Schema might have changed, recreate
                    self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")
                    self._duckdb_conn.execute(f"CREATE TABLE {temp_source} ({col_def_str})")
            else:
                self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")
                self._duckdb_conn.execute(f"CREATE TABLE {temp_source} ({col_def_str})")
                self._staging_tables.add(staging_key)

            # Insert values using executemany for bulk performance
            placeholders = ", ".join(["?" for _ in columns])

            # Convert all records first, then bulk insert
            all_values = [convert_record(record, log_first=(i == 0)) for i, record in enumerate(records)]
            self._duckdb_conn.executemany(
                f"INSERT INTO {temp_source} VALUES ({placeholders})", all_values
            )

            # Build DELETE + INSERT pattern instead of MERGE INTO
            # DuckDB's postgres_scanner strips type casts when translating MERGE to UPDATE,
            # causing type mismatch errors. DELETE+INSERT with explicit casts is more reliable.

            full_table = f"{self.duckdb_alias}.{self.schema}.{target_table}"

            # Helper to get valid PG cast type compatible with DuckDB parser
            def get_pg_cast_type(c):
                col_info = table_schema.get(c, {"type": "text"})
                pg_type = col_info.get("type", "text")
                udt_name = col_info.get("udt_name")

                # Handle arrays: Map internal names like _text to TEXT[] for DuckDB
                if pg_type == "array" and udt_name:
                    if udt_name.startswith("_"):
                        inner = udt_name[1:]
                        if inner == "text":
                            return "TEXT[]"
                        if inner == "varchar" or inner == "bpchar":
                            return "VARCHAR[]"
                        if inner == "int2":
                            return "SMALLINT[]"
                        if inner == "int4":
                            return "INTEGER[]"
                        if inner == "int8":
                            return "BIGINT[]"
                        if inner == "float4":
                            return "FLOAT[]"
                        if inner == "float8":
                            return "DOUBLE[]"
                        if inner == "bool":
                            return "BOOLEAN[]"
                        return f"{inner}[]"
                    return "VARCHAR[]"

                # Handle JSONB/JSON
                if pg_type in ("json", "jsonb"):
                    return "JSON"

                # Handle TIME WITH TIME ZONE (use VARCHAR to avoid cast errors)
                if pg_type in ("time with time zone", "timetz"):
                    return "VARCHAR"

                # Handle Geospatial types (map to VARCHAR for DuckDB compatibility)
                if pg_type in (
                    "geography",
                    "geometry",
                    "point",
                    "polygon",
                    "linestring",
                ):
                    return "VARCHAR"

                # Use base type for others
                return pg_type

            # 1. DELETE existing rows that match keys
            if key_columns:
                pk_conditions = " AND ".join(
                    [f'target."{k}" = source."{k}"' for k in key_columns]
                )

                # DuckDB doesn't support DELETE ... FROM ... USING directly for Postgres attached tables
                # We use a subquery with IN or a JOIN-like condition if possible,
                # but the most reliable way in DuckDB for this is a combined DELETE via the scanner

                # However, for simplicity and reliability with common PKs (like a single ID):
                if len(key_columns) == 1:
                    k = key_columns[0]
                    # Get the right PG type for casting in the subquery
                    cast_type = get_pg_cast_type(k)

                    delete_sql = f"""
                        DELETE FROM {full_table} 
                        WHERE "{k}" IN (SELECT "{k}"::{cast_type} FROM {temp_source})
                    """
                else:
                    # Multiple PKs - use tuple comparison (supported by PG)
                    pk_list = ", ".join([f'"{k}"' for k in key_columns])

                    # Construct casted select list for DuckDB
                    select_pks = ", ".join(
                        [f'"{k}"::{get_pg_cast_type(k)}' for k in key_columns]
                    )

                    delete_sql = f"""
                        DELETE FROM {full_table}
                        WHERE ({pk_list}) IN (SELECT {select_pks} FROM {temp_source})
                    """

                self._logger.debug(f"Executing DELETE: {delete_sql}")
                self._duckdb_conn.execute(delete_sql)

            # 2. INSERT new records
            insert_cols = ", ".join([f'"{c}"' for c in columns])

            # Construct SELECT with explicit casts to original PG types
            select_list = ", ".join([f'"{c}"::{get_pg_cast_type(c)}' for c in columns])

            insert_sql = f"""
                INSERT INTO {full_table} ({insert_cols})
                SELECT {select_list} FROM {temp_source}
            """

            self._logger.debug(f"Executing INSERT: {insert_sql}")
            self._duckdb_conn.execute(insert_sql)

            # Cleanup
            self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_source}")

            return len(records)

        except Exception as e:
            self._logger.error(f"PostgreSQL sync failed: {e}")
            raise DestinationException(f"PostgreSQL sync failed: {e}")

    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write batch of records to PostgreSQL.

        New Flow:
        1. Insert batch into DuckDB with original table name
        2. Apply filter_sql in DuckDB (if defined)
        3. Apply custom_sql transformation in DuckDB (if defined)
        4. MERGE INTO destination table
        5. Cleanup DuckDB table

        Args:
            records: CDC records to write
            table_sync: Table sync configuration

        Returns:
            Number of records written
        """
        if not self._is_initialized:
            self.initialize()

        if not records:
            return 0

        source_table = table_sync.table_name  # e.g., 'tbl_sales'
        target_table = table_sync.table_name_target
        safe_table_name = source_table.replace(".", "_").replace("-", "_")

        try:
            # Step 1: Insert batch into DuckDB with original table name
            self._insert_batch_to_duckdb(records, source_table)

            # Step 2: Apply filter SQL in DuckDB (modifies table in-place)
            if table_sync.filter_sql:
                self._apply_filters_in_duckdb(source_table, table_sync.filter_sql)

            # Step 3: Execute custom SQL or select all
            transformed = self._execute_custom_sql_from_duckdb(
                source_table, table_sync.custom_sql
            )

            # Validation: Filter out rows where all values are None
            # This prevents inserting empty rows
            valid_rows = []
            skipped_count = 0
            for row in transformed:
                # Check if any value in the row is not None
                if any(v is not None for v in row.values()):
                    valid_rows.append(row)
                else:
                    skipped_count += 1

            if skipped_count > 0:
                self._logger.warning(
                    f"Skipped {skipped_count} rows with all null values for {target_table}"
                )

            if not valid_rows:
                self._logger.info(
                    f"No valid rows to write to {target_table} (all rows had null values)"
                )
                return 0

            transformed = valid_rows

            # Validate target table exists on destination before MERGE
            target_schema = self._get_table_schema(target_table)
            if not target_schema:
                raise DestinationException(
                    f"Target table '{self.schema}.{target_table}' does not exist "
                    f"on destination '{self._config.name}'. "
                    f"Please verify 'table_name_target' is set correctly in "
                    f"the pipeline table sync configuration. "
                    f"Source table: '{source_table}', "
                    f"configured target: '{target_table}'."
                )

            # Determine primary key columns for MERGE
            # Priority: custom primary keys > target table PK (for custom SQL) > source CDC PK
            if table_sync.primary_key_column_target:
                # User has specified custom primary key columns
                custom_keys = [
                    k.strip()
                    for k in table_sync.primary_key_column_target.split(";")
                    if k.strip()
                ]
                key_columns = custom_keys
                self._logger.info(
                    f"Using custom primary key columns for '{target_table}': {key_columns}"
                )
            elif table_sync.custom_sql:
                # Custom SQL may transform data into a different schema
                # (e.g., transaction rows → daily aggregates).
                # Use the TARGET table's PK, not the source CDC record's PK.
                key_columns = self._get_target_primary_key(target_table)
                if not key_columns:
                    # Fallback: try source record PK if target PK detection fails
                    source_pk = self._get_primary_key_columns(records[0])
                    # Only use source PK if it exists in the transformed output
                    output_cols = set(transformed[0].keys())
                    if all(k in output_cols for k in source_pk):
                        key_columns = source_pk
                        self._logger.warning(
                            f"Could not detect target PK for '{target_table}', "
                            f"falling back to source PK: {source_pk}"
                        )
                    else:
                        # Last resort: use first column of transformed output
                        key_columns = [list(output_cols)[0]]
                        self._logger.warning(
                            f"Could not detect target PK for '{target_table}' "
                            f"and source PK {source_pk} not in output columns. "
                            f"Using first output column as key: {key_columns}"
                        )
            else:
                key_columns = self._get_primary_key_columns(records[0])

            # Step 4: MERGE INTO destination
            written = self._merge_into_postgres(transformed, target_table, key_columns)

            self._logger.debug(f"Wrote {written} records to {target_table}")
            return written

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Connection error (OperationalError) or closed connection (InterfaceError)
            error_msg = str(e)
            self._logger.error(f"PostgreSQL sync failed: {error_msg}")

            # Mark connection as unhealthy
            self._is_initialized = False

            # Force send notification
            try:
                notification_repo = NotificationLogRepository()
                
                # Sanitize error message before sending to notification
                sanitized_error = sanitize_for_db(e, self._config.name, "POSTGRES")
                
                notification_repo.upsert_notification_by_key(
                    NotificationLogCreate(
                        key_notification=f"destination_connection_error_{self.destination_id}",
                        title=f"PostgreSQL Connection Error",
                        message=f"Failed to connect to PostgreSQL destination {self._config.name}: {sanitized_error}",
                        type="ERROR",
                        is_force_sent=True,
                    )
                )
            except Exception as notify_error:
                self._logger.error(f"Failed to log notification: {notify_error}")

            # Wrap in DestinationException for proper DLQ handling
            raise DestinationException(
                f"PostgreSQL sync failed: {error_msg}",
                {"destination_id": self._config.id},
            )

        except Exception as e:
            # Notify on error
            try:
                notification_repo = NotificationLogRepository()

                # Check for connection issues in error message if generic exception caught
                error_msg = str(e).lower()
                is_force_sent = (
                    "connection" in error_msg
                    or "refused" in error_msg
                    or "timeout" in error_msg
                    or "operationalerror" in error_msg
                )

                # Sanitize error message before sending to notification
                sanitized_error = sanitize_for_db(e, self._config.name, "POSTGRES")

                notification_repo.upsert_notification_by_key(
                    NotificationLogCreate(
                        key_notification=f"destination_error_{self.destination_id}_{source_table}",
                        title=f"PostgreSQL Sync Error: {target_table}",
                        message=f"Failed to sync table {source_table} to {target_table}: {sanitized_error}",
                        type="ERROR",
                        is_force_sent=is_force_sent,
                    )
                )
            except Exception as notify_error:
                self._logger.error(f"Failed to log notification: {notify_error}")

            # Re-raise original exception
            raise e

        finally:
            # Step 5: Cleanup DuckDB table
            try:
                if self._duckdb_conn:
                    self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
            except Exception as e:
                self._logger.warning(
                    f"Failed to cleanup DuckDB table {safe_table_name}: {e}"
                )

    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: dict[str, Any],
    ) -> bool:
        """
        Create PostgreSQL table based on Debezium schema.

        Args:
            table_name: Target table name
            schema: Debezium schema dict

        Returns:
            True if table was created
        """
        if not self._pg_conn:
            self.initialize()

        try:
            # Check if table exists
            with self._pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """,
                    (self.schema, table_name),
                )
                exists = cur.fetchone()[0]

                if exists:
                    return False

                # Build CREATE TABLE from Debezium schema
                columns = self._schema_to_pg_columns(schema)

                if not columns:
                    self._logger.warning(f"No columns found in schema for {table_name}")
                    return False

                col_defs = ", ".join(columns)
                sql = f'CREATE TABLE "{self.schema}"."{table_name}" ({col_defs})'

                cur.execute(sql)
                self._logger.info(f"Created table: {self.schema}.{table_name}")
                return True

        except Exception as e:
            self._logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def _schema_to_pg_columns(self, schema: dict[str, Any]) -> list[str]:
        """
        Convert Debezium schema to PostgreSQL column definitions.

        Args:
            schema: Debezium schema dict

        Returns:
            List of column definitions
        """
        # Debezium type to PostgreSQL type mapping
        type_map = {
            "int32": "INTEGER",
            "int64": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "boolean": "BOOLEAN",
            "string": "TEXT",
            "bytes": "BYTEA",
        }

        columns = []
        fields = schema.get("fields", [])

        for field in fields:
            name = field.get("field", field.get("name", ""))
            field_type = field.get("type", "string")

            if not name:
                continue

            # Handle complex types
            if isinstance(field_type, dict):
                field_type = field_type.get("type", "string")

            pg_type = type_map.get(field_type, "TEXT")
            optional = field.get("optional", True)

            col_def = f'"{name}" {pg_type}'
            if not optional:
                col_def += " NOT NULL"

            columns.append(col_def)

        return columns

    def _cleanup_connections(self) -> None:
        """Internal method to cleanup connections without logging."""
        if self._duckdb_conn:
            try:
                self._duckdb_conn.close()
            except Exception:
                pass
            self._duckdb_conn = None

        if self._pg_conn:
            try:
                self._pg_conn.close()
            except Exception:
                pass
            self._pg_conn = None

        # Clear staging table tracker
        self._staging_tables.clear()

    def close(self) -> None:
        """Close DuckDB and PostgreSQL connections."""
        if self._duckdb_conn:
            try:
                self._duckdb_conn.close()
            except Exception as e:
                self._logger.warning(f"Error closing DuckDB connection: {e}")
            self._duckdb_conn = None

        if self._pg_conn:
            try:
                self._pg_conn.close()
            except Exception as e:
                self._logger.warning(f"Error closing PostgreSQL connection: {e}")
            self._pg_conn = None

        # Clear staging table tracker
        self._staging_tables.clear()

        self._is_initialized = False
        self._logger.info(f"PostgreSQL destination closed: {self._config.name}")

    def test_connection(self) -> bool:
        """
        Test if PostgreSQL connection is healthy.

        Performs a lightweight connection test without full initialization.
        Used by DLQ recovery worker to check destination health.

        Returns:
            True if connection is healthy
        """
        try:
            # Quick connection test
            test_conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=5,
            )

            # Execute simple query to verify connection
            with test_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

            test_conn.close()
            return True

        except Exception as e:
            self._logger.debug(f"PostgreSQL connection test failed: {e}")
            return False
