"""
Destination writer for Flow Task output nodes.

Abstract base + concrete implementations for writing DuckDB query results
to target destinations. Supports APPEND and UPSERT (MERGE INTO) modes.

Designed for extension: register new writers via DestinationWriterRegistry.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import duckdb
import structlog

logger = structlog.get_logger(__name__)


class BaseDestinationWriter(ABC):
    """Abstract destination writer."""

    @abstractmethod
    def write(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_cte: str,
        cte_prefix: str,
        target_table: str,
        schema_name: str,
        write_mode: str,
        upsert_keys: List[str],
        output_alias: str,
        destination_id: Optional[int] = None,
    ) -> int:
        """
        Write data from the CTE to the destination table.

        Args:
            conn: Active DuckDB connection (with destination already ATTACHed)
            source_cte: Name of the CTE containing data to write
            cte_prefix: The full WITH ... block to prepend to queries
            target_table: Target table name
            schema_name: Target schema name
            write_mode: APPEND or UPSERT
            upsert_keys: Columns to use as merge keys (UPSERT mode)
            output_alias: The DuckDB alias for the destination connection

        Returns:
            Number of rows written
        """
        ...

    @abstractmethod
    def get_row_count(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
    ) -> int:
        """Count rows in the source CTE before writing."""
        ...


class PostgresDestinationWriter(BaseDestinationWriter):
    """
    Write to a PostgreSQL destination via DuckDB postgres extension.

    The destination must already be ATTACHed in the DuckDB connection
    under `output_alias`.

    Type-mismatch handling for columns whose DuckDB representation differs
    from the PostgreSQL target type:

    geometry/geography:
      Snowflake GEOGRAPHY/GEOMETRY → GeoJSON VARCHAR in DuckDB.
      Fix: ST_AsText(ST_GeomFromGeoJSON(col)) → WKT, accepted by PostGIS.

    jsonb / json:
      Snowflake VARIANT/OBJECT → VARCHAR or DuckDB STRUCT/MAP in DuckDB.
      Fix: col::JSON → DuckDB JSON type, which the postgres extension maps
      to PG jsonb/json without type-mismatch errors.

    array types (udt_name starts with '_'):
      Snowflake ARRAY → VARCHAR JSON-array string or DuckDB LIST in DuckDB.
      Fix: json_transform(col::JSON, '["<elem_type>"]') → typed DuckDB list,
      which the postgres extension maps to the correct PG array type.
      Falls back to VARCHAR[] for unknown element types.
    """

    # Maps PostgreSQL array element udt_name → DuckDB element type string
    # used when converting a VARCHAR JSON-array to a typed DuckDB list.
    _PG_ARRAY_ELEM_TO_DUCKDB: Dict[str, str] = {
        "int2": "SMALLINT",
        "int4": "INTEGER",
        "int8": "BIGINT",
        "float4": "FLOAT",
        "float8": "DOUBLE",
        "numeric": "DOUBLE",
        "text": "VARCHAR",
        "varchar": "VARCHAR",
        "bpchar": "VARCHAR",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMPTZ",
        "uuid": "VARCHAR",
        "json": "JSON",
        "jsonb": "JSON",
    }

    def _get_pg_col_type_map(
        self,
        conn: duckdb.DuckDBPyConnection,
        output_alias: str,
        schema_name: str,
        target_table: str,
    ) -> Dict[str, str]:
        """
        Return {column_name_lower: udt_name_lower} for every column in the
        target PostgreSQL table.  udt_name is used rather than data_type
        because it exposes PostGIS types ('geometry', 'geography') and the
        internal array element type prefix ('_text', '_int4', etc.).
        """
        try:
            query = (
                f"SELECT column_name, udt_name "
                f"FROM {output_alias}.information_schema.columns "
                f"WHERE table_schema = '{schema_name}' "
                f"AND LOWER(table_name) = LOWER('{target_table}')"
            )
            rows = conn.execute(query).fetchall()
            return {row[0].lower(): row[1].lower() for row in rows}
        except Exception as e:
            logger.warning(
                "Could not introspect target column types — skipping type conversion",
                err=str(e),
            )
            return {}

    def _build_col_expressions(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
        output_alias: str,
        schema_name: str,
        target_table: str,
    ) -> tuple:
        """
        Return (col_names, col_select_expressions).

        Introspects the target PG table to find columns that need an explicit
        cast, then returns per-column DuckDB SQL expressions that produce the
        correct type before the postgres extension pushes rows to PostgreSQL.
        """
        cols_result = conn.execute(
            f"{cte_prefix}\nSELECT * FROM {source_cte} LIMIT 0"
        ).description
        if not cols_result:
            return [], []

        pg_type_map = self._get_pg_col_type_map(conn, output_alias, schema_name, target_table)

        col_names = []
        col_exprs = []
        for col in cols_result:
            name = col[0]
            duckdb_dtype = str(col[1]).upper() if col[1] else ""
            pg_udt = pg_type_map.get(name.lower(), "")
            col_names.append(name)

            # ── geometry / geography ──────────────────────────────────────────
            if pg_udt in ("geometry", "geography"):
                if "GEOMETRY" in duckdb_dtype:
                    # DuckDB native GEOMETRY → emit as WKT
                    col_exprs.append(f"ST_AsText({name})")
                else:
                    # VARCHAR GeoJSON from Snowflake → parse → WKT
                    col_exprs.append(
                        f"CASE WHEN {name} IS NULL THEN NULL "
                        f"ELSE ST_AsText(ST_GeomFromGeoJSON({name}::VARCHAR)) END"
                    )

            # ── jsonb / json ──────────────────────────────────────────────────
            elif pg_udt in ("jsonb", "json"):
                # Cast to DuckDB JSON type; the postgres extension maps
                # DuckDB JSON → PG json/jsonb without a type-mismatch error.
                col_exprs.append(
                    f"CASE WHEN {name} IS NULL THEN NULL "
                    f"ELSE {name}::JSON END"
                )

            # ── array types (udt_name starts with '_') ────────────────────────
            elif pg_udt.startswith("_"):
                elem_udt = pg_udt[1:]  # e.g. '_text' → 'text'
                duckdb_elem = self._PG_ARRAY_ELEM_TO_DUCKDB.get(elem_udt, "VARCHAR")
                is_already_list = (
                    "LIST" in duckdb_dtype
                    or "[]" in duckdb_dtype
                    or duckdb_dtype.startswith("[")
                )
                if is_already_list:
                    # Already a DuckDB list; just ensure element type matches
                    col_exprs.append(
                        f"CASE WHEN {name} IS NULL THEN NULL "
                        f"ELSE {name}::{duckdb_elem}[] END"
                    )
                else:
                    # VARCHAR JSON-array string from Snowflake
                    # json_transform parses a JSON array and returns a typed DuckDB list.
                    # Use explicit JSON string (double-quoted) as json_transform structure arg.
                    col_exprs.append(
                        f'CASE WHEN {name} IS NULL THEN NULL '
                        f'ELSE json_transform({name}::JSON, \'["{duckdb_elem}"]\') END'
                    )

            # ── everything else ───────────────────────────────────────────────
            else:
                col_exprs.append(name)

        return col_names, col_exprs

    def get_row_count(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
    ) -> int:
        count_sql = f"{cte_prefix}\nSELECT COUNT(*) FROM {source_cte}"
        result = conn.execute(count_sql).fetchone()
        return result[0] if result else 0

    def write(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_cte: str,
        cte_prefix: str,
        target_table: str,
        schema_name: str,
        write_mode: str,
        upsert_keys: List[str],
        output_alias: str,
        destination_id: Optional[int] = None,
    ) -> int:
        fqt = f"{output_alias}.{schema_name}.{target_table}"
        # Count rows once upfront — reuse for the return value
        row_count = self.get_row_count(conn, cte_prefix, source_cte)

        if not target_table:
            raise ValueError(
                "Output node has no target table name configured. "
                "Please set the 'Target Table' field and save the graph before running."
            )

        if row_count == 0:
            logger.info("Source CTE is empty — skipping write", target_table=target_table)
            return 0

        # Build geometry-aware column expressions once — shared by all write modes
        col_names, col_exprs = self._build_col_expressions(
            conn, cte_prefix, source_cte, output_alias, schema_name, target_table
        )

        if write_mode == "UPSERT":
            if not upsert_keys:
                raise ValueError(
                    f"UPSERT mode requires at least one upsert_key for table {target_table}"
                )
            self._upsert(conn, cte_prefix, source_cte, fqt, upsert_keys, col_names, col_exprs, output_alias)
        elif write_mode == "REPLACE":
            # REPLACE mode: truncate + insert
            logger.info("Truncating table", table=fqt)
            conn.execute(f"TRUNCATE TABLE {fqt}")
            self._append(conn, cte_prefix, source_cte, fqt, col_names, col_exprs)
        else:
            # APPEND mode
            self._append(conn, cte_prefix, source_cte, fqt, col_names, col_exprs)

        logger.info(
            "Destination write complete",
            target_table=target_table,
            schema=schema_name,
            write_mode=write_mode,
            rows_written=row_count,
        )
        return row_count

    def _append(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
        fqt: str,
        col_names: List[str] = None,
        col_exprs: List[str] = None,
    ) -> None:
        """INSERT INTO ... SELECT * FROM cte, with optional spatial expression overrides."""
        if col_names is None or col_exprs is None:
            cols_result = conn.execute(
                f"{cte_prefix}\nSELECT * FROM {source_cte} LIMIT 0"
            ).description
            col_names = [d[0] for d in cols_result] if cols_result else []
            col_exprs = col_names[:]

        if not col_names:
            logger.warning("No columns found in source CTE — skipping append")
            return

        col_list = ", ".join(col_names)
        expr_list = ", ".join(col_exprs)
        insert_sql = (
            f"{cte_prefix}\n"
            f"INSERT INTO {fqt} ({col_list})\n"
            f"SELECT {expr_list} FROM {source_cte}"
        )
        conn.execute(insert_sql)

    def _upsert(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
        fqt: str,
        upsert_keys: List[str],
        col_names: List[str] = None,
        col_exprs: List[str] = None,
        output_alias: Optional[str] = None,
    ) -> None:
        """
        UPSERT dispatcher.

        When columns require type conversion (jsonb, geometry, arrays) DuckDB's
        native MERGE creates an internal staging table that loses the converted
        types, causing PostgreSQL to reject the UPDATE.  In that case we route
        through a properly-typed PostgreSQL staging table instead.
        """
        if col_names is None or col_exprs is None:
            cols_result = conn.execute(
                f"{cte_prefix}\nSELECT * FROM {source_cte} LIMIT 0"
            ).description
            col_names = [d[0] for d in cols_result] if cols_result else []
            col_exprs = col_names[:]

        if not col_names:
            logger.warning("No columns found in source CTE — skipping upsert")
            return

        needs_staging = (
            output_alias is not None
            and any(name != expr for name, expr in zip(col_names, col_exprs))
        )

        if needs_staging:
            logger.info(
                "Using PostgreSQL staging-table upsert (type conversions detected)",
                target=fqt,
            )
            self._upsert_via_pg_staging(
                conn, cte_prefix, source_cte, fqt,
                upsert_keys, col_names, col_exprs, output_alias,
            )
        else:
            self._upsert_direct_merge(
                conn, cte_prefix, source_cte, fqt,
                upsert_keys, col_names, col_exprs,
            )

    # ── Direct DuckDB MERGE (no type-conversion needed) ──────────────────────

    def _upsert_direct_merge(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
        fqt: str,
        upsert_keys: List[str],
        col_names: List[str],
        col_exprs: List[str],
    ) -> None:
        """Standard DuckDB MERGE INTO — safe when all columns are plain types."""
        expr_select = ", ".join(
            f"{expr} AS {name}" if expr != name else name
            for name, expr in zip(col_names, col_exprs)
        )

        on_clause = " AND ".join(
            f"tgt.{k} = src.{k}" for k in upsert_keys
        )

        non_key_cols = [c for c in col_names if c not in upsert_keys]
        update_expr = ",\n    ".join(f"{c} = src.{c}" for c in non_key_cols)

        col_list = ", ".join(col_names)
        src_col_list = ", ".join(f"src.{c}" for c in col_names)

        merge_sql = (
            f"{cte_prefix},\n"
            f"__src AS (SELECT {expr_select} FROM {source_cte})\n"
            f"MERGE INTO {fqt} AS tgt\n"
            f"USING __src AS src\n"
            f"ON ({on_clause})\n"
            f"WHEN MATCHED THEN\n"
            f"  UPDATE SET\n    {update_expr}\n"
            f"WHEN NOT MATCHED THEN\n"
            f"  INSERT ({col_list})\n"
            f"  VALUES ({src_col_list})"
        )

        conn.execute(merge_sql)

    # ── Staging-table UPSERT (for jsonb / geometry / array columns) ──────────

    def _upsert_via_pg_staging(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
        fqt: str,
        upsert_keys: List[str],
        col_names: List[str],
        col_exprs: List[str],
        output_alias: str,
    ) -> None:
        """
        Upsert through a properly-typed PostgreSQL staging table.

        DuckDB's MERGE creates an internal staging table whose columns are
        typed from DuckDB's inference (VARCHAR for Snowflake VARIANT).  The
        resulting UPDATE then fails with e.g. "column metadata is of type
        jsonb but expression is of type character varying".

        Work-around:
          1. CREATE TABLE staging (LIKE target) → inherits jsonb, geometry, …
          2. INSERT INTO staging via DuckDB append (correctly maps types)
          3. UPDATE + INSERT natively on PostgreSQL via postgres_execute
          4. DROP staging table
        """
        import random
        import string as _string

        # Derive schema / table from fqt = "alias.schema.table"
        remainder = fqt[len(output_alias) + 1:]  # "schema.table"
        schema_name, target_table = remainder.split(".", 1)

        suffix = "".join(random.choices(_string.ascii_lowercase, k=8))
        stage_table = f"__rosetta_stg_{suffix}"
        stage_fqt = f"{output_alias}.{schema_name}.{stage_table}"

        try:
            # 1 ── Create staging table (same column types as target)
            create_sql = (
                f'CREATE TABLE "{schema_name}"."{stage_table}" '
                f'(LIKE "{schema_name}"."{target_table}")'
            )
            conn.execute(
                f"CALL postgres_execute('{output_alias}', "
                f"'{create_sql}')"
            )

            logger.debug(
                "Created PostgreSQL staging table",
                stage_table=stage_table,
            )

            # 2 ── INSERT data with type-converted expressions.
            #      DuckDB's INSERT (append) correctly maps
            #      DuckDB JSON → PG jsonb, GEOMETRY → PostGIS, etc.
            self._append(
                conn, cte_prefix, source_cte, stage_fqt,
                col_names, col_exprs,
            )

            # Snowflake returns UPPERCASE column names but PG target/staging
            # tables have lowercase.  The native PG SQL below uses
            # double-quoted identifiers, so we must lowercase everything.
            pg_col_names = [c.lower() for c in col_names]
            pg_upsert_keys = [k.lower() for k in upsert_keys]

            # 3a ── UPDATE existing rows: staging → target
            non_key_cols = [c for c in pg_col_names if c not in pg_upsert_keys]

            if non_key_cols:
                update_set = ", ".join(
                    f'"{c}" = src."{c}"' for c in non_key_cols
                )
                on_where = " AND ".join(
                    f'tgt."{k}" = src."{k}"' for k in pg_upsert_keys
                )
                update_sql = (
                    f'UPDATE "{schema_name}"."{target_table}" AS tgt '
                    f'SET {update_set} '
                    f'FROM "{schema_name}"."{stage_table}" AS src '
                    f'WHERE {on_where}'
                )
                conn.execute(
                    f"CALL postgres_execute('{output_alias}', "
                    f"$rosetta_stg${update_sql}$rosetta_stg$)"
                )

            # 3b ── INSERT new rows that don't yet exist in target
            col_list_q = ", ".join(f'"{c}"' for c in pg_col_names)
            src_cols = ", ".join(f'src."{c}"' for c in pg_col_names)
            not_exists = " AND ".join(
                f'tgt."{k}" = src."{k}"' for k in pg_upsert_keys
            )
            insert_sql = (
                f'INSERT INTO "{schema_name}"."{target_table}" ({col_list_q}) '
                f'SELECT {src_cols} '
                f'FROM "{schema_name}"."{stage_table}" AS src '
                f'WHERE NOT EXISTS ('
                f'SELECT 1 FROM "{schema_name}"."{target_table}" AS tgt '
                f'WHERE {not_exists})'
            )
            conn.execute(
                f"CALL postgres_execute('{output_alias}', "
                f"$rosetta_stg${insert_sql}$rosetta_stg$)"
            )

            logger.debug(
                "Staging-table upsert complete",
                stage_table=stage_table,
            )

        finally:
            # 4 ── Cleanup staging table
            try:
                conn.execute(
                    f"CALL postgres_execute('{output_alias}', "
                    f"'DROP TABLE IF EXISTS \"{schema_name}\".\"{stage_table}\"')"
                )
            except Exception:
                pass


# ─── Snowflake Writer ─────────────────────────────────────────────────────────


class SnowflakeDestinationWriter(BaseDestinationWriter):
    """
    Write to a Snowflake destination via snowflake-connector-python.

    DuckDB's Snowflake extension is read-only, so data is fetched from
    the CTE as a pandas DataFrame and written via write_pandas().
    """

    def get_row_count(
        self,
        conn: duckdb.DuckDBPyConnection,
        cte_prefix: str,
        source_cte: str,
    ) -> int:
        count_sql = f"{cte_prefix}\nSELECT COUNT(*) FROM {source_cte}"
        result = conn.execute(count_sql).fetchone()
        return result[0] if result else 0

    def write(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_cte: str,
        cte_prefix: str,
        target_table: str,
        schema_name: str,
        write_mode: str,
        upsert_keys: List[str],
        output_alias: str,
        destination_id: Optional[int] = None,
    ) -> int:
        import json as _json

        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        from snowflake.connector.pandas_tools import write_pandas

        from app.core.database import get_db_session
        from app.core.security import decrypt_value

        # Fetch data from DuckDB CTE as Arrow table (avoid premature Pandas conversion)
        fetch_sql = f"{cte_prefix}\nSELECT * FROM {source_cte}"
        arrow_table = conn.execute(fetch_sql).fetch_arrow_table()

        if arrow_table.num_rows == 0:
            return 0

        # Convert to pandas only when needed for write_pandas()
        # This defers the costly Arrow→Pandas conversion until the last moment
        df = arrow_table.to_pandas(self_destruct=True)  # self_destruct frees Arrow buffers early
        del arrow_table  # release Arrow memory immediately

        if destination_id is None:
            raise ValueError("SnowflakeDestinationWriter requires destination_id")

        # Fetch destination config
        with get_db_session() as db:
            from sqlalchemy import text

            row = db.execute(
                text("SELECT config FROM destinations WHERE id = :id"),
                {"id": destination_id},
            ).fetchone()

        if not row:
            raise ValueError(f"Destination {destination_id} not found")

        config = row.config if isinstance(row.config, dict) else _json.loads(row.config)

        # Build Snowflake connection
        conn_params: dict = {
            "user": config.get("user"),
            "account": config.get("account"),
            "role": config.get("role"),
            "warehouse": config.get("warehouse"),
            "database": config.get("database"),
            "schema": schema_name or config.get("schema"),
            "application": "Rosetta_ETL",
        }

        if config.get("private_key"):
            pk_str = config["private_key"].strip().replace("\\n", "\n")
            if not pk_str.startswith("-----"):
                try:
                    pk_str = decrypt_value(pk_str)
                except Exception:
                    pass
            passphrase = None
            if config.get("private_key_passphrase"):
                pp = config["private_key_passphrase"]
                try:
                    pp = decrypt_value(pp)
                except Exception:
                    pass
                passphrase = pp.encode()
            p_key = serialization.load_pem_private_key(
                pk_str.encode(), password=passphrase, backend=default_backend()
            )
            conn_params["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif config.get("password"):
            try:
                conn_params["password"] = decrypt_value(config["password"])
            except Exception:
                conn_params["password"] = config["password"]

        sf_conn = snowflake.connector.connect(**conn_params)
        try:
            if write_mode == "REPLACE":
                cursor = sf_conn.cursor()
                cursor.execute(
                    f"TRUNCATE TABLE IF EXISTS {schema_name}.{target_table}"
                )
                cursor.close()

            if write_mode == "UPSERT":
                rows_written = self._upsert_snowflake(
                    sf_conn, df, target_table, schema_name, upsert_keys
                )
            else:
                # APPEND or REPLACE (after truncate)
                _success, _num_chunks, num_rows, _ = write_pandas(
                    sf_conn,
                    df,
                    target_table,
                    schema=schema_name,
                    auto_create_table=False,
                )
                rows_written = num_rows

            logger.info(
                "Snowflake write complete",
                target_table=target_table,
                schema=schema_name,
                write_mode=write_mode,
                rows_written=rows_written,
            )
            return rows_written
        finally:
            sf_conn.close()

    def _upsert_snowflake(
        self,
        sf_conn: Any,
        df: Any,
        target_table: str,
        schema_name: str,
        upsert_keys: List[str],
    ) -> int:
        """Snowflake MERGE INTO via temporary staging table."""
        import random
        import string

        from snowflake.connector.pandas_tools import write_pandas

        stage_table = f"__tmp_{''.join(random.choices(string.ascii_lowercase, k=8))}"
        cursor = sf_conn.cursor()
        try:
            cursor.execute(
                f"CREATE TEMPORARY TABLE {schema_name}.{stage_table} "
                f"LIKE {schema_name}.{target_table}"
            )
            write_pandas(sf_conn, df, stage_table, schema=schema_name)

            cols = list(df.columns)
            on_clause = " AND ".join(f"tgt.{k} = src.{k}" for k in upsert_keys)
            non_key_cols = [c for c in cols if c not in upsert_keys]
            update_expr = ", ".join(f"tgt.{c} = src.{c}" for c in non_key_cols)
            col_list = ", ".join(cols)
            src_col_list = ", ".join(f"src.{c}" for c in cols)

            merge_sql = (
                f"MERGE INTO {schema_name}.{target_table} AS tgt "
                f"USING {schema_name}.{stage_table} AS src "
                f"ON ({on_clause}) "
            )
            if non_key_cols:
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {update_expr} "
            merge_sql += (
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) "
                f"VALUES ({src_col_list})"
            )

            cursor.execute(merge_sql)
            return len(df)
        finally:
            try:
                cursor.execute(
                    f"DROP TABLE IF EXISTS {schema_name}.{stage_table}"
                )
            except Exception:
                pass
            cursor.close()


# ─── Registry ─────────────────────────────────────────────────────────────────

class DestinationWriterRegistry:
    """Maps destination type strings to writer implementations."""

    _registry: Dict[str, type] = {
        "POSTGRES": PostgresDestinationWriter,
        "POSTGRESQL": PostgresDestinationWriter,
        "SNOWFLAKE": SnowflakeDestinationWriter,
    }

    @classmethod
    def get_writer(cls, dest_type: str) -> BaseDestinationWriter:
        writer_cls = cls._registry.get(dest_type.upper())
        if not writer_cls:
            raise ValueError(
                f"No writer registered for destination type '{dest_type}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return writer_cls()

    @classmethod
    def register(cls, dest_type: str, writer_class: type) -> None:
        """Register a new destination writer (e.g., Snowflake, BigQuery)."""
        cls._registry[dest_type.upper()] = writer_class
