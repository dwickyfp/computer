"""
Schema Evolution Service.

This service handles schema evolution when changes are detected in source tables.
It applies the necessary DDL changes to Snowflake target tables and recreates tasks.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.models.pipeline import Pipeline, PipelineStatus
from app.domain.models.table_metadata import TableMetadata

logger = get_logger(__name__)


class SchemaEvolutionService:
    """
    Service to handle schema evolution in Snowflake.

    When schema changes are detected in source tables connected to pipelines,
    this service:
    1. Alters the target table in Snowflake
    2. Recreates the merge task with updated column list
    3. Updates pipeline status to REFRESH
    """

    def __init__(self, db: Session):
        """Initialize schema evolution service."""
        self.db = db

    def handle_schema_evolution(
        self,
        pipeline: Pipeline,
        table_metadata: TableMetadata,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        change_type: str,
    ) -> None:
        """
        Handle schema evolution for a specific table in a pipeline.

        Args:
            pipeline: The pipeline connected to the source
            table_metadata: The table metadata with schema changes
            old_schema: Previous schema dictionary
            new_schema: New schema dictionary
            change_type: Type of change (NEW COLUMN, DROP COLUMN, CHANGES TYPE)
        """
        logger.info(
            f"Handling schema evolution for table {table_metadata.table_name}",
            extra={
                "pipeline_id": pipeline.id,
                "table_id": table_metadata.id,
                "change_type": change_type,
            },
        )

        destination = pipeline.destination
        table_name = table_metadata.table_name

        try:
            # Connect to Snowflake
            conn = self._get_snowflake_connection(destination)
            cursor = conn.cursor()

            try:
                # 1. Apply ALTER TABLE to target table based on change type
                if change_type == "NEW COLUMN":
                    self._handle_new_columns(
                        cursor, destination, table_name, old_schema, new_schema
                    )
                elif change_type == "DROP COLUMN":
                    self._handle_dropped_columns(
                        cursor, destination, table_name, old_schema, new_schema
                    )
                elif change_type == "CHANGES TYPE":
                    self._handle_type_changes(
                        cursor, destination, table_name, old_schema, new_schema
                    )

                # 2. Recreate the merge task with updated column list
                self._recreate_merge_task(cursor, pipeline, table_name, new_schema)

                # 3. Update pipeline status to REFRESH
                pipeline.status = PipelineStatus.REFRESH.value
                self.db.commit()

                logger.info(
                    f"Schema evolution completed for table {table_name}",
                    extra={"pipeline_id": pipeline.id, "change_type": change_type},
                )

            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            logger.error(
                f"Schema evolution failed for table {table_name}: {e}",
                extra={"pipeline_id": pipeline.id, "error": str(e)},
                exc_info=True,
            )
            raise

    def _handle_new_columns(
        self,
        cursor,
        destination: Destination,
        table_name: str,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> None:
        """Handle addition of new columns to the target table."""
        new_column_names = set(new_schema.keys()) - set(old_schema.keys())

        for col_name in new_column_names:
            col_info = new_schema[col_name]
            sf_type = self._map_postgres_to_snowflake(col_info)

            # Determine if we should add a DEFAULT clause
            default_clause = ""
            if col_info.get("has_default") and col_info.get("default_value"):
                parsed_default = self._parse_postgres_default(
                    col_info["default_value"], col_info
                )
                # Only add default if it resolves to a literal (string, number, boolean)
                # Skip SQL expressions like CURRENT_TIMESTAMP()
                if parsed_default and self._is_snowflake_literal(parsed_default):
                    default_clause = f" DEFAULT {parsed_default}"

            alter_ddl = f"""
                ALTER TABLE {destination.snowflake_database}.{destination.snowflake_schema}.{table_name}
                ADD COLUMN {col_name} {sf_type}{default_clause}
            """

            try:
                cursor.execute(alter_ddl)
                logger.info(
                    f"Added column {col_name} to target table {table_name} (Default: {default_clause.strip() or 'None'})"
                )
            except Exception as e:
                logger.error(f"Failed to add column {col_name}: {e}")
                raise

    def _handle_dropped_columns(
        self,
        cursor,
        destination: Destination,
        table_name: str,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> None:
        """Handle removal of columns from the target table."""
        dropped_column_names = set(old_schema.keys()) - set(new_schema.keys())

        for col_name in dropped_column_names:
            alter_ddl = f"""
                ALTER TABLE {destination.snowflake_database}.{destination.snowflake_schema}.{table_name}
                DROP COLUMN {col_name}
            """

            try:
                cursor.execute(alter_ddl)
                logger.warning(
                    f"Dropped column {col_name} from target table {table_name}"
                )
            except Exception as e:
                logger.error(f"Failed to drop column {col_name}: {e}")
                raise

    def _handle_type_changes(
        self,
        cursor,
        destination: Destination,
        table_name: str,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> None:
        """Handle type changes for existing columns."""
        common_columns = set(old_schema.keys()) & set(new_schema.keys())

        for col_name in common_columns:
            old_type = old_schema[col_name].get("real_data_type") or old_schema[
                col_name
            ].get("data_type")
            new_type = new_schema[col_name].get("real_data_type") or new_schema[
                col_name
            ].get("data_type")

            if old_type != new_type:
                sf_type = self._map_postgres_to_snowflake(new_schema[col_name])

                alter_ddl = f"""
                    ALTER TABLE {destination.snowflake_database}.{destination.snowflake_schema}.{table_name}
                    ALTER COLUMN {col_name} SET DATA TYPE {sf_type}
                """

                try:
                    cursor.execute(alter_ddl)
                    logger.info(
                        f"Changed type of column {col_name} to {sf_type} in table {table_name}"
                    )
                except Exception as e:
                    logger.error(f"Failed to change type of column {col_name}: {e}")
                    # Log but continue - type changes can fail if data is incompatible
                    logger.warning(
                        f"Continuing despite type change failure for {col_name}"
                    )

    def _recreate_merge_task(
        self, cursor, pipeline: Pipeline, table_name: str, new_schema: Dict[str, Any]
    ) -> None:
        """Recreate the merge task with updated column list and COALESCE for new NOT NULL columns."""
        destination = pipeline.destination

        landing_db = destination.snowflake_landing_database
        landing_schema = destination.snowflake_landing_schema
        landing_table = f"LANDING_{table_name}"
        stream_name = f"STREAM_{landing_table}"

        target_db = destination.snowflake_database
        target_schema = destination.snowflake_schema
        target_table = table_name

        # Convert schema dict to list format expected by task generator
        columns = list(new_schema.values())

        # Generate task DDL with COALESCE for NOT NULL columns
        task_ddl = self._generate_merge_task_ddl_with_coalesce(
            pipeline,
            landing_db,
            landing_schema,
            landing_table,
            stream_name,
            target_db,
            target_schema,
            target_table,
            columns,
        )

        task_name = f"ROSETTA_TASK_MERGE_{target_table}"

        try:
            # Suspend task before recreation
            try:
                cursor.execute(
                    f"ALTER TASK {landing_db}.{landing_schema}.{task_name} SUSPEND"
                )
            except Exception:
                pass  # Task might not exist

            # Create or replace task
            cursor.execute(task_ddl)

            # Resume task
            cursor.execute(
                f"ALTER TASK {landing_db}.{landing_schema}.{task_name} RESUME"
            )

            logger.info(f"Recreated merge task for table {table_name}")
        except Exception as e:
            logger.error(f"Failed to recreate merge task: {e}")
            raise

    def _generate_merge_task_ddl_with_coalesce(
        self,
        pipeline: Pipeline,
        l_db: str,
        l_schema: str,
        l_table: str,
        stream: str,
        t_db: str,
        t_schema: str,
        t_table: str,
        columns: List[Dict],
    ) -> str:
        """Generate MERGE task DDL with COALESCE for NOT NULL columns without defaults."""

        # Find primary key columns
        pk_cols = []
        for col in columns:
            if col.get("is_primary_key") is True:
                pk_cols.append(col["column_name"])

        # Fallback to 'id' or first column if no PK found
        if not pk_cols:
            for col in columns:
                if "id" in col["column_name"].lower():
                    pk_cols.append(col["column_name"])
                    break
        if not pk_cols:
            pk_cols.append(columns[0]["column_name"])

        # Prepare JOIN condition for MERGE
        join_condition = " AND ".join([f"T.{pk} = S.{pk}" for pk in pk_cols])

        # Prepare Partition By columns for De-duplication
        partition_by = ", ".join(pk_cols)

        col_names = [c["column_name"] for c in columns]

        # Build SELECT clause with COALESCE for NOT NULL columns
        select_cols = []
        for col in columns:
            col_name = col["column_name"]
            is_nullable = col.get("is_nullable", "YES")

            if is_nullable == "NO":
                # Column is NOT NULL, apply COALESCE with appropriate default
                default_val = self._get_coalesce_default(col)
                select_cols.append(f"COALESCE({col_name}, {default_val}) AS {col_name}")
            else:
                select_cols.append(col_name)

        # Add system columns
        select_cols.append("operation")
        select_cols.append("sync_timestamp_rosetta")

        select_clause = ",\n                    ".join(select_cols)

        indent = "            "

        # UPDATE SET clause (exclude PKs)
        update_cols = [c for c in col_names if c not in pk_cols]
        if not update_cols:
            set_clause = ", ".join([f"{c} = S.{c}" for c in col_names])
        else:
            set_clause = f",\n{indent}            ".join(
                [f"{c} = S.{c}" for c in update_cols]
            )

        val_clause = ", ".join([f"S.{c}" for c in col_names])
        col_list = ", ".join(col_names)

        # Use Snowflake scripting block to run MERGE then DELETE from landing table
        task_ddl = f"""
        CREATE OR REPLACE TASK {l_db}.{l_schema}.ROSETTA_TASK_MERGE_{t_table}
        WAREHOUSE = {pipeline.destination.snowflake_warehouse}
        SCHEDULE = '60 MINUTE'
        WHEN SYSTEM$STREAM_HAS_DATA('{l_db}.{l_schema}.{stream}')
        AS
        BEGIN
            -- Step 1: Merge data from stream to target table
            MERGE INTO {t_db}.{t_schema}.{t_table} AS T
            USING (
                SELECT * FROM (
                    SELECT 
                        {select_clause},
                        ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY sync_timestamp_rosetta DESC) as rn
                    FROM {l_db}.{l_schema}.{stream}
                ) WHERE rn = 1
            ) AS S
            ON {join_condition}
            WHEN MATCHED AND S.operation = 'D' THEN
                DELETE
            WHEN MATCHED AND S.operation != 'D' THEN
                UPDATE SET 
                {set_clause}
            WHEN NOT MATCHED AND S.operation != 'D' THEN
                INSERT ({col_list})
                VALUES ({val_clause});
            
            -- Step 2: Clean up landing table after merge
            DELETE FROM {l_db}.{l_schema}.{l_table};
        END;
        """

        return task_ddl

    def _get_coalesce_default(self, col: Dict) -> str:
        """
        Get the appropriate default value for COALESCE based on column metadata.

        Strategy:
        1. If has_default=True and default_value is a literal, use it
        2. Otherwise, use type-based fallback
        """
        has_default = col.get("has_default", False)
        default_value = col.get("default_value")

        if has_default and default_value:
            parsed_default = self._parse_postgres_default(default_value, col)
            if parsed_default:
                return parsed_default

        # Type-based fallback
        return self._get_type_based_default(col)

    def _is_snowflake_literal(self, value: str) -> bool:
        """
        Check if the value is a Snowflake literal (string, number, or boolean).
        Expected format is what _parse_postgres_default returns (e.g. quoted string).
        SQL expressions like CURRENT_TIMESTAMP() are NOT considered literals.
        """
        if not value:
            return False

        value = str(value).strip()

        # String literal (quoted)
        if value.startswith("'") and value.endswith("'"):
            return True

        # Boolean literal
        if value.upper() in ("TRUE", "FALSE"):
            return True

        # Number literal
        try:
            float(value)
            return True
        except ValueError:
            pass

        return False

    def _parse_postgres_default(self, default_value: str, col: Dict) -> Optional[str]:
        """
        Parse PostgreSQL default value and convert to Snowflake equivalent.

        Returns None if the default can't be meaningfully converted.
        """
        if not default_value:
            return None

        default_str = str(default_value).strip()

        # Skip sequence-based defaults (auto-increment)
        if "nextval" in default_str.lower():
            return None

        # Handle type casts like 'value'::text or 'value'::character varying
        cast_match = re.match(r"'([^']*)'::[\w\s]+", default_str)
        if cast_match:
            return f"'{cast_match.group(1)}'"

        # Handle simple quoted strings
        if default_str.startswith("'") and default_str.endswith("'"):
            return default_str

        # Handle numeric values
        try:
            float(default_str)
            return default_str
        except ValueError:
            pass

        # Handle boolean
        if default_str.lower() in ("true", "false"):
            return default_str.upper()

        # Handle CURRENT_TIMESTAMP and similar
        if "current_timestamp" in default_str.lower():
            return "CURRENT_TIMESTAMP()"
        if "current_date" in default_str.lower():
            return "CURRENT_DATE()"
        if "now()" in default_str.lower():
            return "CURRENT_TIMESTAMP()"

        # Handle JSON empty object/array
        if default_str in ("'{}'::jsonb", "'{}'::json"):
            return "PARSE_JSON('{}')"
        if default_str in ("'[]'::jsonb", "'[]'::json"):
            return "PARSE_JSON('[]')"

        return None

    def _get_type_based_default(self, col: Dict) -> str:
        """Get a sensible default value based on the data type."""
        pg_type = str(col.get("real_data_type") or col.get("data_type", "")).upper()

        if "INT" in pg_type or "SERIAL" in pg_type:
            return "0"
        elif "NUMERIC" in pg_type or "DECIMAL" in pg_type:
            return "0"
        elif "FLOAT" in pg_type or "DOUBLE" in pg_type or "REAL" in pg_type:
            return "0.0"
        elif "BOOL" in pg_type:
            return "FALSE"
        elif "DATE" in pg_type and "TIME" not in pg_type:
            return "CURRENT_DATE()"
        elif "TIMESTAMP" in pg_type:
            return "CURRENT_TIMESTAMP()"
        elif "JSON" in pg_type:
            return "PARSE_JSON('{}')"
        elif "UUID" in pg_type:
            return "UUID_STRING()"
        elif "ARRAY" in pg_type:
            return "ARRAY_CONSTRUCT()"
        else:
            # VARCHAR, TEXT, CHAR, and other string types
            return "''"

    def _map_postgres_to_snowflake(self, col: Dict) -> str:
        """Map PostgreSQL data type to Snowflake data type."""
        pg_type = str(col.get("real_data_type") or col.get("data_type", "")).upper()
        precision = col.get("numeric_precision")
        scale = col.get("numeric_scale")

        if "INT" in pg_type or "SERIAL" in pg_type:
            return "NUMBER(38,0)"
        elif "NUMERIC" in pg_type or "DECIMAL" in pg_type:
            if precision is not None and scale is not None:
                return f"NUMBER({precision}, {scale})"
            return "NUMBER(38,4)"
        elif "FLOAT" in pg_type or "DOUBLE" in pg_type:
            return "FLOAT"
        elif "REAL" in pg_type:
            return "FLOAT"
        elif "BOOL" in pg_type:
            return "BOOLEAN"
        elif "DATE" in pg_type and "TIME" not in pg_type:
            return "DATE"
        elif "TIMESTAMPTZ" in pg_type or "TIMESTAMP WITH TIME ZONE" in pg_type:
            # PostgreSQL TIMESTAMPTZ -> Snowflake TIMESTAMP_TZ
            return "TIMESTAMP_TZ"
        elif "TIMESTAMP" in pg_type:
            # PostgreSQL TIMESTAMP (without timezone) -> Snowflake TIMESTAMP_NTZ
            return "TIMESTAMP_NTZ"
        elif "TIMETZ" in pg_type or "TIME WITH TIME ZONE" in pg_type:
            # PostgreSQL TIMETZ -> Snowflake TIME (no TZ equivalent)
            return "TIME"
        elif "TIME" in pg_type:
            return "TIME"
        elif "JSON" in pg_type:
            return "VARIANT"
        elif "ARRAY" in pg_type:
            return "ARRAY"
        elif "UUID" in pg_type:
            return "VARCHAR(36)"
        elif "GEOMETRY" in pg_type:
            return "GEOMETRY"
        elif "GEOGRAPHY" in pg_type:
            return "GEOGRAPHY"
        else:
            return "VARCHAR"

    def _get_snowflake_connection(self, destination: Destination):
        """Create a Snowflake connection using destination credentials."""
        from app.core.security import decrypt_value

        private_key_str = decrypt_value(destination.snowflake_private_key.strip())
        passphrase = None
        if destination.snowflake_private_key_passphrase:
            passphrase = decrypt_value(
                destination.snowflake_private_key_passphrase
            ).encode()

        p_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=passphrase,
            backend=default_backend(),
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            user=destination.snowflake_user,
            account=destination.snowflake_account,
            private_key=pkb,
            role=destination.snowflake_role,
            warehouse=destination.snowflake_warehouse,
            database=destination.snowflake_database,
            schema=destination.snowflake_schema,
            client_session_keep_alive=False,
            application="Rosetta_ETL",
        )
