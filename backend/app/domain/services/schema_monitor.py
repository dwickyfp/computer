"""
Schema Monitoring Service.

This service is responsible for:
1. Monitoring tables in source publications.
2. Checking schema changes (columns, types).
3. Recording schema history.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio

import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy.orm import Session
from deepdiff import DeepDiff

from app.core.config import get_settings
from app.core.database import get_session_context
from app.core.security import decrypt_value
from app.core.logging import get_logger
from app.domain.models.history_schema_evolution import HistorySchemaEvolution
from app.domain.models.source import Source
from app.domain.models.table_metadata import TableMetadata
from app.domain.repositories.source import SourceRepository

logger = get_logger(__name__)
settings = get_settings()


class SchemaMonitorService:
    """
    Service to monitor schema changes in source databases.
    """

    def __init__(self):
        self.running = False
        self._stop_event = asyncio.Event()

    async def monitor_all_sources(self) -> None:
        """
        Iterate through all sources and check their schema status.
        """
        logger.info("Starting schema monitoring cycle")

        try:
            with get_session_context() as db:
                repository = SourceRepository(db)
                sources = repository.get_all()

                # Process sources
                for source in sources:
                    if not source.is_publication_enabled:
                        continue

                    try:
                        await self.check_source_schema(source, db)
                    except Exception as e:
                        logger.error(
                            f"Error checking schema for source {source.name}",
                            extra={"error": str(e), "source_id": source.id},
                        )
        except Exception as e:
            logger.error("Error in schema monitoring cycle", extra={"error": str(e)})
        finally:
            logger.info("Completed schema monitoring cycle")

    async def check_source_schema(self, source: Source, db: Session) -> None:
        """
        Check schema for a single source.
        """
        # Connect to source database
        conn = None
        try:
            password = source.pg_password
            if password:
                try:
                    password = decrypt_value(password)
                except Exception:
                    logger.warning(
                        f"Failed to decrypt password for source {source.name}, attempting raw value",
                        extra={"source_id": source.id},
                    )
                    # Use raw value as fallback
                    password = source.pg_password

            logger.info(
                f"Connecting to source {source.name} ({source.pg_host}:{source.pg_port}/{source.pg_database})",
                extra={"source_id": source.id},
            )

            conn = psycopg2.connect(
                host=source.pg_host,
                port=source.pg_port,
                dbname=source.pg_database,
                user=source.pg_username,
                password=password,
                connect_timeout=settings.wal_monitor_timeout_seconds,
            )

            # 1. Sync table list from publication
            await self.sync_table_list(source, conn, db)

            # 2. Check schema for each table in metadata list
            # We refresh the source object or query tables directly to get the updated list
            updated_tables = (
                db.query(TableMetadata)
                .filter(TableMetadata.source_id == source.id)
                .all()
            )

            for table in updated_tables:
                await self.fetch_and_compare_schema(source, table, conn, db)

        except Exception as e:
            logger.error(
                f"Failed to connect to source {source.name}",
                extra={"error": str(e), "source_id": source.id},
            )
        finally:
            if conn:
                conn.close()

    async def sync_table_list(self, source: Source, conn, db: Session) -> None:
        """
        Sync local TableMetadata list with source publication tables.
        """
        try:
            with conn.cursor() as cur:
                # Query to get tables in the publication
                # Note: This query assumes standard PG publication-table mapping
                query = """
                    SELECT schemaname, tablename 
                    FROM pg_publication_tables 
                    WHERE pubname = %s;
                """
                cur.execute(query, (source.publication_name,))
                pub_tables = {
                    f"{row[0]}.{row[1]}" if row[0] != "public" else row[1]: row
                    for row in cur.fetchall()
                }
                # Simplify: we assume just table name if public, or explicit schema.table
                # For this logic, let's stick to the user request "table_name" which implies simple name or consistent format.
                # The user query example used `c.table_name`.
                # Let's assume public schema for now as per user query "WHERE c.table_schema = 'public'"

                # Re-reading user query: "WHERE c.table_schema = 'public'"
                # So we only care about public tables for now.

                pub_table_names = set(
                    row[1] for row in pub_tables.values() if row[0] == "public"
                )

                # Get existing tracked tables
                existing_tables = (
                    db.query(TableMetadata)
                    .filter(TableMetadata.source_id == source.id)
                    .all()
                )
                existing_table_names = {t.table_name for t in existing_tables}

                # Add new tables
                for new_table in pub_table_names - existing_table_names:
                    # Fetch schema immediately for new tables
                    try:
                        schema_list = self.fetch_table_schema(conn, new_table)

                        if not schema_list:
                            logger.warning(
                                f"Skipping table {new_table}: No schema columns found. "
                                "Table may be empty or inaccessible."
                            )
                            continue

                        # Convert to dict format
                        schema_dict = {
                            col["column_name"]: dict(col) for col in schema_list
                        }

                        new_metadata = TableMetadata(
                            source_id=source.id,
                            table_name=new_table,
                            schema_table=schema_dict,  # Set schema immediately
                            is_changes_schema=False,
                        )
                        db.add(new_metadata)
                        db.flush()  # Get ID for history record

                        # Create INITIAL_LOAD history record
                        history = HistorySchemaEvolution(
                            table_metadata_list_id=new_metadata.id,
                            schema_table_old={},
                            schema_table_new=schema_dict,
                            changes_type="INITIAL_LOAD",
                            version_schema=1,
                        )
                        db.add(history)

                        logger.info(
                            f"Added new table tracking with schema: {new_table} "
                            f"({len(schema_list)} columns)"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to fetch schema for new table {new_table}: {e}. "
                            "Skipping table registration."
                        )
                        db.rollback()
                        continue

                # Check existing tables for missing schemas and fix them
                for table in existing_tables:
                    # Only process tables that are still in publication
                    if table.table_name not in pub_table_names:
                        continue

                    # Check if schema is missing or empty
                    if not table.schema_table or table.schema_table == {}:
                        logger.info(
                            f"Found table {table.table_name} without schema, fetching now..."
                        )
                        try:
                            schema_list = self.fetch_table_schema(
                                conn, table.table_name
                            )

                            if not schema_list:
                                logger.warning(
                                    f"Could not fetch schema for {table.table_name}. "
                                    "Table may be empty or inaccessible."
                                )
                                continue

                            # Convert to dict format
                            schema_dict = {
                                col["column_name"]: dict(col) for col in schema_list
                            }

                            # Update table metadata
                            table.schema_table = schema_dict
                            table.is_changes_schema = False

                            # Check if INITIAL_LOAD history exists
                            existing_history = (
                                db.query(HistorySchemaEvolution)
                                .filter(
                                    HistorySchemaEvolution.table_metadata_list_id
                                    == table.id,
                                    HistorySchemaEvolution.changes_type
                                    == "INITIAL_LOAD",
                                )
                                .first()
                            )

                            if not existing_history:
                                # Create INITIAL_LOAD history record
                                history = HistorySchemaEvolution(
                                    table_metadata_list_id=table.id,
                                    schema_table_old={},
                                    schema_table_new=schema_dict,
                                    changes_type="INITIAL_LOAD",
                                    version_schema=1,
                                )
                                db.add(history)
                                logger.info(
                                    f"Fixed table {table.table_name}: Added schema and INITIAL_LOAD history "
                                    f"({len(schema_list)} columns)"
                                )
                            else:
                                # Update existing INITIAL_LOAD with correct schema
                                existing_history.schema_table_new = schema_dict
                                logger.info(
                                    f"Fixed table {table.table_name}: Updated schema "
                                    f"({len(schema_list)} columns)"
                                )

                        except Exception as e:
                            logger.error(
                                f"Failed to fetch schema for existing table {table.table_name}: {e}"
                            )
                            continue

                # Delete removed tables
                for removed_table in existing_table_names - pub_table_names:
                    # User said: "if table not exist in list publication but exist in table then delete"
                    table_to_delete = next(
                        t for t in existing_tables if t.table_name == removed_table
                    )
                    db.delete(table_to_delete)
                    logger.info(f"Removed table tracking: {removed_table}")

                db.commit()

        except Exception as e:
            db.rollback()
            raise e

    async def fetch_and_compare_schema(
        self, source: Source, table: TableMetadata, conn, db: Session
    ) -> None:
        """
        Fetch current schema and compare with stored schema.
        """
        new_schema_list = self.fetch_table_schema(conn, table.table_name)
        logger.info(
            f"Fetched schema for {table.table_name}: {len(new_schema_list)} columns"
        )

        # Validate schema is not empty
        if not new_schema_list:
            logger.warning(
                f"Skipping schema update for {table.table_name}: "
                "No columns found in schema fetch."
            )
            return

        # Convert list to a comparable dictionary/JSON structure
        # Assuming list of dicts: [{'column_name': 'id', 'data_type': 'BIGINT', ...}]
        # We can key by column name for easier comparison
        # Important: Convert RealDictRow to standard dict for DeepDiff
        new_schema_dict = {col["column_name"]: dict(col) for col in new_schema_list}

        old_schema_dict = table.schema_table or {}

        # If first run (old is None/Empty), record as Version 1
        if not old_schema_dict:
            # Check if INITIAL_LOAD already exists (prevent duplicates)
            existing_history = (
                db.query(HistorySchemaEvolution)
                .filter(
                    HistorySchemaEvolution.table_metadata_list_id == table.id,
                    HistorySchemaEvolution.changes_type == "INITIAL_LOAD",
                )
                .first()
            )

            if existing_history:
                # INITIAL_LOAD already exists, just update table metadata
                logger.info(
                    f"INITIAL_LOAD history already exists for {table.table_name}, "
                    "updating table metadata only"
                )
                table.schema_table = new_schema_dict
                table.is_changes_schema = False
                db.commit()
                return

            # Create History Record for Initial Load
            history = HistorySchemaEvolution(
                table_metadata_list_id=table.id,
                schema_table_old={},  # Nothing before
                schema_table_new=new_schema_dict,
                changes_type="INITIAL_LOAD",
                version_schema=1,
            )
            db.add(history)

            table.schema_table = new_schema_dict
            table.is_changes_schema = False
            db.commit()
            logger.info(
                f"Initial schema loaded and history recorded for {table.table_name}"
            )
            return

        # Compare
        # 1. Detect Changes
        ddiff = DeepDiff(old_schema_dict, new_schema_dict, ignore_order=True)

        if not ddiff:
            logger.info(f"No schema changes for {table.table_name}")
            return  # No changes

        # Identify change types
        change_type = "UNKNOWN"
        if "dictionary_item_added" in ddiff:
            change_type = "NEW COLUMN"
        elif "dictionary_item_removed" in ddiff:
            change_type = "DROP COLUMN"
        elif "values_changed" in ddiff:
            change_type = "CHANGES TYPE"

        # Create History Record
        # Calculate version - check for existing entries to prevent duplicates
        version = (
            db.query(HistorySchemaEvolution)
            .filter(HistorySchemaEvolution.table_metadata_list_id == table.id)
            .count()
        ) + 1

        # Check if this version already exists (prevent race condition duplicates)
        existing_version = (
            db.query(HistorySchemaEvolution)
            .filter(
                HistorySchemaEvolution.table_metadata_list_id == table.id,
                HistorySchemaEvolution.version_schema == version,
            )
            .first()
        )

        if existing_version:
            logger.warning(
                f"Schema version {version} already exists for {table.table_name}. "
                "Skipping duplicate history record creation."
            )
            # Update table metadata with new schema anyway
            table.schema_table = new_schema_dict
            table.is_changes_schema = True
            db.commit()
            return

        history = HistorySchemaEvolution(
            table_metadata_list_id=table.id,
            schema_table_old=old_schema_dict,
            schema_table_new=new_schema_dict,
            changes_type=change_type,
            version_schema=version,
        )
        db.add(history)

        # Update Table Metadata
        table.schema_table = new_schema_dict
        table.is_changes_schema = True

        db.commit()
        logger.info(f"Schema change detected for {table.table_name}: {change_type}")

        # Handle schema evolution for connected pipelines
        await self._apply_schema_evolution(
            source, table, old_schema_dict, new_schema_dict, change_type, db
        )

    def fetch_table_schema(self, conn, table_name: str) -> List[Dict]:
        """
        Fetch schema using user-provided queries with fallback.
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Complex query with PostGIS support
            complex_query = f"""
                SELECT 
                    c.column_name,
                    c.is_nullable,
                    c.numeric_precision,
                    c.numeric_scale,
                    CASE 
                        WHEN c.udt_name = 'geometry' THEN 'GEOMETRY'
                        WHEN c.udt_name = 'geography' THEN 'GEOGRAPHY'
                        ELSE UPPER(c.data_type)
                    END AS real_data_type,
                    CASE 
                        WHEN pk.column_name IS NOT NULL THEN TRUE 
                        ELSE FALSE 
                    END AS is_primary_key,

                    CASE 
                        WHEN c.column_default IS NOT NULL THEN TRUE 
                        ELSE FALSE 
                    END AS has_default,
                    c.column_default AS default_value
                FROM 
                    information_schema.columns c
                LEFT JOIN 
                    geometry_columns gc 
                    ON c.table_schema = gc.f_table_schema 
                    AND c.table_name = gc.f_table_name 
                    AND c.column_name = gc.f_geometry_column
                LEFT JOIN 
                    geography_columns gg 
                    ON c.table_schema = gg.f_table_schema 
                    AND c.table_name = gg.f_table_name 
                    AND c.column_name = gg.f_geography_column
                LEFT JOIN (
                    SELECT 
                        kcu.table_schema, 
                        kcu.table_name, 
                        kcu.column_name
                    FROM 
                        information_schema.key_column_usage kcu
                    JOIN 
                        information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name 
                        AND kcu.table_schema = tc.table_schema
                    WHERE 
                        tc.constraint_type = 'PRIMARY KEY'
                ) pk 
                    ON c.table_schema = pk.table_schema 
                    AND c.table_name = pk.table_name 
                    AND c.column_name = pk.column_name
                WHERE 
                    c.table_schema = 'public' 
                    and c.table_name = '{table_name}'
                ORDER BY 
                    c.table_name, 
                    c.ordinal_position;
            """

            try:
                # First check if geometry_columns exists to avoid throwing error inside the query execution if possible,
                # or just try-catch the execution.
                # User suggested: check "select * from geometry_columns;" first.
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'geometry_columns'"
                )
                has_postgis = cur.fetchone() is not None

                if has_postgis:
                    cur.execute(complex_query)
                    return cur.fetchall()
            except Exception:
                # Fallback or if just check fails
                conn.rollback()  # Reset transaction from error
                pass

            # Fallback Query
            fallback_query = f"""
                SELECT
                    c.column_name,
                    c.is_nullable,
                    c.numeric_precision,
                    c.numeric_scale,
                    UPPER(c.data_type) aS real_data_type,
                    CASE 
                            WHEN pk.column_name IS NOT NULL THEN TRUE 
                            ELSE FALSE 
                        END AS is_primary_key,

                        CASE 
                            WHEN c.column_default IS NOT NULL THEN TRUE 
                            ELSE FALSE 
                        END AS has_default,
                        c.column_default AS default_value
                    FROM
                    information_schema.columns c
                    LEFT JOIN (
                        SELECT 
                            kcu.table_schema, 
                            kcu.table_name, 
                            kcu.column_name
                        FROM 
                            information_schema.key_column_usage kcu
                        JOIN 
                            information_schema.table_constraints tc 
                            ON kcu.constraint_name = tc.constraint_name 
                            AND kcu.table_schema = tc.table_schema
                        WHERE 
                            tc.constraint_type = 'PRIMARY KEY'
                    ) pk 
                        ON c.table_schema = pk.table_schema 
                        AND c.table_name = pk.table_name 
                        AND c.column_name = pk.column_name
                    WHERE
                    c.table_schema = 'public' 
                    and c.table_name = '{table_name}'
                    ORDER BY
                    c.ordinal_position;
            """
            cur.execute(fallback_query)
            return cur.fetchall()

    async def _apply_schema_evolution(
        self,
        source: Source,
        table: TableMetadata,
        old_schema: dict,
        new_schema: dict,
        change_type: str,
        db: Session,
    ) -> None:
        """
        Mark pipelines for refresh when schema changes are detected.

        When a schema change is detected, this method:
        1. Finds pipelines connected to the source
        2. Marks each pipeline with ready_refresh = True
        3. Does NOT change the pipeline status

        The actual schema evolution will be applied when the pipeline is refreshed.
        """
        from app.domain.repositories.pipeline import PipelineRepository

        pipeline_repo = PipelineRepository(db)
        pipelines = pipeline_repo.get_by_source_id(source.id)

        if not pipelines:
            logger.info(
                f"No pipelines connected to source {source.name}, skipping ready_refresh marking",
                extra={"source_id": source.id, "table_name": table.table_name},
            )
            return

        logger.info(
            f"Found {len(pipelines)} pipeline(s) connected to source {source.name}",
            extra={"source_id": source.id, "table_name": table.table_name},
        )

        # Mark all connected pipelines as ready for refresh
        for pipeline in pipelines:
            try:
                # Only set ready_refresh for running pipelines
                if pipeline.status == "START":
                    logger.info(
                        f"Marking pipeline {pipeline.name} as ready_refresh due to schema change",
                        extra={
                            "pipeline_id": pipeline.id,
                            "table_name": table.table_name,
                            "change_type": change_type,
                        },
                    )

                    # Set ready_refresh flag without changing pipeline status
                    pipeline.ready_refresh = True

                    logger.info(
                        f"Pipeline {pipeline.name} marked for refresh (status unchanged)",
                        extra={
                            "pipeline_id": pipeline.id,
                            "current_status": pipeline.status,
                        },
                    )
                else:
                    logger.info(
                        f"Skipping ready_refresh for pipeline {pipeline.name} (status: {pipeline.status})",
                        extra={
                            "pipeline_id": pipeline.id,
                            "current_status": pipeline.status,
                        },
                    )
            except Exception as e:
                logger.error(
                    f"Failed to mark pipeline {pipeline.name} for refresh: {e}",
                    extra={
                        "pipeline_id": pipeline.id,
                        "table_name": table.table_name,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Continue with other pipelines even if one fails

        # Commit all changes at once
        try:
            db.commit()
            logger.info(
                f"Successfully marked {len(pipelines)} pipeline(s) as ready_refresh",
                extra={"source_id": source.id, "table_name": table.table_name},
            )
        except Exception as e:
            db.rollback()
            logger.error(
                f"Failed to commit ready_refresh changes: {e}",
                extra={"source_id": source.id, "table_name": table.table_name},
            )
            raise

    def stop(self):
        self._stop_event.set()
