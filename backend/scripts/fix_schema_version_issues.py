"""
Script to fix existing schema version issues in the database.

This script:
1. Identifies tables with empty schemas
2. Removes duplicate version records
3. Re-fetches schemas for tables with issues
4. Rebuilds schema history correctly

Usage:
    cd backend
    uv run python scripts/fix_schema_version_issues.py
"""

import asyncio
from sqlalchemy import create_engine, select, delete, func
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.models.table_metadata import TableMetadata
from app.domain.models.history_schema_evolution import HistorySchemaEvolution
from app.domain.models.source import Source
from app.domain.services.schema_monitor import SchemaMonitorService
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
import psycopg2
from app.core.security import decrypt_value

logger = get_logger(__name__)
settings = get_settings()


async def fix_schema_issues():
    """Fix schema version issues in the database."""

    # Create database connection
    engine = create_engine(settings.DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        logger.info("Starting schema issue fix process...")

        # 1. Find tables with empty schemas
        tables_with_empty_schema = (
            db.query(TableMetadata)
            .filter(
                (TableMetadata.schema_table.is_(None))
                | (TableMetadata.schema_table == {})
            )
            .all()
        )

        logger.info(f"Found {len(tables_with_empty_schema)} tables with empty schemas")

        # 2. Find duplicate version records
        duplicate_versions = (
            db.query(
                HistorySchemaEvolution.table_metadata_list_id,
                HistorySchemaEvolution.version_schema,
                func.count(HistorySchemaEvolution.id).label("count"),
            )
            .group_by(
                HistorySchemaEvolution.table_metadata_list_id,
                HistorySchemaEvolution.version_schema,
            )
            .having(func.count(HistorySchemaEvolution.id) > 1)
            .all()
        )

        logger.info(f"Found {len(duplicate_versions)} duplicate version entries")

        # 3. Remove duplicate versions (keep oldest)
        for table_id, version, count in duplicate_versions:
            logger.info(
                f"Removing {count - 1} duplicate records for table_id={table_id}, version={version}"
            )

            # Get all records for this table/version
            records = (
                db.query(HistorySchemaEvolution)
                .filter(
                    HistorySchemaEvolution.table_metadata_list_id == table_id,
                    HistorySchemaEvolution.version_schema == version,
                )
                .order_by(HistorySchemaEvolution.created_at.asc())
                .all()
            )

            # Keep first (oldest), delete rest
            for record in records[1:]:
                db.delete(record)

        db.commit()
        logger.info("Removed duplicate version records")

        # 4. Re-fetch schemas for tables with empty schemas
        schema_monitor = SchemaMonitorService()

        for table in tables_with_empty_schema:
            try:
                # Get source
                source = db.query(Source).filter(Source.id == table.source_id).first()
                if not source:
                    logger.warning(f"Source not found for table {table.table_name}")
                    continue

                # Connect to source
                conn = psycopg2.connect(
                    host=source.pg_host,
                    port=source.pg_port,
                    dbname=source.pg_database,
                    user=source.pg_username,
                    password=(
                        decrypt_value(source.pg_password)
                        if source.pg_password
                        else None
                    ),
                    connect_timeout=10,
                )

                # Fetch schema
                schema_list = schema_monitor.fetch_table_schema(conn, table.table_name)
                conn.close()

                if not schema_list:
                    logger.warning(
                        f"Could not fetch schema for table {table.table_name}. "
                        "Table may be empty or inaccessible."
                    )
                    continue

                # Convert to dict
                schema_dict = {col["column_name"]: dict(col) for col in schema_list}

                # Update table metadata
                table.schema_table = schema_dict
                table.is_changes_schema = False

                # Check if INITIAL_LOAD history exists
                initial_history = (
                    db.query(HistorySchemaEvolution)
                    .filter(
                        HistorySchemaEvolution.table_metadata_list_id == table.id,
                        HistorySchemaEvolution.changes_type == "INITIAL_LOAD",
                    )
                    .first()
                )

                if not initial_history:
                    # Create INITIAL_LOAD history
                    history = HistorySchemaEvolution(
                        table_metadata_list_id=table.id,
                        schema_table_old={},
                        schema_table_new=schema_dict,
                        changes_type="INITIAL_LOAD",
                        version_schema=1,
                    )
                    db.add(history)
                    logger.info(
                        f"Fixed table {table.table_name}: Added schema and INITIAL_LOAD history"
                    )
                else:
                    # Update existing INITIAL_LOAD with correct schema
                    initial_history.schema_table_new = schema_dict
                    logger.info(
                        f"Fixed table {table.table_name}: Updated INITIAL_LOAD schema"
                    )

                db.commit()

            except Exception as e:
                logger.error(f"Failed to fix table {table.table_name}: {e}")
                db.rollback()
                continue

        logger.info("Schema issue fix process completed successfully")

        # 5. Generate summary report
        total_tables = db.query(TableMetadata).count()
        total_history = db.query(HistorySchemaEvolution).count()
        tables_with_schema = (
            db.query(TableMetadata)
            .filter(
                TableMetadata.schema_table.isnot(None), TableMetadata.schema_table != {}
            )
            .count()
        )

        print("\n" + "=" * 60)
        print("SCHEMA FIX SUMMARY")
        print("=" * 60)
        print(f"Total tables: {total_tables}")
        print(f"Tables with valid schema: {tables_with_schema}")
        print(f"Tables fixed: {len(tables_with_empty_schema)}")
        print(f"Total history records: {total_history}")
        print(f"Duplicate versions removed: {len(duplicate_versions)}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Error during schema fix: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(fix_schema_issues())
