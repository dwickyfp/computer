"""
Repository classes for database CRUD operations.

Provides data access layer for Rosetta Compute Engine.
"""

import logging
from typing import Any, Optional
from datetime import datetime

from psycopg2.extras import Json

from core.database import DatabaseSession
from core.db_utils import retry_on_connection_error, is_connection_error
from core.kafka_schema import classify_schema_change, normalize_schema_definition
from core.models import (
    Source,
    Destination,
    Pipeline,
    PipelineDestination,
    PipelineDestinationTableSync,
    PipelineMetadata,
    TableMetadataList,
    DataFlowRecordMonitoring,
)
from core.exceptions import DatabaseException

logger = logging.getLogger(__name__)


class SourceRepository:
    """Repository for Source CRUD operations."""

    @staticmethod
    def get_by_id(source_id: int) -> Optional[Source]:
        """Get source by ID."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM sources WHERE id = %s", (source_id,))
            row = session.fetchone()
            return Source.from_dict(row) if row else None

    @staticmethod
    def get_by_name(name: str) -> Optional[Source]:
        """Get source by name."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM sources WHERE name = %s", (name,))
            row = session.fetchone()
            return Source.from_dict(row) if row else None

    @staticmethod
    def get_all() -> list[Source]:
        """Get all sources."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM sources ORDER BY id")
            return [Source.from_dict(row) for row in session.fetchall()]


class DestinationRepository:
    """Repository for Destination CRUD operations."""

    @staticmethod
    def get_by_id(destination_id: int) -> Optional[Destination]:
        """Get destination by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM destinations WHERE id = %s", (destination_id,)
            )
            row = session.fetchone()
            return Destination.from_dict(row) if row else None

    @staticmethod
    def get_all() -> list[Destination]:
        """Get all destinations."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM destinations ORDER BY id")
            return [Destination.from_dict(row) for row in session.fetchall()]


class PipelineRepository:
    """Repository for Pipeline CRUD operations."""

    @staticmethod
    @retry_on_connection_error(max_retries=3, delay=0.5)
    def get_by_id(
        pipeline_id: int, include_relations: bool = False
    ) -> Optional[Pipeline]:
        """
        Get pipeline by ID.

        Args:
            pipeline_id: Pipeline ID
            include_relations: If True, load source and destinations
        """
        with DatabaseSession() as session:
            session.execute("SELECT * FROM pipelines WHERE id = %s", (pipeline_id,))
            row = session.fetchone()

            if not row:
                return None

            pipeline = Pipeline.from_dict(row)

            if include_relations:
                # Load source
                pipeline.source = SourceRepository.get_by_id(pipeline.source_id)

                # Load destinations
                pipeline.destinations = (
                    PipelineDestinationRepository.get_by_pipeline_id(
                        pipeline_id, include_table_syncs=True
                    )
                )

            return pipeline

    @staticmethod
    def get_by_name(name: str) -> Optional[Pipeline]:
        """Get pipeline by name."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM pipelines WHERE name = %s", (name,))
            row = session.fetchone()
            return Pipeline.from_dict(row) if row else None

    @staticmethod
    @retry_on_connection_error(max_retries=3, delay=0.5)
    def get_active_pipelines() -> list[Pipeline]:
        """Get all pipelines with START status."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines WHERE status = 'START' ORDER BY id"
            )
            return [Pipeline.from_dict(row) for row in session.fetchall()]

    @staticmethod
    def get_all() -> list[Pipeline]:
        """Get all pipelines."""
        with DatabaseSession() as session:
            session.execute("SELECT * FROM pipelines ORDER BY id")
            return [Pipeline.from_dict(row) for row in session.fetchall()]

    @staticmethod
    @retry_on_connection_error(max_retries=3, delay=0.5)
    def update_status(pipeline_id: int, status: str) -> bool:
        """Update pipeline status."""
        with DatabaseSession() as session:
            session.execute(
                """
                UPDATE pipelines 
                SET status = %s, updated_at = TIMEZONE('Asia/Jakarta', NOW())
                WHERE id = %s
                """,
                (status, pipeline_id),
            )
            return session.rowcount > 0


class PipelineDestinationRepository:
    """Repository for PipelineDestination CRUD operations."""

    @staticmethod
    def get_by_id(pd_id: int) -> Optional[PipelineDestination]:
        """Get pipeline destination by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination WHERE id = %s", (pd_id,)
            )
            row = session.fetchone()
            return PipelineDestination.from_dict(row) if row else None

    @staticmethod
    def get_by_pipeline_id(
        pipeline_id: int, include_table_syncs: bool = False
    ) -> list[PipelineDestination]:
        """Get all destinations for a pipeline."""
        with DatabaseSession() as session:
            session.execute(
                """
                SELECT pd.*, d.name as dest_name, d.type as dest_type, d.config as dest_config
                FROM pipelines_destination pd
                JOIN destinations d ON pd.destination_id = d.id
                WHERE pd.pipeline_id = %s
                ORDER BY pd.id
                """,
                (pipeline_id,),
            )

            results = []
            for row in session.fetchall():
                pd = PipelineDestination.from_dict(row)
                pd.destination = Destination(
                    id=row["destination_id"],
                    name=row["dest_name"],
                    type=row["dest_type"],
                    config=row["dest_config"],
                )

                if include_table_syncs:
                    pd.table_syncs = TableSyncRepository.get_by_pipeline_destination_id(
                        pd.id
                    )

                results.append(pd)

            return results

    @staticmethod
    def update_error(
        pd_id: int, is_error: bool, error_message: Optional[str] = None
    ) -> bool:
        """Update pipeline destination error status."""
        with DatabaseSession() as session:
            if is_error:
                session.execute(
                    """
                    UPDATE pipelines_destination 
                    SET is_error = TRUE, error_message = %s, last_error_at = TIMEZONE('Asia/Jakarta', NOW()), updated_at = TIMEZONE('Asia/Jakarta', NOW())
                    WHERE id = %s
                    """,
                    (error_message, pd_id),
                )
            else:
                session.execute(
                    """
                    UPDATE pipelines_destination 
                    SET is_error = FALSE, error_message = NULL, updated_at = TIMEZONE('Asia/Jakarta', NOW())
                    WHERE id = %s
                    """,
                    (pd_id,),
                )
            return session.rowcount > 0


class TableSyncRepository:
    """Repository for PipelineDestinationTableSync CRUD operations."""

    @staticmethod
    def get_by_id(sync_id: int) -> Optional[PipelineDestinationTableSync]:
        """Get table sync by ID."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination_table_sync WHERE id = %s",
                (sync_id,),
            )
            row = session.fetchone()
            return PipelineDestinationTableSync.from_dict(row) if row else None

    @staticmethod
    def get_by_pipeline_destination_id(
        pd_id: int,
    ) -> list[PipelineDestinationTableSync]:
        """Get all table syncs for a pipeline destination."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipelines_destination_table_sync WHERE pipeline_destination_id = %s ORDER BY id",
                (pd_id,),
            )
            return [
                PipelineDestinationTableSync.from_dict(row)
                for row in session.fetchall()
            ]

    @staticmethod
    def get_by_table_name(
        pd_id: int, table_name: str
    ) -> Optional[PipelineDestinationTableSync]:
        """Get table sync by pipeline destination ID and table name."""
        with DatabaseSession() as session:
            session.execute(
                """
                SELECT * FROM pipelines_destination_table_sync 
                WHERE pipeline_destination_id = %s AND table_name = %s
                """,
                (pd_id, table_name),
            )
            row = session.fetchone()
            return PipelineDestinationTableSync.from_dict(row) if row else None

    @staticmethod
    def update_error(
        sync_id: int, is_error: bool, error_message: Optional[str] = None
    ) -> bool:
        """Update table sync error status."""
        with DatabaseSession() as session:
            if is_error:
                session.execute(
                    """
                    UPDATE pipelines_destination_table_sync 
                    SET is_error = TRUE, error_message = %s, updated_at = TIMEZONE('Asia/Jakarta', NOW())
                    WHERE id = %s
                    """,
                    (error_message, sync_id),
                )
            else:
                session.execute(
                    """
                    UPDATE pipelines_destination_table_sync 
                    SET is_error = FALSE, error_message = NULL, updated_at = TIMEZONE('Asia/Jakarta', NOW())
                    WHERE id = %s
                    """,
                    (sync_id,),
                )
            return session.rowcount > 0


class TableMetadataRepository:
    """Repository for TableMetadataList CRUD operations."""

    @staticmethod
    def get_by_source_id(source_id: int) -> list[TableMetadataList]:
        """Get all table metadata for a source."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM table_metadata_list WHERE source_id = %s ORDER BY table_name",
                (source_id,),
            )
            return [TableMetadataList.from_dict(row) for row in session.fetchall()]

    @staticmethod
    def get_table_names_for_source(source_id: int) -> list[str]:
        """Get list of table names for a source."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT table_name FROM table_metadata_list WHERE source_id = %s ORDER BY table_name",
                (source_id,),
            )
            return [row["table_name"] for row in session.fetchall()]

    @staticmethod
    def get_by_source_and_name(
        source_id: int, table_name: str
    ) -> Optional[TableMetadataList]:
        """Get one table metadata row by source and table name."""
        with DatabaseSession() as session:
            session.execute(
                """
                SELECT * FROM table_metadata_list
                WHERE source_id = %s AND table_name = %s
                """,
                (source_id, table_name),
            )
            row = session.fetchone()
            return TableMetadataList.from_dict(row) if row else None

    @staticmethod
    def sync_inferred_schema(
        source_id: int,
        table_name: str,
        schema_table: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist an inferred Kafka schema and create version history if it changed."""
        normalized_schema = normalize_schema_definition(schema_table)
        if not normalized_schema:
            return {"updated": False, "schema_table": {}}

        with DatabaseSession() as session:
            session.execute(
                """
                SELECT * FROM table_metadata_list
                WHERE source_id = %s AND table_name = %s
                FOR UPDATE
                """,
                (source_id, table_name),
            )
            row = session.fetchone()
            if not row:
                return {
                    "updated": False,
                    "reason": "missing_table",
                    "schema_table": normalized_schema,
                }

            table = TableMetadataList.from_dict(row)
            current_schema = normalize_schema_definition(table.schema_table)

            if current_schema == normalized_schema:
                session.execute(
                    """
                    SELECT COALESCE(MAX(version_schema), 0) AS version
                    FROM history_schema_evolution
                    WHERE table_metadata_list_id = %s
                    """,
                    (table.id,),
                )
                version_row = session.fetchone() or {}
                return {
                    "updated": False,
                    "version": int(version_row.get("version") or (1 if current_schema else 0)),
                    "change_type": None,
                    "schema_table": current_schema,
                }

            if not current_schema:
                session.execute(
                    """
                    SELECT id
                    FROM history_schema_evolution
                    WHERE table_metadata_list_id = %s AND changes_type = 'INITIAL_LOAD'
                    ORDER BY id
                    LIMIT 1
                    """,
                    (table.id,),
                )
                existing_initial = session.fetchone()
                if existing_initial:
                    session.execute(
                        """
                        UPDATE history_schema_evolution
                        SET schema_table_new = %s,
                            updated_at = TIMEZONE('Asia/Jakarta', NOW())
                        WHERE id = %s
                        """,
                        (Json(normalized_schema), existing_initial["id"]),
                    )
                else:
                    session.execute(
                        """
                        INSERT INTO history_schema_evolution
                        (
                            table_metadata_list_id,
                            schema_table_old,
                            schema_table_new,
                            changes_type,
                            version_schema,
                            created_at,
                            updated_at
                        )
                        VALUES
                        (
                            %s,
                            %s,
                            %s,
                            'INITIAL_LOAD',
                            1,
                            TIMEZONE('Asia/Jakarta', NOW()),
                            TIMEZONE('Asia/Jakarta', NOW())
                        )
                        """,
                        (table.id, Json({}), Json(normalized_schema)),
                    )

                session.execute(
                    """
                    UPDATE table_metadata_list
                    SET schema_table = %s,
                        is_changes_schema = FALSE,
                        updated_at = TIMEZONE('Asia/Jakarta', NOW())
                    WHERE id = %s
                    """,
                    (Json(normalized_schema), table.id),
                )
                return {
                    "updated": True,
                    "version": 1,
                    "change_type": "INITIAL_LOAD",
                    "schema_table": normalized_schema,
                }

            session.execute(
                """
                SELECT COALESCE(MAX(version_schema), 0) AS version
                FROM history_schema_evolution
                WHERE table_metadata_list_id = %s
                """,
                (table.id,),
            )
            version_row = session.fetchone() or {}
            latest_version = int(version_row.get("version") or 0)
            next_version = latest_version + 1 if latest_version > 0 else 2
            change_type = classify_schema_change(current_schema, normalized_schema)

            session.execute(
                """
                INSERT INTO history_schema_evolution
                (
                    table_metadata_list_id,
                    schema_table_old,
                    schema_table_new,
                    changes_type,
                    version_schema,
                    created_at,
                    updated_at
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    TIMEZONE('Asia/Jakarta', NOW()),
                    TIMEZONE('Asia/Jakarta', NOW())
                )
                """,
                (
                    table.id,
                    Json(current_schema),
                    Json(normalized_schema),
                    change_type,
                    next_version,
                ),
            )
            session.execute(
                """
                UPDATE table_metadata_list
                SET schema_table = %s,
                    is_changes_schema = TRUE,
                    updated_at = TIMEZONE('Asia/Jakarta', NOW())
                WHERE id = %s
                """,
                (Json(normalized_schema), table.id),
            )
            return {
                "updated": True,
                "version": next_version,
                "change_type": change_type,
                "schema_table": normalized_schema,
            }


class PipelineMetadataRepository:
    """Repository for PipelineMetadata CRUD operations."""

    @staticmethod
    def get_by_pipeline_id(pipeline_id: int) -> Optional[PipelineMetadata]:
        """Get metadata for a pipeline."""
        with DatabaseSession() as session:
            session.execute(
                "SELECT * FROM pipeline_metadata WHERE pipeline_id = %s", (pipeline_id,)
            )
            row = session.fetchone()
            return PipelineMetadata.from_dict(row) if row else None

    @staticmethod
    @retry_on_connection_error(max_retries=3, delay=0.5)
    def upsert(pipeline_id: int, status: str, error: Optional[str] = None) -> int:
        """Insert or update pipeline metadata."""
        with DatabaseSession() as session:
            try:
                if error:
                    session.execute(
                        """
                        INSERT INTO pipeline_metadata (pipeline_id, status, last_error, last_error_at, last_start_at)
                        VALUES (%s, %s, %s, TIMEZONE('Asia/Jakarta', NOW()), TIMEZONE('Asia/Jakarta', NOW()))
                        ON CONFLICT (pipeline_id) DO UPDATE 
                        SET status = EXCLUDED.status, 
                            last_error = EXCLUDED.last_error,
                            last_error_at = TIMEZONE('Asia/Jakarta', NOW()),
                            updated_at = TIMEZONE('Asia/Jakarta', NOW())
                        RETURNING id
                        """,
                        (pipeline_id, status, error),
                    )
                else:
                    session.execute(
                        """
                        INSERT INTO pipeline_metadata (pipeline_id, status, last_start_at)
                        VALUES (%s, %s, TIMEZONE('Asia/Jakarta', NOW()))
                        ON CONFLICT (pipeline_id) DO UPDATE 
                        SET status = EXCLUDED.status,
                            last_start_at = CASE WHEN EXCLUDED.status = 'RUNNING' THEN TIMEZONE('Asia/Jakarta', NOW()) ELSE pipeline_metadata.last_start_at END,
                            updated_at = TIMEZONE('Asia/Jakarta', NOW())
                        RETURNING id
                        """,
                        (pipeline_id, status),
                    )
                row = session.fetchone()
                return row["id"] if row else 0
            except Exception as e:
                # Re-raise transient connection errors so @retry_on_connection_error
                # can do its job.  Swallow only non-retryable SQL errors.
                if is_connection_error(e):
                    raise
                # FK violation means the pipeline row was deleted; this is not
                # an application error — downgrade to WARNING to avoid false alarms.
                err_str = str(e).lower()
                if (
                    "foreign key" in err_str
                    or "violates foreign key constraint" in err_str
                ):
                    logger.warning(
                        f"Pipeline {pipeline_id} no longer exists in the pipelines "
                        f"table — skipping metadata upsert (FK violation)."
                    )
                    return 0
                logger.error(
                    f"Error upserting pipeline metadata for pipeline {pipeline_id}: {e}"
                )
                return 0


class DataFlowRepository:
    """Repository for DataFlowRecordMonitoring CRUD operations."""

    @staticmethod
    def insert(record: DataFlowRecordMonitoring) -> int:
        """Insert a new data flow record."""
        with DatabaseSession() as session:
            session.execute(
                """
                INSERT INTO data_flow_record_monitoring 
                (pipeline_id, pipeline_destination_id, source_id, pipeline_destination_table_sync_id, table_name, record_count)
                VALUES (%(pipeline_id)s, %(pipeline_destination_id)s, %(source_id)s, %(pipeline_destination_table_sync_id)s, %(table_name)s, %(record_count)s)
                RETURNING id
                """,
                record.to_insert_dict(),
            )
            row = session.fetchone()
            return row["id"] if row else 0

    @staticmethod
    def increment_count(
        pipeline_id: int,
        pipeline_destination_id: int,
        source_id: int,
        table_sync_id: int,
        table_name: str,
        count: int = 1,
    ) -> None:
        """Insert new record count entry (append-only for time series monitoring)."""
        with DatabaseSession() as session:
            session.execute(
                """
                INSERT INTO data_flow_record_monitoring 
                (pipeline_id, pipeline_destination_id, source_id, pipeline_destination_table_sync_id, table_name, record_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_id,
                    pipeline_destination_id,
                    source_id,
                    table_sync_id,
                    table_name,
                    count,
                ),
            )
