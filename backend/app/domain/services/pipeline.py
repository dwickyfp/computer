"""
Pipeline service containing business logic.

Implements business rules and orchestrates repository operations for pipelines.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEntityError
from app.core.logging import get_logger
from app.domain.models.pipeline import (
    Pipeline,
    PipelineMetadata,
    PipelineStatus,
    PipelineDestination,
    PipelineDestinationTableSync,
)
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
from app.domain.schemas.pipeline import (
    PipelineCreate,
    PipelineUpdate,
    PipelineDestinationResponse,
    PipelineDestinationTableSyncResponse,
    TableValidationResponse,
)
from app.domain.services.source import SourceService
from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
from app.core.security import decrypt_value
from sqlalchemy import func, desc, and_
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import duckdb
import hashlib
import json
import re
import base64
from app.infrastructure.redis import get_redis
from app.domain.schemas.pipeline_preview import PipelinePreviewRequest, PipelinePreviewResponse

logger = get_logger(__name__)


class PipelineService:
    """
    Service layer for Pipeline entity.

    Implements business logic for managing ETL pipeline configurations.
    """

    def __init__(self, db: Session):
        """Initialize pipeline service."""
        self.db = db
        self.repository = PipelineRepository(db)

    def mark_ready_for_refresh(self, pipeline_id: int) -> None:
        """
        Mark pipeline as ready for refresh.
        Only sets ready_refresh for running pipelines (status='START').

        Args:
            pipeline_id: Pipeline identifier
        """
        try:
            pipeline = self.repository.get_by_id(pipeline_id)
            if pipeline.status == "START":
                pipeline.ready_refresh = True
                self.db.commit()
                logger.info(f"Marked pipeline {pipeline_id} as ready for refresh")
            else:
                logger.info(
                    f"Skipping ready_refresh for pipeline {pipeline_id} (status: {pipeline.status})"
                )
        except Exception as e:
            logger.error(f"Failed to mark pipeline {pipeline_id} for refresh: {e}")

    def _cleanup_unused_tags(self, tag_ids: list[int]) -> None:
        """
        Cleanup tags that are no longer associated with any table sync.

        Args:
            tag_ids: List of tag IDs to check and cleanup
        """
        from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList

        for tag_id in tag_ids:
            # Check if tag is still used
            count = (
                self.db.query(PipelineDestinationTableSyncTag)
                .filter(PipelineDestinationTableSyncTag.tag_id == tag_id)
                .count()
            )

            if count == 0:
                # Tag is unused, delete it
                tag = self.db.query(TagList).filter(TagList.id == tag_id).first()
                if tag:
                    logger.info(
                        f"Auto-deleting unused tag: {tag.tag}",
                        extra={"tag_id": tag_id, "tag_name": tag.tag},
                    )
                    self.db.delete(tag)
                    self.db.commit()
                    logger.info(
                        f"Unused tag deleted: {tag.tag}",
                        extra={"tag_id": tag_id},
                    )

    def create_pipeline(self, pipeline_data: PipelineCreate) -> Pipeline:
        """
        Create a new pipeline with associated metadata.

        Args:
            pipeline_data: Pipeline creation data

        Returns:
            Created pipeline
        """
        logger.info("Creating new pipeline", extra={"name": pipeline_data.name})

        # Check if source is already used in another pipeline
        existing_pipelines = self.repository.get_by_source_id(pipeline_data.source_id)
        if existing_pipelines:
            raise DuplicateEntityError(
                entity_type="Pipeline",
                field="source_id",
                value=pipeline_data.source_id,
                details={"message": "Source is already connected to a pipeline"},
            )

        # Force status to PAUSE for initialization
        pipeline_data.status = PipelineStatus.PAUSE

        # Create pipeline with metadata using repository method
        # Note: PipelineCreate no longer has destination_id
        pipeline = self.repository.create_with_metadata(**pipeline_data.dict())

        logger.info(
            "Pipeline created successfully",
            extra={"pipeline_id": pipeline.id, "name": pipeline.name},
        )

        return pipeline

    def add_pipeline_destination(
        self, pipeline_id: int, destination_id: int
    ) -> Pipeline:
        """
        Add a destination to an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            destination_id: Destination identifier

        Returns:
            Updated pipeline
        """
        logger.info(
            "Adding destination to pipeline",
            extra={"pipeline_id": pipeline_id, "destination_id": destination_id},
        )

        pipeline = self.repository.get_by_id(pipeline_id)

        # Check if destination already exists
        existing = (
            self.db.query(PipelineDestination)
            .filter_by(pipeline_id=pipeline_id, destination_id=destination_id)
            .first()
        )
        if existing:
            raise DuplicateEntityError(
                entity_type="PipelineDestination",
                field="destination_id",
                value=destination_id,
                details={"message": "Destination is already added to this pipeline"},
            )

        # Add destination
        new_dest = PipelineDestination(
            pipeline_id=pipeline_id, destination_id=destination_id
        )
        self.db.add(new_dest)
        self.db.commit()
        self.db.refresh(pipeline)

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

        return self.repository.get_by_id_with_relations(pipeline_id)

    def remove_pipeline_destination(
        self, pipeline_id: int, destination_id: int
    ) -> Pipeline:
        """
        Remove a destination from an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            destination_id: Destination identifier

        Returns:
            Updated pipeline
        """
        logger.info(
            "Removing destination from pipeline",
            extra={"pipeline_id": pipeline_id, "destination_id": destination_id},
        )

        pipeline = self.repository.get_by_id(pipeline_id)

        # Check if destination exists
        existing = (
            self.db.query(PipelineDestination)
            .filter_by(pipeline_id=pipeline_id, destination_id=destination_id)
            .first()
        )
        if not existing:
            # If not found, just return current pipeline (idempotent) or raise error?
            # For idempotency, let's log and return.
            logger.warning(
                "Destination not found in pipeline",
                extra={"pipeline_id": pipeline_id, "destination_id": destination_id},
            )
            return self.repository.get_by_id_with_relations(pipeline_id)

        # Collect tag IDs from all table syncs before deletion
        tag_ids = [
            tag_assoc.tag_id
            for table_sync in existing.table_syncs
            for tag_assoc in table_sync.tag_associations
        ]

        # Remove destination (CASCADE will delete table_syncs and tag associations)
        self.db.delete(existing)
        self.db.commit()
        
        # Cleanup unused tags after deletion
        if tag_ids:
            logger.info(f"Checking {len(tag_ids)} tags for cleanup after removing destination from pipeline")
            self._cleanup_unused_tags(tag_ids)
        
        self.db.refresh(pipeline)

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

        return self.repository.get_by_id_with_relations(pipeline_id)

    def validate_target_table(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> TableValidationResponse:
        """
        Validate table name for a destination.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline Destination identifier
            table_name: Table name to validate

        Returns:
            Validation response
        """
        # 1. Basic format validation
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
            return TableValidationResponse(
                valid=False,
                exists=False,
                message="Table name must start with a letter or underscore and contain only alphanumeric characters and underscores.",
            )

        # 2. Get destination
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)
        pipeline_dest = next(
            (pd for pd in pipeline.destinations if pd.id == pipeline_destination_id),
            None,
        )

        if not pipeline_dest:
            # Try to find by destination_id directly if not found by pipeline_destination_id (sometimes frontend sends one or the other)
            # But the API arg is pipeline_destination_id.
            # Let's double check logic. The method signature says pipeline_destination_id.
            # If checking fails, raise error.
            from app.core.exceptions import EntityNotFoundError

            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        destination = pipeline_dest.destination

        if destination.type == "POSTGRES":
            try:
                import psycopg2

                # IMPORTANT: Connect to DESTINATION database to check if table exists there
                # (NOT the source database - we're validating the target table name)
                dest_host = destination.config.get("host")
                dest_port = destination.config.get("port")
                dest_database = destination.config.get("database")
                dest_user = destination.config.get("user")

                logger.info(
                    f"Validating table '{table_name}' in DESTINATION database "
                    f"(host: {dest_host}:{dest_port}, database: {dest_database}, user: {dest_user})"
                )

                conn = psycopg2.connect(
                    host=dest_host,
                    port=dest_port,
                    dbname=dest_database,
                    user=dest_user,
                    password=decrypt_value(destination.config.get("password")),
                    connect_timeout=5,
                )
                cursor = conn.cursor()

                try:
                    # Check existence in DESTINATION database schema
                    pg_schema = destination.config.get("schema") or "public"

                    # Set statement timeout to prevent hanging (5 seconds)
                    cursor.execute("SET statement_timeout = '5s'")

                    # CRITICAL DEBUG: Verify actual database connection (fast query)
                    cursor.execute("SELECT current_database(), current_user")
                    conn_info = cursor.fetchone()
                    actual_db = conn_info[0]
                    actual_user = conn_info[1]

                    logger.info(
                        f"Validating in database='{actual_db}', schema='{pg_schema}', "
                        f"table='{table_name}', user='{actual_user}'"
                    )

                    if actual_db != dest_database:
                        logger.error(
                            f"DATABASE MISMATCH! Connected to '{actual_db}' but expected '{dest_database}'"
                        )

                    # OPTIMIZED: Single query using pg_catalog (much faster than information_schema)
                    # Check both exact match and lowercase for table name
                    query = """
                        WITH target_check AS (
                            SELECT EXISTS (
                                SELECT 1 FROM pg_catalog.pg_tables 
                                WHERE schemaname = %s 
                                AND (tablename = %s OR tablename = LOWER(%s))
                            ) as exists_in_schema
                        ),
                        other_schema_check AS (
                            SELECT schemaname 
                            FROM pg_catalog.pg_tables 
                            WHERE (tablename = %s OR tablename = LOWER(%s))
                            AND schemaname != %s
                            LIMIT 1
                        )
                        SELECT 
                            tc.exists_in_schema,
                            osc.schemaname as other_schema
                        FROM target_check tc
                        LEFT JOIN other_schema_check osc ON true
                    """

                    cursor.execute(
                        query,
                        (pg_schema, table_name, table_name, table_name, table_name, pg_schema)
                    )
                    result = cursor.fetchone()
                    exists = result[0] if result else False
                    other_schema = result[1] if result and len(result) > 1 else None

                    if exists:
                        logger.info(f"Table '{table_name}' FOUND in schema '{pg_schema}'")
                        return TableValidationResponse(
                            valid=True,
                            exists=True,
                            message=f"Table '{table_name}' already exists in DESTINATION database '{actual_db}' schema '{pg_schema}'. It will be used as target.",
                        )
                    else:
                        logger.warning(f"Table '{table_name}' NOT FOUND in schema '{pg_schema}'")
                        
                        # Build helpful message
                        msg = f"Table '{table_name}' does not exist in DESTINATION database '{actual_db}' schema '{pg_schema}' and will be created."
                        
                        if other_schema:
                            msg += f" (Note: Table exists in schema '{other_schema}' - check your destination schema configuration)"
                            logger.warning(f"Table found in OTHER schema: '{other_schema}'")
                        
                        return TableValidationResponse(
                            valid=False,
                            exists=False,
                            message=msg,
                        )
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.error(f"Failed to validate Postgres table: {e}")
                return TableValidationResponse(
                    valid=False,  # If we can't connect, can we validate? Maybe allow it if it's just connectivity issue?
                    # Ideally we fail validation if we can't check.
                    exists=False,
                    message=f"Failed to validate against Postgres destination: {str(e)}",
                )

        if destination.type != "SNOWFLAKE":
            # For others, we might just check regex for now
            return TableValidationResponse(
                valid=True,
                exists=False,
                message="Validation only fully supported for Snowflake and Postgres currently. Basic syntax check passed.",
            )

        # 3. Check existence in DESTINATION (Snowflake)
        try:
            logger.info(
                f"Validating table '{table_name}' in DESTINATION Snowflake database "
                f"(database: {destination.config.get('database')}, "
                f"schema: {destination.config.get('schema')})"
            )

            conn = self._get_snowflake_connection(destination)
            cursor = conn.cursor()
            try:
                config = destination.config
                db = config.get("database")
                schema = config.get("schema")

                exists = self._check_table_exists(cursor, db, schema, table_name)

                logger.info(
                    f"Table '{table_name}' in destination {db}.{schema}: exists={exists}"
                )

                if exists:
                    return TableValidationResponse(
                        valid=True,
                        exists=True,
                        message=f"✓ Table '{table_name}' already exists in DESTINATION database {db}.{schema}. It will be used as target.",
                    )
                else:
                    return TableValidationResponse(
                        valid=True,
                        exists=False,
                        message=f"ℹ Table '{table_name}' is valid and will be created in DESTINATION database {db}.{schema}.",
                    )
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error(f"Failed to validate table name against destination: {e}")
            return TableValidationResponse(
                valid=False,
                exists=False,
                message=f"Failed to validate against DESTINATION database: {str(e)}",
            )

    def get_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Get pipeline by ID with all related entities.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Pipeline entity with relations
        """
        return self.repository.get_by_id_with_relations(pipeline_id)

    def get_pipeline_by_name(self, name: str) -> Pipeline | None:
        """
        Get pipeline by name.

        Args:
            name: Pipeline name

        Returns:
            Pipeline entity or None
        """
        return self.repository.get_by_name(name)

    def list_pipelines(self, skip: int = 0, limit: int = 100) -> List[Pipeline]:
        """
        List all pipelines with pagination.

        Args:
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with relations
        """
        return self.repository.get_all_with_relations(skip=skip, limit=limit)

    def list_pipelines_by_status(
        self, status: PipelineStatus, skip: int = 0, limit: int = 100
    ) -> List[Pipeline]:
        """
        List pipelines filtered by status.

        Args:
            status: Pipeline status to filter by
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with specified status
        """
        return self.repository.get_by_status(status=status, skip=skip, limit=limit)

    def count_pipelines(self) -> int:
        """
        Count total number of pipelines.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_pipeline(
        self, pipeline_id: int, pipeline_data: PipelineUpdate
    ) -> Pipeline:
        """
        Update an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_data: Pipeline update data

        Returns:
            Updated pipeline
        """
        logger.info(
            "Updating pipeline",
            extra={
                "pipeline_id": pipeline_id,
                "fields": pipeline_data.dict(exclude_unset=True),
            },
        )

        # Get existing pipeline to ensure it exists
        pipeline = self.repository.get_by_id(pipeline_id)

        # Update pipeline
        updated_pipeline = self.repository.update(
            pipeline_id, **pipeline_data.dict(exclude_unset=True)
        )

        logger.info(
            "Pipeline updated successfully",
            extra={"pipeline_id": updated_pipeline.id, "name": updated_pipeline.name},
        )

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

        return updated_pipeline

    def delete_pipeline(self, pipeline_id: int) -> None:
        """
        Delete a pipeline and its associated metadata.

        Args:
            pipeline_id: Pipeline identifier
        """
        logger.info("Deleting pipeline", extra={"pipeline_id": pipeline_id})

        # Verify pipeline exists before deletion and collect tag IDs
        pipeline = self.repository.get_by_id(pipeline_id)
        
        # Collect all tag IDs from all table syncs across all destinations
        from app.domain.models.tag import PipelineDestinationTableSyncTag
        
        tag_ids = set()
        for destination in pipeline.destinations:
            for table_sync in destination.table_syncs:
                for tag_assoc in table_sync.tag_associations:
                    tag_ids.add(tag_assoc.tag_id)
        
        # Delete pipeline (metadata will cascade)
        self.repository.delete(pipeline_id)
        
        # Cleanup unused tags after deletion
        if tag_ids:
            logger.info(f"Checking {len(tag_ids)} tags for cleanup after pipeline deletion")
            self._cleanup_unused_tags(list(tag_ids))

        logger.info("Pipeline deleted successfully", extra={"pipeline_id": pipeline_id})

    def start_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Start a pipeline by setting its status to START.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Starting pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.start()

        # Update metadata status to RUNNING
        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_running()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline started successfully", extra={"pipeline_id": pipeline_id})

        return pipeline

    def pause_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Pause a pipeline by setting its status to PAUSE.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Pausing pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.pause()

        # Update metadata status to PAUSED
        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_paused()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline paused successfully", extra={"pipeline_id": pipeline_id})

        return pipeline

    def refresh_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Trigger a pipeline refresh by setting its status to REFRESH.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        from datetime import datetime

        logger.info("Refreshing pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.refresh()
        pipeline.last_refresh_at = datetime.now(ZoneInfo("Asia/Jakarta"))

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline refresh triggered", extra={"pipeline_id": pipeline_id})

        return pipeline

    def record_pipeline_error(self, pipeline_id: int, error_message: str) -> Pipeline:
        """
        Record an error for a pipeline.

        Args:
            pipeline_id: Pipeline identifier
            error_message: Error description

        Returns:
            Updated pipeline
        """
        logger.error(
            "Recording pipeline error",
            extra={"pipeline_id": pipeline_id, "error": error_message},
        )

        pipeline = self.repository.get_by_id(pipeline_id)

        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_error(error_message)

        self.db.commit()
        self.db.refresh(pipeline)

        return pipeline

    def clear_pipeline_error(self, pipeline_id: int) -> Pipeline:
        """
        Clear error state for a pipeline.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Clearing pipeline error", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)

        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.clear_error()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline error cleared", extra={"pipeline_id": pipeline_id})

        return pipeline

    def initialize_pipeline(self, pipeline_id: int) -> None:
        """
        Background task to initialize pipeline resources in Snowflake.
        """
        logger.info(
            "Starting pipeline initialization", extra={"pipeline_id": pipeline_id}
        )

        try:
            # 1. Get Pipeline and Progress
            pipeline = self.repository.get_by_id_with_relations(pipeline_id)
            progress = pipeline.pipeline_progress

            self._update_progress(progress, 0, "Starting initialization", "IN_PROGRESS")

            # 2. Get Source Tables
            self._update_progress(progress, 10, "Fetching source tables", "IN_PROGRESS")
            source_service = SourceService(self.db)
            source_details = source_service.get_source_details(pipeline.source_id)

            # Use source_details.tables (list of source_detail.SourceTableInfo)
            # SourceTableInfo has: id, table_name, schema_table (List[dict])
            tables = source_details.tables

            if not tables:
                self._update_progress(
                    progress, 100, "No tables to process", "COMPLETED"
                )
                pipeline.status = PipelineStatus.PAUSE.value
                self.db.commit()
                return

            # 3. Connect to Snowflake (Iterate over destinations)
            self._update_progress(
                progress, 20, "Initializing destinations", "IN_PROGRESS"
            )

            for index, p_dest in enumerate(pipeline.destinations):
                destination = p_dest.destination
                # Only support Snowflake for now in this provisioner?
                # User asked for Postgres later.
                if destination.type != "SNOWFLAKE":
                    logger.info(
                        f"Skipping provisioning for non-Snowflake destination: {destination.name}"
                    )
                    continue

                conn = self._get_snowflake_connection(destination)
                cursor = conn.cursor()

                try:
                    # Set context
                    config = destination.config
                    landing_db = config.get("landing_database")
                    landing_schema = config.get("landing_schema")
                    target_db = config.get("database")
                    target_schema = config.get("schema")

                    # Validate configuration
                    if not all([landing_db, landing_schema, target_db, target_schema]):
                        raise ValueError(
                            f"Destination configuration incomplete for pipeline {pipeline.name}. "
                            "Ensure landing_database, landing_schema, database, and schema are set."
                        )

                    # Check Databases/Schemas existence? usually assumed or created.
                    # Just use them.

                    # Need TableMetadataRepository to update flags
                    from app.domain.repositories.table_metadata_repo import (
                        TableMetadataRepository,
                    )

                    tm_repo = TableMetadataRepository(self.db)

                    total_tables = len(tables)
                    for index, table in enumerate(tables):
                        current_percent = 20 + int((index / total_tables) * 70)
                        self._update_progress(
                            progress,
                            current_percent,
                            f"Processing table: {table.table_name}",
                            "IN_PROGRESS",
                        )

                        # Process single table using reusable method
                        self.provision_table(
                            pipeline, destination, table, cursor, close_cursor=False
                        )

                finally:
                    cursor.close()
                    conn.close()

            # 5. Finalize
            self._update_progress(
                progress, 100, "Initialization completed", "COMPLETED"
            )
            pipeline.status = PipelineStatus.PAUSE.value
            self.db.commit()

        except Exception as e:
            logger.error(f"Pipeline initialization failed: {e}", exc_info=True)
            # Re-fetch progress attached to session if needed, but it should be attached
            try:
                if progress:
                    self._update_progress(
                        progress,
                        progress.progress,
                        "Initialization failed",
                        "FAILED",
                        str(e),
                    )
            except:
                pass

    def provision_table(
        self,
        pipeline: Pipeline,
        destination,
        table_info,
        cursor=None,
        close_cursor=False,
    ) -> None:
        """
        Provision Snowflake resources for a single table.

        Args:
            pipeline: Pipeline entity
            destination: Destination entity (Snowflake)
            table_info: SourceTableInfo object or similar struct with table_name, schema_definition, id
            cursor: Optional existing Snowflake cursor
            close_cursor: Whether to close the cursor if it was created internally
        """
        logger.info(
            f"Provisioning table {table_info.table_name} for pipeline {pipeline.name} to destination {destination.name}"
        )

        # Find PipelineDestination
        pipeline_dest = next(
            (pd for pd in pipeline.destinations if pd.destination_id == destination.id),
            None,
        )
        if not pipeline_dest:
            logger.error(
                f"PipelineDestination not found for pipeline {pipeline.id} and destination {destination.id}"
            )
            return

        # Get or Create PipelineDestinationTableSync
        table_name = table_info.table_name

        # Check if exists
        sync_record = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(pipeline_destination_id=pipeline_dest.id, table_name=table_name)
            .first()
        )

        if not sync_record:
            sync_record = PipelineDestinationTableSync(
                pipeline_destination_id=pipeline_dest.id,
                table_name=table_name,
                table_name_target=table_name,  # Default target name same as source
                is_exists_table_landing=False,
                is_exists_stream=False,
                is_exists_task=False,
                is_exists_table_destination=False,
            )
            self.db.add(sync_record)
            self.db.flush()  # Flush to get ID if needed, though we operate on object

        conn = None
        if cursor is None:
            conn = self._get_snowflake_connection(destination)
            cursor = conn.cursor()
            close_cursor = True

        try:
            config = destination.config
            target_db = config.get("database")
            target_schema = config.get("schema")
            landing_db = config.get("landing_database")
            landing_schema = config.get("landing_schema")

            # Handle different object structures (SourceTableInfo vs Pydantic model)
            if isinstance(table_info, dict):
                columns = table_info["schema_definition"]
                table_id = table_info["id"]
            else:
                columns = getattr(table_info, "schema_definition", None)

            # Ensure we get the columns correctly, handling potential alias or missing fields
            if not columns and hasattr(table_info, "schema_table"):
                # Fallback to schema_table if schema_definition is missing/empty
                st = getattr(table_info, "schema_table")
                if isinstance(st, list):
                    columns = st
                elif isinstance(st, dict):
                    columns = list(st.values())

            # Final validation
            if not columns:
                logger.error(
                    f"Table {table_name} has no schema definition (columns). Skipping provisioning."
                )
                raise ValueError(
                    f"Table {table_name} has no columns defined. Please refresh source metadata."
                )

            # A. Landing Table (always recreate to ensure schema is up-to-date)
            landing_table = f"LANDING_{table_name}"
            logger.info(
                f"Recreating landing table {landing_db}.{landing_schema}.{landing_table}"
            )
            # Drop existing landing table first (CASCADE to also drop dependent stream)
            cursor.execute(
                f"DROP TABLE IF EXISTS {landing_db}.{landing_schema}.{landing_table} CASCADE"
            )
            landing_ddl = self._generate_landing_ddl(
                landing_db, landing_schema, landing_table, columns
            )
            cursor.execute(landing_ddl)
            sync_record.is_exists_table_landing = True

            # B. Stream (always recreate after landing table recreation)
            stream_name = f"STREAM_{landing_table}"
            logger.info(
                f"Recreating stream {landing_db}.{landing_schema}.{stream_name}"
            )
            stream_ddl = f"CREATE OR REPLACE STREAM {landing_db}.{landing_schema}.{stream_name} ON TABLE {landing_db}.{landing_schema}.{landing_table}"
            cursor.execute(stream_ddl)
            sync_record.is_exists_stream = True

            # C. Destination Table
            target_table = table_name
            # Check if table already exists (if flag is false, double check DB)
            if not sync_record.is_exists_table_destination:
                if self._check_table_exists(
                    cursor, target_db, target_schema, target_table
                ):
                    logger.info(
                        f"Target table {target_db}.{target_schema}.{target_table} already exists, skipping creation."
                    )
                    sync_record.is_exists_table_destination = True
                else:
                    logger.info(
                        f"Creating target table {target_db}.{target_schema}.{target_table}"
                    )
                    target_ddl = self._generate_target_ddl(
                        target_db, target_schema, target_table, columns
                    )
                    cursor.execute(target_ddl)
                    sync_record.is_exists_table_destination = True

            # D. Merge Task (always recreate to ensure task definition is up-to-date)
            landing_table = f"LANDING_{table_name}"
            stream_name = f"STREAM_{landing_table}"
            target_table = table_name
            task_name = f"ROSETTA_TASK_MERGE_{table_name}"

            logger.info(f"Recreating task {landing_db}.{landing_schema}.{task_name}")
            # Drop existing task first
            cursor.execute(
                f"DROP TASK IF EXISTS {landing_db}.{landing_schema}.{task_name}"
            )

            task_ddl = self._generate_merge_task_ddl(
                pipeline,
                destination,
                landing_db,
                landing_schema,
                landing_table,
                stream_name,
                target_db,
                target_schema,
                target_table,
                columns,
            )
            cursor.execute(task_ddl)
            cursor.execute(
                f"ALTER TASK {landing_db}.{landing_schema}.{task_name} RESUME"
            )
            sync_record.is_exists_task = True

            self.db.commit()

        finally:
            if close_cursor:
                cursor.close()
                if conn:
                    conn.close()

    def _check_table_exists(self, cursor, db, schema, table_name) -> bool:
        """Check if a table exists in Snowflake."""
        try:
            # Use SHOW TABLES since it's reliable for existence check
            # Note: SHOW TABLES matches roughly so we filter in python or use exact match dependent on driver behavior
            # Ideally: SHOW TABLES LIKE 'tablename' IN SCHEMA db.schema
            query = f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {db}.{schema}"
            cursor.execute(query)

            # The result cursor will have rows if there are matches
            # We iterate to find exact match because LIKE matching can be fuzzy sometimes depending on wildcard chars
            for result in cursor:
                # result structure depends on connector, usually a tuple or dict
                # index 1 usually is 'name' in SHOW TABLES output
                if result[1] == table_name:
                    return True
            return False

        except Exception as e:
            logger.warning(f"Failed to check if table exists: {e}")
            return False

    def _update_progress(self, progress, percent, step, status, details=None):
        progress.progress = percent
        progress.step = step
        progress.status = status
        if details:
            progress.details = details
        self.db.commit()

    def _get_snowflake_connection(self, destination):
        config = destination.config
        private_key_str = config.get("private_key", "").strip()
        passphrase = None
        if config.get("private_key_passphrase"):
            decrypted_passphrase = decrypt_value(config.get("private_key_passphrase"))
            passphrase = decrypted_passphrase.encode()

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
            user=config.get("user"),
            account=config.get("account"),
            private_key=pkb,
            role=config.get("role"),
            warehouse=config.get("warehouse"),
            database=config.get("database"),
            schema=config.get("schema"),
            client_session_keep_alive=False,
            application="Rosetta_ETL",
        )

    def _map_postgres_to_snowflake(self, col: dict, for_landing: bool = False) -> str:
        """
        Map PostgreSQL data type to Snowflake data type.

        Args:
            col: Column metadata dict containing real_data_type, numeric_precision, etc.
            for_landing: If True, spatial types (GEOGRAPHY/GEOMETRY) will be mapped to VARCHAR
                         since data arrives as text from PostgreSQL WAL.

        Returns:
            Snowflake data type string
        """
        # col contains: column_name, real_data_type, numeric_precision, numeric_scale, ...
        # Note: input might be from SchemaMonitor ('real_data_type') or just 'data_type' if from old metadata

        pg_type = str(col.get("real_data_type") or col.get("data_type")).upper()
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
        elif "DATE" in pg_type:
            return "DATE"
        elif "TIMESTAMPTZ" in pg_type or "TIMESTAMP WITH TIME ZONE" in pg_type:
            # PostgreSQL TIMESTAMPTZ -> Snowflake TIMESTAMP_TZ
            return "TIMESTAMP_TZ"
        elif "TIMESTAMP" in pg_type:
            # PostgreSQL TIMESTAMP (without timezone) -> Snowflake TIMESTAMP_NTZ
            return "TIMESTAMP_NTZ"
        elif "TIMETZ" in pg_type or "TIME WITH TIME ZONE" in pg_type:
            # PostgreSQL TIMETZ -> Snowflake TIME (no TZ equivalent, store as TIME)
            return "TIME"
        elif "TIME" in pg_type:
            return "TIME"
        elif "JSON" in pg_type:
            return "VARIANT"
        elif "ARRAY" in pg_type:
            return "ARRAY"
        elif "UUID" in pg_type:
            return "VARCHAR(36)"
        elif "GEOGRAPHY" in pg_type:
            # Landing table receives text from WAL, target table uses native type
            return "VARCHAR" if for_landing else "GEOGRAPHY"
        elif "GEOMETRY" in pg_type:
            # Landing table receives text from WAL, target table uses native type
            return "VARCHAR" if for_landing else "GEOMETRY"
        else:
            return "VARCHAR"

    def _generate_landing_ddl(self, db, schema, table_name, columns):
        cols_ddl = []
        for col in columns:
            col_name = col["column_name"]
            # Pass entire col dict to mapper, with for_landing=True to use VARCHAR for spatial types
            sf_type = self._map_postgres_to_snowflake(col, for_landing=True)
            cols_ddl.append(f"{col_name} {sf_type}")

        cols_ddl.append("operation VARCHAR(1)")
        cols_ddl.append("sync_timestamp_rosetta TIMESTAMP_TZ")

        ddl = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} ({', '.join(cols_ddl)}) ENABLE_SCHEMA_EVOLUTION = TRUE"
        return ddl

    def _generate_target_ddl(self, db, schema, table_name, columns):
        # Precise type (mapped), no default value, primary key

        cols_ddl = []
        pks = []

        for col in columns:
            col_name = col["column_name"]
            sf_type = self._map_postgres_to_snowflake(col)

            # Basic column definition
            definition = f"{col_name} {sf_type}"
            cols_ddl.append(definition)

            # Check PK
            if col.get("is_primary_key") is True:
                pks.append(col_name)

        # Add PK constraint if exists
        if pks:
            pk_cols = ", ".join(pks)
            # Snowflake supports inline or out-of-line. Out-of-line is cleaner for composites or naming.
            cols_ddl.append(f"CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})")

        cols_definition = ",\n            ".join(cols_ddl)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} (
            {cols_definition}
        ) ENABLE_SCHEMA_EVOLUTION = TRUE;
        """
        return ddl

    def _generate_merge_task_ddl(
        self,
        pipeline,
        destination,
        l_db,
        l_schema,
        l_table,
        stream,
        t_db,
        t_schema,
        t_table,
        columns,
    ):
        # 1. Try to find explicit PK
        pk_cols = []
        for col in columns:
            if col.get("is_primary_key") is True:
                pk_cols.append(col["column_name"])

        # 2. Fallback to 'id' or first column if no PK found
        if not pk_cols:
            for col in columns:
                if "id" in col["column_name"].lower():
                    pk_cols.append(col["column_name"])
                    break
        if not pk_cols:
            pk_cols.append(columns[0]["column_name"])

        # Prepare JOIN condition for MERGE
        # T.id = S.id AND T.key2 = S.key2
        join_condition = " AND ".join([f"T.{pk} = S.{pk}" for pk in pk_cols])

        # Prepare Partition By columns for De-duplication
        partition_by = ", ".join(pk_cols)

        col_names = [c["column_name"] for c in columns]

        # Indent utility
        indent = "            "

        # Build a mapping of column names to their PostgreSQL types for spatial conversion
        col_type_map = {}
        for col in columns:
            pg_type = str(col.get("real_data_type") or col.get("data_type")).upper()
            col_type_map[col["column_name"]] = pg_type

        def get_source_value(col_name: str) -> str:
            """Get the source value expression, applying spatial conversion if needed."""
            pg_type = col_type_map.get(col_name, "")
            if "GEOGRAPHY" in pg_type:
                return f"TRY_TO_GEOGRAPHY(S.{col_name})"
            elif "GEOMETRY" in pg_type:
                return f"TRY_TO_GEOMETRY(S.{col_name})"
            return f"S.{col_name}"

        # UPDATE SET clause
        # exclude PKs from update usually? MERGE allows updating everything except join keys usually.
        # But safest to update all non-PKs.
        update_cols = [c for c in col_names if c not in pk_cols]
        if not update_cols:
            # Edge case: table only has PK columns? Then update is no-op usually or not possible.
            # Or maybe just update one col to itself? Dummy update?
            # Let's assume there's always data columns. If not, we might not need update clause, just insert.
            # But for safety, let's keep all columns if no distinct non-PKs (shouldn't happen in real ETL).
            set_clause = ", ".join([f"{c} = {get_source_value(c)}" for c in col_names])
        else:
            set_clause = f",\n{indent}            ".join(
                [f"{c} = {get_source_value(c)}" for c in update_cols]
            )

        val_clause = ", ".join([get_source_value(c) for c in col_names])
        col_list = ", ".join(col_names)

        # Use Snowflake scripting block to run MERGE then DELETE from landing table
        task_ddl = f"""
        CREATE OR REPLACE TASK {l_db}.{l_schema}.ROSETTA_TASK_MERGE_{t_table}
        WAREHOUSE = {destination.config.get("warehouse")}
        SCHEDULE = '60 MINUTE'
        WHEN SYSTEM$STREAM_HAS_DATA('{l_db}.{l_schema}.{stream}')
        AS
        BEGIN
            -- Step 1: Merge data from stream to target table
            MERGE INTO {t_db}.{t_schema}.{t_table} AS T
            USING (
                SELECT * FROM (
                    SELECT 
                        *, 
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

    def get_pipeline_data_flow_stats(
        self, pipeline_id: int, days: int = 7
    ) -> List[dict]:
        """
        Get data flow statistics for a pipeline, grouped by destination, source table, and target table.

        Args:
            pipeline_id: Pipeline identifier
            days: Number of days to look back

        Returns:
            List of stats per table lineage
        """
        # 1. Get Pipeline and Sync Configuration
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)

        # Pre-fetch sync configs for mapping
        # Map: (pipeline_destination_id, source_table_name) -> { target_table: str, dest_name: str }
        sync_map = {}
        if pipeline.destinations:
            for dest in pipeline.destinations:
                for sync in dest.table_syncs:
                    # Map by sync_id if available (future), or fallback to (dest_id, table_name)
                    # For now, let's map by sync.id directly
                    sync_map[sync.id] = {
                        "target_table": sync.table_name_target,
                        "destination_name": dest.destination.name,
                    }
                    # Also keep legacy map for backward compatibility or when sync_id is null
                    key = (dest.id, sync.table_name)
                    if key not in sync_map:
                        sync_map[key] = {
                            "target_table": sync.table_name_target,
                            "destination_name": dest.destination.name,
                        }

        # 2. Daily Stats Query
        start_date = datetime.now(ZoneInfo("Asia/Jakarta")) - timedelta(days=days)

        daily_query = (
            self.db.query(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
                DataFlowRecordMonitoring.table_name,
                func.date_trunc("day", DataFlowRecordMonitoring.created_at).label(
                    "day"
                ),
                func.sum(DataFlowRecordMonitoring.record_count).label("total_count"),
            )
            .filter(
                DataFlowRecordMonitoring.pipeline_id == pipeline_id,
                DataFlowRecordMonitoring.created_at >= start_date,
            )
            .group_by(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
                DataFlowRecordMonitoring.table_name,
                func.date_trunc("day", DataFlowRecordMonitoring.created_at),
            )
            .order_by(DataFlowRecordMonitoring.table_name, desc("day"))
        )

        daily_results = daily_query.all()

        # 3. Recent 5 Minutes Stats Query
        five_min_ago = datetime.now(ZoneInfo("Asia/Jakarta")) - timedelta(minutes=5)

        recent_query = (
            self.db.query(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
                DataFlowRecordMonitoring.table_name,
                DataFlowRecordMonitoring.created_at,
                DataFlowRecordMonitoring.record_count,
            )
            .filter(
                DataFlowRecordMonitoring.pipeline_id == pipeline_id,
                DataFlowRecordMonitoring.created_at >= five_min_ago,
            )
            .order_by(DataFlowRecordMonitoring.created_at.asc())
        )

        recent_results = recent_query.all()

        # 4. Aggregating results
        stats_map = {}

        # Helper to get meta info using sync_id or fallback
        def get_meta(dest_id, sync_id, table_name):
            # 1. Try sync_id first
            if sync_id and sync_id in sync_map:
                return sync_map[sync_id]

            # 2. Try (dest_id, table)
            if dest_id:
                info = sync_map.get((dest_id, table_name))
                if info:
                    # Check if this info is a specific dict or just one of them?
                    # The tuple key map might be ambiguous if multiple syncs same source-dest-table (rare but possible with custom sql?)
                    # But for general case it works.
                    return info

            # Fallback
            return {
                "target_table": table_name,
                "destination_name": "Unknown Destination",
            }

        # Unique Key generator
        def get_key(dest_id, sync_id, table):
            if sync_id:
                return f"sync_{sync_id}"
            return f"{dest_id or 'none'}_{table}"

        # Process Daily Stats
        for row in daily_results:
            key = get_key(
                row.pipeline_destination_id,
                row.pipeline_destination_table_sync_id,
                row.table_name,
            )
            if key not in stats_map:
                meta = get_meta(
                    row.pipeline_destination_id,
                    row.pipeline_destination_table_sync_id,
                    row.table_name,
                )
                stats_map[key] = {
                    "pipeline_destination_id": row.pipeline_destination_id,
                    "pipeline_destination_table_sync_id": row.pipeline_destination_table_sync_id,
                    "table_name": row.table_name,
                    "target_table_name": meta["target_table"],
                    "destination_name": meta["destination_name"],
                    "daily_stats": [],
                    "recent_stats": [],
                }

            stats_map[key]["daily_stats"].append(
                {
                    "date": row.day.isoformat(),
                    "count": int(row.total_count) if row.total_count else 0,
                }
            )

        # Process Recent Stats
        for row in recent_results:
            key = get_key(
                row.pipeline_destination_id,
                row.pipeline_destination_table_sync_id,
                row.table_name,
            )
            if key not in stats_map:
                meta = get_meta(
                    row.pipeline_destination_id,
                    row.pipeline_destination_table_sync_id,
                    row.table_name,
                )
                stats_map[key] = {
                    "pipeline_destination_id": row.pipeline_destination_id,
                    "pipeline_destination_table_sync_id": row.pipeline_destination_table_sync_id,
                    "table_name": row.table_name,
                    "target_table_name": meta["target_table"],
                    "destination_name": meta["destination_name"],
                    "daily_stats": [],
                    "recent_stats": [],
                }

            # Ensure timestamp is timezone-aware (Asia/Jakarta)
            timestamp = row.created_at
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=ZoneInfo("Asia/Jakarta"))

            stats_map[key]["recent_stats"].append(
                {"timestamp": timestamp.isoformat(), "count": row.record_count}
            )

        return list(stats_map.values())

    def get_destination_tables(
        self, pipeline_id: int, pipeline_destination_id: int
    ) -> List[dict]:
        """
        Get tables available for sync with current configuration using Left Join.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier

        Returns:
            List of tables with sync info
        """
        from app.domain.models.table_metadata import TableMetadata
        from app.domain.models.pipeline import (
            PipelineDestination,
            PipelineDestinationTableSync,
        )
        from app.domain.schemas.pipeline import (
            TableWithSyncInfoResponse,
            ColumnSchemaResponse,
            PipelineDestinationTableSyncResponse,
        )
        from app.core.exceptions import EntityNotFoundError

        # Get pipeline to verify it exists and get source_id
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)

        # Verify destination exists for this pipeline
        pipeline_dest_exists = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest_exists:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # 1. Get List of all tables from source metadata
        tm_repo = TableMetadataRepository(self.db)
        all_tables_meta = tm_repo.get_by_source_id(pipeline.source_id)

        # 2. Get existing sync configurations for this destination
        syncs = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(pipeline_destination_id=pipeline_destination_id)
            .all()
        )
        from collections import defaultdict

        syncs_map = defaultdict(list)
        for s in syncs:
            syncs_map[s.table_name].append(s)

        response_list = []
        for table_meta in all_tables_meta:
            # Parse schema
            columns = []
            if table_meta.schema_table:
                # Handle both list (older format) and dict (newer format) schemas
                schema_items = table_meta.schema_table
                if isinstance(schema_items, dict):
                    schema_items = schema_items.values()

                for col in schema_items:
                    if isinstance(col, dict):
                        columns.append(
                            ColumnSchemaResponse(
                                column_name=col.get("column_name", ""),
                                data_type=col.get("real_data_type")
                                or col.get("data_type", ""),
                                real_data_type=col.get("real_data_type"),
                                is_nullable=col.get("is_nullable") in [True, "YES"],
                                is_primary_key=col.get("is_primary_key", False),
                                has_default=col.get("has_default", False),
                                default_value=(
                                    str(col.get("default_value"))
                                    if col.get("default_value") is not None
                                    else None
                                ),
                                numeric_scale=col.get("numeric_scale"),
                                numeric_precision=col.get("numeric_precision"),
                            )
                        )
                    elif isinstance(col, str):
                        # Handle case where schema might be list of strings logic
                        columns.append(
                            ColumnSchemaResponse(
                                column_name=col,
                                data_type="UNKNOWN",
                                is_nullable=True,
                                is_primary_key=False,
                            )
                        )

            # Convert sync configs (list)
            current_syncs = syncs_map[table_meta.table_name]
            sync_configs_response = [
                PipelineDestinationTableSyncResponse.from_orm(s) for s in current_syncs
            ]

            response_list.append(
                TableWithSyncInfoResponse(
                    table_name=table_meta.table_name,
                    columns=columns,
                    sync_configs=sync_configs_response,
                    is_exists_table_landing=any(
                        s.is_exists_table_landing for s in current_syncs
                    ),
                    is_exists_stream=any(s.is_exists_stream for s in current_syncs),
                    is_exists_task=any(s.is_exists_task for s in current_syncs),
                    is_exists_table_destination=any(
                        s.is_exists_table_destination for s in current_syncs
                    ),
                )
            )

        return [r.dict() for r in response_list]

    def save_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_sync_data
    ) -> "PipelineDestinationTableSync":
        """
        Create or update table sync configuration.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_sync_data: Table sync configuration

        Returns:
            Created/updated table sync
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        if table_sync_data.id:
            # Update specific existing sync
            existing = (
                self.db.query(PipelineDestinationTableSync)
                .filter_by(
                    id=table_sync_data.id,
                    pipeline_destination_id=pipeline_destination_id,
                )
                .first()
            )
            if not existing:
                raise EntityNotFoundError(
                    entity_type="PipelineDestinationTableSync",
                    entity_id=table_sync_data.id,
                )

            # Verify table name matches (optional safety check)
            if existing.table_name != table_sync_data.table_name:
                # Should we allow changing source table? Probably not for a sync object.
                pass

            if table_sync_data.custom_sql:
                self._validate_custom_sql(table_sync_data.custom_sql)

            existing.custom_sql = table_sync_data.custom_sql
            existing.filter_sql = table_sync_data.filter_sql
            existing.primary_key_column_target = table_sync_data.primary_key_column_target
            if table_sync_data.table_name_target:
                existing.table_name_target = table_sync_data.table_name_target

            self.db.commit()
            self.db.refresh(existing)

            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)

            return existing
        else:
            # Create NEW sync (Branch)
            # Check if there is already a sync for this table with same target?
            # Or just allow multiple. We should probably uniqueness on (pipeline_destination_id, table_name, table_name_target)
            target_name = (
                table_sync_data.table_name_target or table_sync_data.table_name
            )

            # Optional: Check uniqueness of target for this source
            # ...
            
            if table_sync_data.custom_sql:
                self._validate_custom_sql(table_sync_data.custom_sql)

            new_sync = PipelineDestinationTableSync(
                pipeline_destination_id=pipeline_destination_id,
                table_name=table_sync_data.table_name,
                table_name_target=target_name,
                custom_sql=table_sync_data.custom_sql,
                filter_sql=table_sync_data.filter_sql,
                primary_key_column_target=table_sync_data.primary_key_column_target,
            )
            self.db.add(new_sync)
            self.db.commit()
            self.db.refresh(new_sync)

            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)

            return new_sync

    def save_table_syncs_bulk(
        self, pipeline_id: int, pipeline_destination_id: int, bulk_request
    ) -> List["PipelineDestinationTableSync"]:
        """
        Bulk create or update table sync configurations.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            bulk_request: Bulk table sync configurations

        Returns:
            List of created/updated table syncs
        """
        results = []
        for table_sync_data in bulk_request.tables:
            result = self.save_table_sync(
                pipeline_id, pipeline_destination_id, table_sync_data
            )
            results.append(result)
        return results

    def delete_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> None:
        """
        Remove table from sync configuration.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_name: Table name to remove
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # Find and delete
        sync = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(
                pipeline_destination_id=pipeline_destination_id, table_name=table_name
            )
            .first()
        )

        if sync:
            # Get associated tag IDs before deletion for cleanup
            tag_ids = [
                assoc.tag_id for assoc in sync.tag_associations
            ]
            
            self.db.delete(sync)
            self.db.commit()

            # Cleanup unused tags after deletion
            if tag_ids:
                self._cleanup_unused_tags(tag_ids)

            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)

    def delete_table_sync_by_id(
        self, pipeline_id: int, pipeline_destination_id: int, sync_config_id: int
    ) -> None:
        """
        Remove a specific table sync configuration by ID.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            sync_config_id: Sync configuration ID to remove
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # Find and delete the specific sync by ID
        sync = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(
                id=sync_config_id, pipeline_destination_id=pipeline_destination_id
            )
            .first()
        )

        if not sync:
            raise EntityNotFoundError(
                entity_type="PipelineDestinationTableSync", entity_id=sync_config_id
            )

        # Get associated tag IDs before deletion for cleanup
        tag_ids = [
            assoc.tag_id for assoc in sync.tag_associations
        ]

        self.db.delete(sync)
        self.db.commit()

        # Cleanup unused tags after deletion
        if tag_ids:
            self._cleanup_unused_tags(tag_ids)

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

    def init_snowflake_table(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> dict:
        """
        Initialize Snowflake objects for a single table.

        Creates landing table, stream, task, and target table if they don't exist.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_name: Table name to initialize

        Returns:
            Status of initialization
        """
        from app.domain.repositories.table_metadata_repo import TableMetadataRepository
        from app.core.exceptions import EntityNotFoundError

        logger.info(
            f"Initializing Snowflake table",
            extra={
                "pipeline_id": pipeline_id,
                "pipeline_destination_id": pipeline_destination_id,
                "table_name": table_name,
            },
        )

        # Get pipeline and destination
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)

        # Find the specific pipeline destination
        pipeline_dest = None
        destination = None
        for pd in pipeline.destinations:
            if pd.id == pipeline_destination_id:
                pipeline_dest = pd
                destination = pd.destination
                break

        if not pipeline_dest or not destination:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        if destination.type != "SNOWFLAKE":
            return {"status": "skipped", "message": "Not a Snowflake destination"}

        # Get table metadata
        tm_repo = TableMetadataRepository(self.db)
        table_meta = tm_repo.get_by_source_and_name(pipeline.source_id, table_name)

        if not table_meta:
            raise EntityNotFoundError(entity_type="TableMetadata", entity_id=table_name)

        # Create a simple object to pass to provision_table
        class TableInfo:
            def __init__(self, meta):
                self.id = meta.id
                self.table_name = meta.table_name
                self.schema_table = meta.schema_table

        table_info = TableInfo(table_meta)

        try:
            self.provision_table(pipeline, destination, table_info)
            return {
                "status": "success",
                "message": f"Snowflake objects created for {table_name}",
            }
        except Exception as e:
            logger.error(f"Failed to initialize Snowflake table: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    def _validate_custom_sql(self, sql: str) -> None:
        """
        Validate custom SQL for forbidden keywords.
        
        Args:
            sql: SQL string to validate
            
        Raises:
            ValueError: If SQL contains forbidden keywords
        """
        if not sql:
            return

        # List of forbidden keywords for custom SQL
        # We only allow SELECT statements basically
        forbidden_keywords = [
            "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "GRANT", "REVOKE", 
            "INSERT", "CREATE", "REPLACE", "MERGE"
        ]
        
        # Simple regex check - word boundary to avoid partial matches
        # We check case-insensitive
        for keyword in forbidden_keywords:
            # Look for keyword as a whole word
            if re.search(rf"\b{keyword}\b", sql, re.IGNORECASE):
                raise ValueError(
                    f"SQL validation failed: Operation '{keyword}' is not allowed. "
                    "Only SELECT statements are permitted."
                )

    @staticmethod
    def _filter_sql_to_where_clause(filter_sql: str) -> str:
        """
        Convert a filter_sql string (v2 JSON or legacy semicolon format) to a SQL WHERE clause.
        
        Supports:
        - V2 JSON: {"version": 2, "groups": [...], "interLogic": [...]}
        - Legacy: "column operator value;column2 operator value2"
        
        Returns:
            SQL WHERE clause string (without the WHERE keyword), or empty string.
        """
        if not filter_sql or not filter_sql.strip():
            return ""

        def condition_to_sql(c: dict) -> str:
            column = c.get("column", "")
            if not column:
                return ""
            op = c.get("operator", "").upper()
            value = c.get("value", "")
            value2 = c.get("value2", "")

            if op in ("IS NULL", "IS NOT NULL"):
                return f"{column} {op}"
            if not value and op != "IN":
                return ""
            if op == "BETWEEN" and value2:
                return f"{column} BETWEEN '{value}' AND '{value2}'"
            if op in ("LIKE", "ILIKE"):
                return f"{column} {op} '%{value}%'"
            if op == "IN":
                vals = [v.strip() for v in value.split(",") if v.strip()]
                if not vals:
                    return ""
                quoted = ", ".join(v if re.match(r"^-?\d+(\.\d+)?$", v) else f"'{v}'" for v in vals)
                return f"{column} IN ({quoted})"
            is_num = bool(re.match(r"^-?\d+(\.\d+)?$", value))
            quoted_value = value if is_num else f"'{value}'"
            return f"{column} {c.get('operator', '=')} {quoted_value}"

        # Try V2 JSON format
        try:
            parsed = json.loads(filter_sql)
            if isinstance(parsed, dict) and parsed.get("version") == 2:
                group_sqls = []
                for g in parsed.get("groups", []):
                    parts = [condition_to_sql(c) for c in g.get("conditions", [])]
                    parts = [p for p in parts if p]
                    if not parts:
                        continue
                    intra = g.get("intraLogic", "AND")
                    group_sqls.append(
                        f"({f' {intra} '.join(parts)})" if len(parts) > 1 else parts[0]
                    )
                if not group_sqls:
                    return ""
                result = group_sqls[0]
                inter_logic = parsed.get("interLogic", [])
                for i in range(1, len(group_sqls)):
                    logic = inter_logic[i - 1] if i - 1 < len(inter_logic) else "AND"
                    result += f" {logic} {group_sqls[i]}"
                return result
        except (json.JSONDecodeError, TypeError):
            pass

        # Legacy semicolon format
        parts = [s.strip() for s in filter_sql.split(";") if s.strip()]
        return " AND ".join(parts)

    def preview_custom_sql(self, request: PipelinePreviewRequest) -> PipelinePreviewResponse:
        """
        Preview table data using DuckDB with attached Postgres databases.
        
        When custom SQL is provided, it is rewritten to qualify table names.
        When no custom SQL is provided, a direct query is built from the table name.
        If filter_sql is present, it is added as a WHERE clause.
        
        Args:
            request: Preview request containing table context and optional SQL/filter
            
        Returns:
            Preview response with data and columns
        """
        try:
            # 0. Validate SQL (only if custom SQL is provided)
            if request.sql:
                self._validate_custom_sql(request.sql)

            # 1. Calculate Hash for Caching
            # Include source/dest IDs, filter_sql in hash to prevent cross-context collisions
            # and to invalidate cache when filter changes
            filter_str = request.filter_sql or ""
            sql_str = request.sql or ""
            input_string = f"{sql_str}{request.source_id}{request.destination_id}{request.table_name}{filter_str}"
            query_hash = hashlib.sha256(input_string.encode()).hexdigest()
            cache_key = f"preview:{query_hash}"
            
            # 2. Check Redis Cache
            redis_client = None
            try:
                redis_client = get_redis()
                if redis_client:
                    cached = redis_client.get(cache_key)
                    if cached:
                        try:
                            data = json.loads(cached)
                            logger.info(f"Returning cached preview for key {cache_key}")
                            return PipelinePreviewResponse(**data)
                        except Exception as e:
                            logger.warning(f"Failed to parse cached preview: {e}")
            except Exception as e:
                logger.warning(f"Redis error during preview cache check: {e}")
                
            # 3. Get Source and Destination Configuration
            try:
                from app.domain.repositories.source import SourceRepository
                source_repo = SourceRepository(self.db)
                source_details = source_repo.get_by_id(request.source_id)
                if not source_details:
                    raise ValueError(f"Source {request.source_id} not found")
                
                from app.domain.models.destination import Destination
                dest = self.db.query(Destination).filter(Destination.id == request.destination_id).first()
                if not dest:
                    raise ValueError(f"Destination {request.destination_id} not found")
                
                # Decrypt passwords
                src_pass = decrypt_value(source_details.pg_password)
                src_conn_str = f"postgresql://{source_details.pg_username}:{src_pass}@{source_details.pg_host}:{source_details.pg_port}/{source_details.pg_database}"
                
                dest_config = dest.config
                dest_pass = decrypt_value(dest_config.get("password", ""))
                dest_conn_str = f"postgresql://{dest_config.get('user')}:{dest_pass}@{dest_config.get('host')}:{dest_config.get('port')}/{dest_config.get('database')}"
                
            except Exception as e:
                logger.error(f"Failed to retrieve connection details: {e}")
                return PipelinePreviewResponse(columns=[], data=[], error=f"Failed to retrieve connection details: {str(e)}")

            # 4. Build Query
            # Sanitized alias names (must match what we use in ATTACH below)
            sanitized_source_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_details.name.lower())
            source_prefix = f"pg_src_{sanitized_source_name}"
            
            sanitized_dest_name = re.sub(r"[^a-zA-Z0-9_]", "_", dest.name.lower())
            dest_prefix = f"pg_{sanitized_dest_name}"

            # Parse filter_sql into WHERE clause
            where_clause = ""
            if request.filter_sql:
                parsed_filter = self._filter_sql_to_where_clause(request.filter_sql)
                if parsed_filter:
                    where_clause = f" WHERE {parsed_filter}"

            if request.sql:
                # Custom SQL mode:
                # Flow: 1) Select raw data with filter + limit  2) Apply custom SQL on that data
                #
                # Build a CTE "filtered_source" from the raw table with filter & limit,
                # then rewrite the custom SQL to reference that CTE instead of the raw table.
                filtered_source_cte = (
                    f"SELECT * FROM {source_prefix}.{request.table_name}{where_clause} LIMIT 100"
                )

                # Rewrite table references in custom SQL to point to the filtered CTE
                rewritten_sql = request.sql
                table_pattern = re.compile(
                    rf'(?<![\.\w"]){re.escape(request.table_name)}(?![\.\w"])',
                    re.IGNORECASE
                )
                rewritten_sql = table_pattern.sub("filtered_source", rewritten_sql)
                rewritten_sql = rewritten_sql.strip().rstrip(';')

                final_query = (
                    f"WITH filtered_source AS ({filtered_source_cte}) "
                    f"SELECT * FROM ({rewritten_sql}) AS result_sql LIMIT 100"
                )
            else:
                # Direct table query mode: SELECT * FROM pg_src_<source_name>.<table_name>
                base_query = f"SELECT * FROM {source_prefix}.{request.table_name}"
                final_query = f"{base_query}{where_clause} LIMIT 100"
            
            logger.info(f"Executing preview query: {final_query}")
            
            # 5. Execute in DuckDB
            con = duckdb.connect(":memory:")
            # Set memory limit to avoid OOM
            con.execute("SET memory_limit='1GB'")
            
            # Install & Load Postgres Extension
            con.execute("INSTALL postgres;")
            con.execute("LOAD postgres;")
            
            # Attach databases
            try:
                con.execute(f"ATTACH '{src_conn_str}' AS {source_prefix} (TYPE postgres, READ_ONLY);")
            except Exception as e:
                logger.error(f"Failed to attach source DB: {e}")
                raise ValueError(f"Could not connect to source database: {e}")
                
            try:
                con.execute(f"ATTACH '{dest_conn_str}' AS {dest_prefix} (TYPE postgres, READ_ONLY);")
            except Exception as e:
                logger.warning(f"Failed to attach destination DB: {e}")
                # We continue even if dest fails, as query might only need source
            
            # Execute Query
            result = con.execute(final_query).fetch_arrow_table()
            
            # Process Results
            columns = result.column_names
            data = result.to_pylist()

            # Extract types from Arrow schema
            column_types = []
            for field in result.schema:
                dtype = str(field.type).lower()
                if any(t in dtype for t in ['int', 'float', 'decimal', 'double']):
                    column_types.append('number')
                elif 'bool' in dtype:
                    column_types.append('boolean')
                elif any(t in dtype for t in ['date', 'time', 'timestamp']):
                    column_types.append('date')
                else:
                    column_types.append('text')
            
            # Serialize special types
            serialized_data = []
            for row in data:
                new_row = {}
                for k, v in row.items():
                    if isinstance(v, (datetime, date)):
                        new_row[k] = v.isoformat()
                    elif isinstance(v, (bytes, bytearray)):
                         new_row[k] = base64.b64encode(v).decode('utf-8')
                    else:
                        new_row[k] = v
                serialized_data.append(new_row)
            
            response = PipelinePreviewResponse(columns=columns, column_types=column_types, data=serialized_data)
            
            # 6. Cache Result
            try:
                if redis_client:
                    # Cache for 5 minutes
                    redis_client.setex(cache_key, 300, response.json())
            except Exception as e:
                logger.warning(f"Failed to cache preview result: {e}")
                
            return response
            
        except Exception as e:
            logger.error(f"Preview execution failed: {e}", exc_info=True)
            # Return error in response rather than 500
            return PipelinePreviewResponse(columns=[], column_types=[], data=[], error=str(e))
        finally:
            if 'con' in locals():
                try:
                    con.close()
                except:
                    pass
