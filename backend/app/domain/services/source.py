"""
Source service containing business logic.

Implements business rules and orchestrates repository operations for sources.
"""

from typing import List
from datetime import datetime, timezone, timedelta
import asyncio

from sqlalchemy.orm import Session
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from app.core.logging import get_logger
from app.core.exceptions import EntityNotFoundError
from app.domain.models.history_schema_evolution import HistorySchemaEvolution
from app.domain.models.source import Source
from app.domain.repositories.source import SourceRepository
from app.domain.repositories.wal_monitor_repo import WALMonitorRepository
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
from app.domain.repositories.history_schema_evolution_repo import (
    HistorySchemaEvolutionRepository,
)
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.services.wal_monitor import WALMonitorService
from app.domain.schemas.source import (
    SourceConnectionTest,
    SourceCreate,
    SourceUpdate,
    SourceResponse,
)
from app.domain.schemas.source_detail import (
    SourceDetailResponse,
    SourceTableInfo,
    TableSchemaResponse,
    TableSchemaDiff,
)
from app.domain.schemas.wal_monitor import WALMonitorResponse
from app.domain.services.schema_monitor import SchemaMonitorService


from app.infrastructure.redis import RedisClient
from app.core.security import encrypt_value, decrypt_value

logger = get_logger(__name__)


class SourceService:
    """
    Service layer for Source entity.

    Implements business logic for managing PostgreSQL source configurations.
    """

    def __init__(self, db: Session):
        """Initialize source service."""
        self.db = db
        self.repository = SourceRepository(db)

    def create_source(self, source_data: SourceCreate) -> Source:
        """
        Create a new source.

        Args:
            source_data: Source creation data

        Returns:
            Created source
        """
        logger.info("Creating new source", extra={"name": source_data.name})

        # Encrypt password before saving
        if source_data.pg_password:
            source_data.pg_password = encrypt_value(source_data.pg_password)

        source = self.repository.create(**source_data.dict())

        # Update table list
        try:
            self._update_source_table_list(source)
            # Commit again to save table list
            self.db.commit()
            self.db.refresh(source)
        except Exception as e:
            logger.error(f"Failed to fetch table list: {e}")

        # Initialize WAL monitor status immediately
        try:
            logger.info(
                "Initializing WAL monitor status for new source",
                extra={"source_id": source.id, "name": source.name}
            )
            wal_monitor_service = WALMonitorService()
            wal_monitor_service.monitor_source_sync(source, self.db)
            logger.info(
                "WAL monitor status initialized successfully",
                extra={"source_id": source.id}
            )
        except Exception as e:
            # Don't fail source creation if WAL monitoring fails
            logger.warning(
                "Failed to initialize WAL monitor status",
                extra={"source_id": source.id, "error": str(e)}
            )

        logger.info(
            "Source created successfully",
            extra={"source_id": source.id, "name": source.name},
        )

        return source

    def get_source(self, source_id: int) -> Source:
        """
        Get source by ID.

        Args:
            source_id: Source identifier

        Returns:
            Source entity
        """
        return self.repository.get_by_id(source_id)

    def get_source_by_name(self, name: str) -> Source | None:
        """
        Get source by name.

        Args:
            name: Source name

        Returns:
            Source entity or None
        """
        return self.repository.get_by_name(name)

    def list_sources(self, skip: int = 0, limit: int = 100) -> List[Source]:
        """
        List all sources with pagination.

        Args:
            skip: Number of sources to skip
            limit: Maximum number of sources to return

        Returns:
            List of sources
        """
        return self.repository.get_all(skip=skip, limit=limit)

    def count_sources(self) -> int:
        """
        Count total number of sources.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_source(self, source_id: int, source_data: SourceUpdate) -> Source:
        """
        Update source.

        Args:
            source_id: Source identifier
            source_data: Source update data

        Returns:
            Updated source
        """
        logger.info("Updating source", extra={"source_id": source_id})

        # Filter out None values for partial updates
        update_data = source_data.dict(exclude_unset=True)

        # Remove empty string password if present (treat as no update)
        if "pg_password" in update_data and (
            update_data["pg_password"] is None or update_data["pg_password"] == ""
        ):
            del update_data["pg_password"]

        # Encrypt password if provided
        if "pg_password" in update_data:
            update_data["pg_password"] = encrypt_value(update_data["pg_password"])

        source = self.repository.update(source_id, **update_data)

        # Update table list if connection details changed
        if any(
            k in update_data
            for k in ["pg_host", "pg_port", "pg_database", "pg_username", "pg_password"]
        ):
            try:
                self._update_source_table_list(source)
                self.db.commit()
                self.db.refresh(source)
            except Exception as e:
                logger.error(f"Failed to refresh table list: {e}")

        logger.info("Source updated successfully", extra={"source_id": source.id})

        return source

    def delete_source(self, source_id: int) -> None:
        """
        Delete source.

        Args:
            source_id: Source identifier
        """
        logger.info("Deleting source", extra={"source_id": source_id})

        # Explicitly delete WAL Metrics first
        from app.domain.models.wal_metric import WALMetric

        self.db.query(WALMetric).filter(WALMetric.source_id == source_id).delete()

        self.repository.delete(source_id)

        logger.info("Source deleted successfully", extra={"source_id": source_id})

    def test_connection_config(self, config: SourceConnectionTest) -> bool:
        """
        Test database connection using provided configuration.

        Args:
            config: Source connection details

        Returns:
            True if connection successful, False otherwise
        """
        import psycopg2

        try:
            logger.info(
                "Testing connection configuration",
                extra={
                    "host": config.pg_host,
                    "port": config.pg_port,
                    "db": config.pg_database,
                },
            )

            conn = psycopg2.connect(
                host=config.pg_host,
                port=config.pg_port,
                dbname=config.pg_database,
                user=config.pg_username,
                password=config.pg_password,
                connect_timeout=5,
            )
            conn.close()
            return True
        except ImportError:
            logger.warning("psycopg2 not installed, simulating successful connection")
            return True
        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"error": str(e)},
            )
            return False

    def test_connection(self, source_id: int) -> bool:
        """
        Test database connection for a source.

        Args:
            source_id: Source identifier

        Returns:
            True if connection successful, False otherwise
        """
        source = self.repository.get_by_id(source_id)

        # Create config from source
        config = SourceConnectionTest(
            pg_host=source.pg_host,
            pg_port=source.pg_port,
            pg_database=source.pg_database,
            pg_username=source.pg_username,
            pg_password=decrypt_value(source.pg_password) if source.pg_password else "",
        )

        return self.test_connection_config(config)

    def get_source_details(self, source_id: int, force_refresh: bool = False) -> SourceDetailResponse:
        """
        Get detailed information for a source.

        Includes WAL monitor metrics and table metadata.

        Args:
            source_id: Source identifier
            force_refresh: If True, bypass cache and refresh from source database

        Returns:
            Source details
        """
        # Check cache first (unless force_refresh)
        if not force_refresh:
            try:
                from app.infrastructure.redis_client import RedisClient
                import json
                
                cache_key = f"source_details:{source_id}"
                redis_client = RedisClient.get_instance()
                cached = redis_client.get(cache_key)
                
                if cached:
                    logger.info(f"Cache HIT for source details {source_id}")
                    cached_data = json.loads(cached)
                    return SourceDetailResponse(**cached_data)
            except Exception as e:
                logger.warning(f"Cache read error for source {source_id}: {e}")
        
        # 1. Get Source
        source = self.get_source(source_id)

        # Conditional Realtime Fetch (only if force_refresh=True)
        # This avoids slow network calls to source database on every page load
        if force_refresh:
            self._update_source_table_list(source)
            registered_tables = self._sync_publication_tables(source)
            self.db.add(source)
            self.db.commit()
            self.db.refresh(source)
        else:
            # Fast path: just get registered tables from publication query
            registered_tables = self._get_publication_tables(source)

        # 2. Get WAL Monitor
        wal_monitor_repo = WALMonitorRepository(self.db)
        wal_monitor = wal_monitor_repo.get_by_source(source_id)

        # 3. Get Tables with Version Count
        table_repo = TableMetadataRepository(self.db)
        tables_with_count = table_repo.get_tables_with_version_count(source_id)

        source_tables = []
        for table, count in tables_with_count:
            # Filter: Only include tables present in the REALTIME publication query
            if table.table_name not in registered_tables:
                continue

            # count is now MAX(version_schema) from HistorySchemaEvolution.
            # INITIAL_LOAD has version_schema=1, subsequent changes increment it.
            # If no history records exist yet, default to version 1.
            version = count if count > 0 else 1

            source_tables.append(
                SourceTableInfo(
                    id=table.id,
                    table_name=table.table_name or "Unknown",
                    version=version,
                    schema_table=(
                        list(table.schema_table.values())
                        if isinstance(table.schema_table, dict)
                        else (
                            table.schema_table
                            if isinstance(table.schema_table, list)
                            else []
                        )
                    ),
                )
            )

        # 4. Get Destinations via Pipelines
        pipeline_repo = PipelineRepository(self.db)
        pipelines = pipeline_repo.get_by_source_id(source_id)

        # Extract unique destination names from all pipelines' destinations
        destination_names = list(
            set(
                pd.destination.name
                for p in pipelines
                for pd in p.destinations
                if pd.destination
            )
        )

        result = SourceDetailResponse(
            source=SourceResponse.from_orm(source),
            wal_monitor=(
                WALMonitorResponse.from_orm(wal_monitor) if wal_monitor else None
            ),
            tables=source_tables,
            destinations=destination_names,
        )
        
        # Cache the result for 30 seconds
        try:
            from app.infrastructure.redis_client import RedisClient
            import json
            
            cache_key = f"source_details:{source_id}"
            redis_client = RedisClient.get_instance()
            # Convert to dict for caching
            result_dict = result.dict()
            redis_client.setex(cache_key, 30, json.dumps(result_dict))
            logger.info(f"Cached source details for {source_id} with 30s TTL")
        except Exception as e:
            logger.warning(f"Failed to cache source details for {source_id}: {e}")
        
        return result

    def get_table_schema_by_version(
        self, table_id: int, version: int
    ) -> TableSchemaResponse:
        """
        Get table schema for a specific version with evolution info.

        Args:
            table_id: Table ID
            version: Schema version

        Returns:
            TableSchemaResponse containing columns and diff
        """
        table_repo = TableMetadataRepository(self.db)
        history_repo = HistorySchemaEvolutionRepository(self.db)

        table = table_repo.get_by_id(table_id)
        if not table:
            raise EntityNotFoundError(entity_type="TableMetadata", entity_id=table_id)

        current_version = (
            self.db.query(HistorySchemaEvolution)
            .filter(HistorySchemaEvolution.table_metadata_list_id == table.id)
            .count()
        ) + 1

        if version < 1 or version > current_version:
            raise ValueError(f"Version must be between 1 and {current_version}")

        # 1. Fetch Schema Column Data
        if version == current_version:
            schema_data = table.schema_table
        else:
            history = history_repo.get_by_table_and_version(table.id, version)
            if not history:
                raise EntityNotFoundError(
                    entity_type="HistorySchemaEvolution",
                    entity_id=f"{table.id}-v{version}",
                )

            # CRITICAL FIX: For INITIAL_LOAD (version 1), schema is in schema_table_new
            # For subsequent versions, schema is in schema_table_old
            if history.changes_type == "INITIAL_LOAD":
                schema_data = history.schema_table_new
            else:
                schema_data = history.schema_table_old

        # Validate schema data is not empty
        if not schema_data:
            logger.warning(
                f"Empty schema data for table {table.table_name} version {version}"
            )
            # Return empty columns list instead of failing
            schema_data = {}

        columns = []
        if isinstance(schema_data, dict):
            columns = list(schema_data.values())
        elif isinstance(schema_data, list):
            columns = schema_data

        # 2. Calculate Diff (Changes introduced IN this version)
        diff = None
        if version > 1:
            # Fetch history for "creation of this version" (Transition V(N-1) -> V(N))
            # History record with version_schema = N - 1
            hist_diff = history_repo.get_by_table_and_version(table.id, version - 1)
            if hist_diff:
                old = hist_diff.schema_table_old or {}
                new = hist_diff.schema_table_new or {}

                # New Columns: Present in NEW but not OLD
                new_cols = list(set(new.keys()) - set(old.keys()))

                # Dropped Columns: Present in OLD but not NEW
                dropped_keys = set(old.keys()) - set(new.keys())
                dropped_cols = [old[k] for k in dropped_keys]

                # Type Changes: Present in both, different types
                type_changes = {}
                common = set(old.keys()) & set(new.keys())
                for k in common:
                    old_t = old[k].get("real_data_type") or old[k].get("data_type")
                    new_t = new[k].get("real_data_type") or new[k].get("data_type")
                    if old_t != new_t:
                        type_changes[k] = {"old_type": old_t, "new_type": new_t}

                diff = TableSchemaDiff(
                    new_columns=new_cols,
                    dropped_columns=dropped_cols,
                    type_changes=type_changes,
                )

        return TableSchemaResponse(columns=columns, diff=diff)

    def _get_connection(self, source: Source):
        """Helper to get postgres connection"""
        conn = psycopg2.connect(
            host=source.pg_host,
            port=source.pg_port,
            dbname=source.pg_database,
            user=source.pg_username,
            password=decrypt_value(source.pg_password) if source.pg_password else None,
            connect_timeout=5,
        )
        return conn

    def _update_source_table_list(self, source: Source) -> None:
        """
        Fetch public tables from source database and upate list_tables.
        """
        try:
            conn = self._get_connection(source)
            with conn.cursor() as cur:
                # 1. (Removed) Fetch tables
                # query = """
                #     SELECT table_name
                #     FROM information_schema.tables
                #     WHERE table_schema = 'public'
                #     AND table_type = 'BASE TABLE';
                # """
                # cur.execute(query)
                # tables = [row[0] for row in cur.fetchall()]
                # source.list_tables = tables

                # 2. Check Publication Status
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (source.publication_name,),
                )
                source.is_publication_enabled = bool(cur.fetchone())

                # 3. Check Replication Status
                slot_name = source.replication_name
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                source.is_replication_enabled = bool(cur.fetchone())

                # 4. Update check timestamp
                # Use Asia/Jakarta (UTC+7)
                jakarta_tz = timezone(timedelta(hours=7))
                source.last_check_replication_publication = datetime.now(jakarta_tz)

            conn.close()

        except Exception as e:
            logger.error(f"Error fetching metadata for source {source.name}: {e}")
            pass
    
    def _pause_running_pipelines_for_source(self, source_id: int) -> None:
        """
        Pause all running pipelines for a given source.
        This is called when publication or replication slot is dropped.
        """
        try:
            # Local import to avoid circular dependency
            from app.domain.services.pipeline import PipelineService
            
            pipeline_repo = PipelineRepository(self.db)
            pipelines = pipeline_repo.get_by_source_id(source_id)
            
            # Filter for running pipelines only
            running_pipelines = [
                p for p in pipelines 
                if p.status in ['START', 'REFRESH']
            ]
            
            if not running_pipelines:
                logger.info(f"No running pipelines found for source {source_id}")
                return
            
            logger.info(
                f"Pausing {len(running_pipelines)} running pipeline(s) for source {source_id}"
            )
            
            pipeline_service = PipelineService(self.db)
            for pipeline in running_pipelines:
                try:
                    pipeline_service.pause_pipeline(pipeline.id)
                    logger.info(
                        f"Successfully paused pipeline {pipeline.id} ({pipeline.name})"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to pause pipeline {pipeline.id} ({pipeline.name}): {e}"
                    )
                    # Continue pausing other pipelines even if one fails
                    continue
            
        except Exception as e:
            logger.error(
                f"Error pausing pipelines for source {source_id}: {e}"
            )
            # Don't raise - this is a best-effort operation

    def _sync_publication_tables(self, source: Source) -> None:
        """
        Sync registered tables from pg_publication_tables to TableMetadata.
        """
        try:
            conn = self._get_connection(source)
            with conn.cursor() as cur:
                # 1. Fetch tables in publication
                query = "SELECT tablename FROM pg_publication_tables WHERE pubname = %s"
                cur.execute(query, (source.publication_name,))
                registered_tables = {row[0] for row in cur.fetchall()}

            # CONN kept open for schema fetching if needed, or close and reopen in helper?
            # Better to reuse conn.
            # But _get_table_schema takes conn.
            # Let's keep conn open or pass checks.
            # Actually conn is closed below. Let's create missing, THEN fetch schema.
            pass  # Placeholder line to match context if needed, but we'll rewrite logic slightly.

            # 2. Sync with local TableMetadata
            table_repo = TableMetadataRepository(self.db)
            existing_tables = table_repo.get_by_source_id(source.id)
            existing_table_names = {t.table_name for t in existing_tables}

            conn_for_schema = self._get_connection(
                source
            )  # Open new conn for schema fetching loop

            try:
                # 3. Create missing tables
                monitor = SchemaMonitorService()
                for table_name in registered_tables:
                    if table_name not in existing_table_names:
                        # Fetch Schema using SchemaMonitorService
                        schema_list = monitor.fetch_table_schema(
                            conn_for_schema, table_name
                        )

                        if not schema_list:
                            logger.warning(
                                f"Skipping table {table_name}: No schema columns found. "
                                "Table may be empty or inaccessible."
                            )
                            continue

                        # Convert to dict format as expected by SchemaMonitor logic
                        schema_details = {
                            col["column_name"]: dict(col) for col in schema_list
                        }

                        # Create new TableMetadata
                        try:
                            new_table = table_repo.create(
                                source_id=source.id,
                                table_name=table_name,
                                schema_table=schema_details,
                            )

                            # Create INITIAL_LOAD history record
                            from app.domain.models.history_schema_evolution import (
                                HistorySchemaEvolution,
                            )

                            history = HistorySchemaEvolution(
                                table_metadata_list_id=new_table.id,
                                schema_table_old={},
                                schema_table_new=schema_details,
                                changes_type="INITIAL_LOAD",
                                version_schema=1,
                            )
                            self.db.add(history)
                            self.db.commit()

                            logger.info(
                                f"Added table {table_name} with schema ({len(schema_list)} columns)"
                            )
                        except Exception as e:
                            # Likely IntegrityError if race condition
                            logger.warning(f"Skipping creation of {table_name}: {e}")
                            self.db.rollback()

                # 4. Fix existing tables without schemas
                for table in existing_tables:
                    # Only process tables still in publication
                    if table.table_name not in registered_tables:
                        continue

                    # Check if schema is missing or empty
                    if not table.schema_table or table.schema_table == {}:
                        logger.info(
                            f"Found existing table {table.table_name} without schema, fetching now..."
                        )
                        try:
                            schema_list = monitor.fetch_table_schema(
                                conn_for_schema, table.table_name
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
                            from app.domain.models.history_schema_evolution import (
                                HistorySchemaEvolution,
                            )

                            existing_history = (
                                self.db.query(HistorySchemaEvolution)
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
                                self.db.add(history)
                                self.db.commit()
                                logger.info(
                                    f"Fixed table {table.table_name}: Added schema and history "
                                    f"({len(schema_list)} columns)"
                                )
                            else:
                                # Update existing INITIAL_LOAD with correct schema
                                existing_history.schema_table_new = schema_dict
                                self.db.commit()
                                logger.info(
                                    f"Fixed table {table.table_name}: Updated schema "
                                    f"({len(schema_list)} columns)"
                                )
                        except Exception as e:
                            logger.error(
                                f"Failed to fetch schema for existing table {table.table_name}: {e}"
                            )
                            self.db.rollback()
                            continue

            finally:
                conn_for_schema.close()

            conn.close()

            # Update total tables count on source
            source.total_tables = len(registered_tables)

            return registered_tables

        except Exception as e:
            logger.error(
                f"Error syncing publication tables for source {source.name}: {e}"
            )
            return set()

    def _get_publication_tables(self, source: Source) -> set:
        """
        Fast fetch of registered tables from pg_publication_tables.
        Lightweight alternative to _sync_publication_tables for read-only operations.
        
        Args:
            source: Source entity
            
        Returns:
            Set of table names in the publication
        """
        try:
            conn = self._get_connection(source)
            with conn.cursor() as cur:
                query = "SELECT tablename FROM pg_publication_tables WHERE pubname = %s"
                cur.execute(query, (source.publication_name,))
                registered_tables = {row[0] for row in cur.fetchall()}
            conn.close()
            return registered_tables
        except Exception as e:
            logger.warning(
                f"Failed to fetch publication tables for source {source.name}: {e}"
            )
            # Fallback: return tables from local metadata
            table_repo = TableMetadataRepository(self.db)
            existing_tables = table_repo.get_by_source_id(source.id)
            return {t.table_name for t in existing_tables}

    def refresh_source_metadata(self, source_id: int) -> None:
        """Manually refresh source metadata."""
        source = self.get_source(source_id)
        
        # Store previous state to detect external drops
        previous_publication_enabled = source.is_publication_enabled
        previous_replication_enabled = source.is_replication_enabled
        
        self._update_source_table_list(source)
        self._sync_publication_tables(source)
        self.db.commit()
        self.db.refresh(source)
        
        # Check if publication or replication was dropped externally
        if (previous_publication_enabled and not source.is_publication_enabled) or \
           (previous_replication_enabled and not source.is_replication_enabled):
            logger.warning(
                f"Publication or replication slot dropped externally for source {source_id}. "
                "Auto-pausing running pipelines."
            )
            self._pause_running_pipelines_for_source(source_id)

        # Invalidate Available Tables Cache
        try:
            redis_client = RedisClient.get_instance()
            redis_client.delete(f"source:{source_id}:tables")
        except Exception as e:
            logger.warning(f"Failed to invalidate cache for source {source_id}: {e}")

    def create_publication(self, source_id: int, tables: List[str]) -> None:
        source = self.get_source(source_id)
        if not tables:
            raise ValueError("At least one table must be selected")

        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                tables_str = ", ".join([f'"{t}"' for t in tables])
                query = f"CREATE PUBLICATION {source.publication_name} FOR TABLE {tables_str} WITH (publish = 'insert, update, delete');"
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error(f"Failed to create publication: {e}")
            raise ValueError(f"Failed to create publication: {str(e)}")

    def drop_publication(self, source_id: int) -> None:
        source = self.get_source(source_id)
        try:
            # Pause running pipelines first
            logger.info(f"Auto-pausing running pipelines for source {source_id} before dropping publication")
            self._pause_running_pipelines_for_source(source_id)
            
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f"DROP PUBLICATION IF EXISTS {source.publication_name};"
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()

            # Cleanup pipeline table sync configurations for this source
            # This will CASCADE delete associated tags via ondelete="CASCADE"
            from app.domain.models.pipeline import Pipeline, PipelineDestination, PipelineDestinationTableSync
            from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList
            
            logger.info(f"Cleaning up pipeline table sync configurations for source {source_id}")
            pipelines = self.db.query(Pipeline).filter(Pipeline.source_id == source_id).all()
            
            # Collect all tag IDs before deletion for cleanup
            all_tag_ids = set()
            for pipeline in pipelines:
                # Get all destinations for this pipeline
                pipeline_dest_ids = [pd.id for pd in pipeline.destinations]
                
                if pipeline_dest_ids:
                    # Get all tag IDs associated with these table syncs
                    tag_ids = (
                        self.db.query(PipelineDestinationTableSyncTag.tag_id)
                        .join(
                            PipelineDestinationTableSync,
                            PipelineDestinationTableSync.id == PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id
                        )
                        .filter(PipelineDestinationTableSync.pipeline_destination_id.in_(pipeline_dest_ids))
                        .distinct()
                        .all()
                    )
                    all_tag_ids.update([tag_id[0] for tag_id in tag_ids])
                    
                    # Delete all table sync configurations for these destinations
                    # CASCADE will automatically delete associated tag associations
                    deleted_count = (
                        self.db.query(PipelineDestinationTableSync)
                        .filter(PipelineDestinationTableSync.pipeline_destination_id.in_(pipeline_dest_ids))
                        .delete(synchronize_session=False)
                    )
                    logger.info(f"Deleted {deleted_count} table sync configurations for pipeline {pipeline.id}")
            
            self.db.commit()

            # Cleanup unused tags after deletion
            if all_tag_ids:
                logger.info(f"Checking {len(all_tag_ids)} tags for cleanup")
                for tag_id in all_tag_ids:
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

            # Cleanup Metadata (this CASCADE deletes history_schema_evolution)
            table_repo = TableMetadataRepository(self.db)
            table_repo.delete_by_source_id(source_id)

            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error(f"Failed to drop publication: {e}")
            self.db.rollback()
            raise ValueError(f"Failed to drop publication: {str(e)}")

    def create_replication_slot(self, source_id: int) -> None:
        source = self.get_source(source_id)
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                slot_name = source.replication_name
                # Check if exists first to avoid error? Or just try create
                # The user asked for specific query
                query = f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput');"
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error(f"Failed to create replication slot: {e}")
            raise ValueError(f"Failed to create replication slot: {str(e)}")

    def drop_replication_slot(self, source_id: int) -> None:
        source = self.get_source(source_id)
        try:
            # Pause running pipelines first
            logger.info(f"Auto-pausing running pipelines for source {source_id} before dropping replication slot")
            self._pause_running_pipelines_for_source(source_id)
            
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                slot_name = source.replication_name
                query = f"SELECT pg_drop_replication_slot('{slot_name}');"
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error(f"Failed to drop replication slot: {e}")
            raise ValueError(f"Failed to drop replication slot: {str(e)}")

    def register_table_to_publication(self, source_id: int, table_name: str) -> None:
        """
        Register a table to the creation publication.
        """
        source = self.get_source(source_id)
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f'ALTER PUBLICATION {source.publication_name} ADD TABLE "{table_name}"'
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)

            # Auto-provision resources if pipelines exist
            try:
                # Local import to avoid circular dependency
                from app.domain.services.pipeline import PipelineService
                from app.domain.repositories.table_metadata_repo import (
                    TableMetadataRepository,
                )

                pipeline_repo = PipelineRepository(self.db)
                pipelines = pipeline_repo.get_by_source_id(source_id)

                if pipelines:
                    logger.info(
                        f"Triggering auto-provisioning for table {table_name} on {len(pipelines)} pipelines"
                    )

                    # Fetch table info (metadata)
                    table_repo = TableMetadataRepository(self.db)
                    table_meta = table_repo.get_by_source_and_name(
                        source_id, table_name
                    )

                    if table_meta:
                        pipeline_service = PipelineService(self.db)
                        for pipeline in pipelines:
                            # Set ready_refresh=True only if pipeline is running
                            if pipeline.status == "START":
                                pipeline.ready_refresh = True

                            for pd in pipeline.destinations:
                                if pd.destination.type == "SNOWFLAKE":
                                    try:
                                        pipeline_service.provision_table(
                                            pipeline, pd.destination, table_meta
                                        )
                                    except Exception as exc:
                                        logger.error(
                                            f"Failed to auto-provision table {table_name} for pipeline {pipeline.id} destination {pd.destination.name}: {exc}"
                                        )

                        # Commit the ready_refresh changes
                        self.db.commit()
                        logger.info(
                            f"Marked {len(pipelines)} pipeline(s) as ready for refresh"
                        )
                    else:
                        logger.warning(
                            f"Metadata for table {table_name} not found after refresh, skipping provisioning"
                        )

            except Exception as e:
                logger.error(f"Auto-provisioning process failed: {e}")
                # Don't raise here to avoid failing the registration itself if provisioning fails

        except Exception as e:
            logger.error(f"Failed to register table {table_name}: {e}")
            raise ValueError(f"Failed to register table: {str(e)}")

    def unregister_table_from_publication(
        self, source_id: int, table_name: str
    ) -> None:
        """
        Unregister (drop) a table from the publication.
        """
        source = self.get_source(source_id)
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f'ALTER PUBLICATION {source.publication_name} DROP TABLE "{table_name}"'
                logger.info(f"Executing: {query}")
                cur.execute(query)
            conn.close()

            # Cleanup Metadata for this table
            table_repo = TableMetadataRepository(self.db)
            table_repo.delete_table(source_id, table_name)

            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error(f"Failed to unregister table {table_name}: {e}")
            raise ValueError(f"Failed to unregister table: {str(e)}")

    def fetch_available_tables(self, source_id: int) -> List[str]:
        """
        Fetch all available public tables from the source database.

        Returns:
            List of table names
        """
        source = self.get_source(source_id)

        # Redis Key
        cache_key = f"source:{source_id}:tables"

        try:
            # 1. Try Cache
            redis_client = RedisClient.get_instance()
            cached_tables = redis_client.get(cache_key)
            if cached_tables:
                import json

                return json.loads(cached_tables)
        except Exception as e:
            logger.warning(f"Redis cache error: {e}")

        # 2. Fetch from DB
        try:
            conn = self._get_connection(source)
            with conn.cursor() as cur:
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cur.execute(query)
                tables = [row[0] for row in cur.fetchall()]
            conn.close()

            # 3. Set Cache (TTL 5 minutes)
            try:
                import json

                redis_client = RedisClient.get_instance()
                redis_client.setex(cache_key, 300, json.dumps(tables))
            except Exception as e:
                logger.warning(f"Failed to cache tables for source {source_id}: {e}")

            return tables
        except Exception as e:
            logger.error(
                f"Failed to fetch available tables for source {source.name}: {e}"
            )
            raise ValueError(f"Failed to fetch tables: {str(e)}")

    def refresh_available_tables(self, source_id: int) -> List[str]:
        """
        Force refresh available tables from source and update cache.
        """
        source = self.get_source(source_id)
        cache_key = f"source:{source_id}:tables"

        try:
            conn = self._get_connection(source)
            with conn.cursor() as cur:
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cur.execute(query)
                tables = [row[0] for row in cur.fetchall()]
            conn.close()

            # Update Cache
            try:
                import json

                redis_client = RedisClient.get_instance()
                redis_client.setex(cache_key, 300, json.dumps(tables))
            except Exception as e:
                logger.error(
                    f"Failed to update cache during refresh for source {source_id}: {e}"
                )

            return tables
        except Exception as e:
            logger.error(f"Failed to refresh table list for source {source.name}: {e}")
            raise ValueError(f"Failed to refresh tables: {str(e)}")

    def fetch_schema(
        self, source_id: int, table_name: str | None = None, only_tables: bool = False
    ) -> dict[str, list[str]]:
        """
        Fetch schema (tables and columns) from the source.

        Args:
            source_id: Source identifier
            table_name: Optional table name to filter by
            only_tables: If True, returns only table names (values are empty lists)

        Returns:
            Dictionary mapping table names to list of column names (or empty list if only_tables)
        """
        source = self.get_source(source_id)

        # Redis Key - include table_name/only_tables if provided
        cache_key = f"source:{source_id}:schema"
        if table_name:
            cache_key += f":table:{table_name}"
        if only_tables:
            cache_key += ":only_tables"

        try:
            # 1. Try Cache
            redis_client = RedisClient.get_instance()
            cached_schema = redis_client.get(cache_key)
            if cached_schema:
                import json

                return json.loads(cached_schema)
        except Exception as e:
            logger.warning(f"Redis cache error: {e}")

        schema_data = {}

        try:
            conn = self._get_connection(source)

            with conn.cursor() as cur:
                if only_tables:
                    # Fetch ONLY table names
                    query = """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_type = 'BASE TABLE'
                    """
                    params = []
                    if table_name:
                        query += " AND table_name ILIKE %s"
                        params.append(table_name)
                    query += " ORDER BY table_name;"

                    cur.execute(query, tuple(params))
                    rows = cur.fetchall()
                    for (table,) in rows:
                        schema_data[table] = []
                else:
                    # Fetch tables and columns from information_schema
                    query = """
                        SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                    """
                    params = []
                    if table_name:
                        # Use ILIKE for case-insensitive matching
                        query += " AND table_name ILIKE %s"
                        params.append(table_name)

                    query += " ORDER BY table_name, ordinal_position;"

                    cur.execute(query, tuple(params))
                    rows = cur.fetchall()

                    for table, column in rows:
                        if table not in schema_data:
                            schema_data[table] = []
                        schema_data[table].append(column)

            conn.close()

            # 3. Cache Result (TTL: 5 minutes)
            try:
                import json

                redis_client = RedisClient.get_instance()
                redis_client.setex(cache_key, 300, json.dumps(schema_data))
            except Exception as e:
                logger.warning(f"Failed to cache source schema: {e}")

            return schema_data

        except Exception as e:
            logger.error(f"Failed to fetch source schema: {e}")
            raise ValueError(f"Failed to fetch source schema: {str(e)}")

    def duplicate_source(self, source_id: int) -> Source:
        """
        Duplicate an existing source.

        Args:
            source_id: Source identifier to duplicate

        Returns:
            New Source entity
        """
        from sqlalchemy import select
        from app.core.exceptions import DuplicateEntityError

        original_source = self.get_source(source_id)

        # Prepare base names for duplication
        base_name = original_source.name
        base_rep_name = original_source.replication_name
        base_pub_name = original_source.publication_name

        # Generate new name with "-copy" prefix
        # Use a try-catch approach with retry logic in case of race conditions
        counter = 1
        max_retries = 100
        created_source = None

        while created_source is None and counter <= max_retries:
            new_name = (
                f"{base_name}-copy" if counter == 1 else f"{base_name}-copy-{counter}"
            )
            # Update replication name for each attempt to avoid replication_name conflicts
            attempt_rep_name = (
                f"{base_rep_name}_copy"
                if counter == 1
                else f"{base_rep_name}_copy_{counter}"
            )
            # Update publication name with -copy suffix
            attempt_pub_name = (
                f"{base_pub_name}_copy"
                if counter == 1
                else f"{base_pub_name}_copy_{counter}"
            )

            try:
                # 3. Create new source configuration
                source_data = SourceCreate(
                    name=new_name,
                    pg_host=original_source.pg_host,
                    pg_port=original_source.pg_port,
                    pg_database=original_source.pg_database,
                    pg_username=original_source.pg_username,
                    pg_password=(
                        decrypt_value(original_source.pg_password)
                        if original_source.pg_password
                        else None
                    ),
                    publication_name=attempt_pub_name,
                    replication_name=attempt_rep_name,
                )

                created_source = self.create_source(source_data)

            except DuplicateEntityError as e:
                # Name or replication_name already exists, try with next counter
                logger.debug(
                    f"Duplicate detected for {new_name}, trying next counter",
                    extra={"counter": counter, "error": str(e)},
                )
                counter += 1
                if counter > max_retries:
                    logger.error(
                        "Failed to create duplicate source after max retries",
                        extra={"original_id": source_id, "base_name": base_name},
                    )
                    raise

        return created_source
