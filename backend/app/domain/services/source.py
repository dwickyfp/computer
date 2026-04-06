"""
Source service containing business logic.

Implements business rules and orchestrates repository operations for sources.
"""

from typing import Any, List
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
from app.infrastructure.kafka import create_admin_client
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

    @staticmethod
    def _source_type(source: Source | SourceCreate | SourceUpdate | SourceConnectionTest) -> str:
        return str(getattr(source, "type", "POSTGRES") or "POSTGRES").upper()

    def _is_postgres_source(self, source: Source | SourceCreate | SourceUpdate | SourceConnectionTest) -> bool:
        return self._source_type(source) == "POSTGRES"

    def _require_postgres_source(self, source: Source) -> None:
        if not self._is_postgres_source(source):
            raise ValueError("This operation is only available for POSTGRES sources")

    @staticmethod
    def _system_kafka_group_id(source_id: int) -> str:
        return f"rosetta-kafka-source-{source_id}"

    def _ensure_kafka_group_id(self, source: Source) -> None:
        if self._is_postgres_source(source):
            return
        config = dict(source.config or {})
        if str(config.get("group_id") or "").strip():
            return
        config["group_id"] = self._system_kafka_group_id(source.id)
        source.config = config
        self.db.add(source)

    def _encrypt_source_config(self, source_type: str, config: dict[str, Any]) -> dict[str, Any]:
        config = dict(config or {})
        if source_type == "POSTGRES" and config.get("password"):
            config["password"] = encrypt_value(config["password"])
        if source_type == "KAFKA" and config.get("sasl_password"):
            config["sasl_password"] = encrypt_value(config["sasl_password"])
        return config

    def _flatten_source_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        source_type = str(payload.get("type") or "POSTGRES").upper()
        config = self._encrypt_source_config(source_type, payload.get("config") or {})
        payload["type"] = source_type
        payload["config"] = config

        if source_type == "POSTGRES":
            payload.update(
                pg_host=config.get("host"),
                pg_port=config.get("port", 5432),
                pg_database=config.get("database"),
                pg_username=config.get("username"),
                pg_password=config.get("password"),
                publication_name=config.get("publication_name"),
                replication_name=config.get("replication_name"),
            )
        else:
            payload.update(
                pg_host=None,
                pg_port=None,
                pg_database=None,
                pg_username=None,
                pg_password=None,
                publication_name=None,
                replication_name=None,
                is_publication_enabled=False,
                is_replication_enabled=False,
                last_check_replication_publication=None,
            )

        return payload

    def _build_kafka_admin_client_config(self, source: Source) -> dict[str, Any]:
        config = dict(source.config or {})
        client = {
            "bootstrap.servers": config.get("bootstrap_servers", ""),
        }
        if config.get("security_protocol"):
            client["security.protocol"] = config["security_protocol"]
        if config.get("sasl_mechanism"):
            client["sasl.mechanism"] = config["sasl_mechanism"]
        if config.get("sasl_username"):
            client["sasl.username"] = config["sasl_username"]
        if config.get("sasl_password"):
            client["sasl.password"] = decrypt_value(config["sasl_password"])
        if config.get("ssl_ca_location"):
            client["ssl.ca.location"] = config["ssl_ca_location"]
        if config.get("ssl_certificate_location"):
            client["ssl.certificate.location"] = config["ssl_certificate_location"]
        if config.get("ssl_key_location"):
            client["ssl.key.location"] = config["ssl_key_location"]
        return client

    def _discover_kafka_tables(self, source: Source) -> list[str]:
        if self._is_postgres_source(source):
            raise ValueError("Kafka topic discovery is only available for KAFKA sources")

        config = dict(source.config or {})
        bootstrap_servers = config.get("bootstrap_servers", "")
        admin = create_admin_client(self._build_kafka_admin_client_config(source))
        try:
            metadata = admin.list_topics(timeout=10)
        except Exception as exc:
            raise ValueError(
                "Failed to fetch Kafka metadata from "
                f"'{bootstrap_servers}'. Ensure the broker hostname is reachable "
                "from the backend service. If the bootstrap server is reachable "
                "but metadata still times out, verify Kafka advertised.listeners "
                "is not pointing to an internal hostname such as 'kafka:9092'. "
                f"Original error: {exc}"
            ) from exc
        prefix = config.get("topic_prefix", "")
        prefix_with_dot = f"{prefix}." if prefix else ""
        tables = []
        for topic in metadata.topics.keys():
            if topic.startswith("_"):
                continue
            if prefix_with_dot and topic.startswith(prefix_with_dot):
                tables.append(topic.removeprefix(prefix_with_dot))
            elif prefix and topic == prefix:
                continue
        return sorted(set(tables))

    def _sync_kafka_tables(self, source: Source) -> list[str]:
        tables = self._discover_kafka_tables(source)
        table_repo = TableMetadataRepository(self.db)
        existing = {table.table_name: table for table in table_repo.get_by_source_id(source.id)}

        for table_name in tables:
            if table_name not in existing:
                table_repo.create(source_id=source.id, table_name=table_name, schema_table={})

        for table_name, table in existing.items():
            if table_name not in tables:
                self.db.delete(table)

        source.total_tables = len(tables)
        self.db.flush()
        return tables

    def _get_cached_kafka_tables(self, source_id: int) -> set[str]:
        table_repo = TableMetadataRepository(self.db)
        return {
            table.table_name
            for table in table_repo.get_by_source_id(source_id)
            if table.table_name
        }

    def create_source(self, source_data: SourceCreate) -> Source:
        """
        Create a new source.

        Args:
            source_data: Source creation data

        Returns:
            Created source
        """
        logger.info("Creating new source", extra={"name": source_data.name})

        payload = self._flatten_source_payload(source_data.model_dump())
        source = self.repository.create(**payload)

        try:
            if self._is_postgres_source(source):
                self._update_source_table_list(source)
                self.db.commit()
                self.db.refresh(source)

                try:
                    logger.info(
                        "Initializing WAL monitor status for new source",
                        extra={"source_id": source.id, "name": source.name},
                    )
                    wal_monitor_service = WALMonitorService()
                    wal_monitor_service.monitor_source_sync(source, self.db)
                    logger.info(
                        "WAL monitor status initialized successfully",
                        extra={"source_id": source.id},
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to initialize WAL monitor status",
                        extra={"source_id": source.id, "error": str(e)},
                    )
            else:
                self._ensure_kafka_group_id(source)
                self._sync_kafka_tables(source)
                self.db.commit()
                self.db.refresh(source)
        except Exception as e:
            logger.error("Failed to initialize source metadata: %s", e)

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
        update_data = source_data.model_dump(exclude_unset=True)
        source = self.repository.get_by_id(source_id)

        if "config" in update_data and update_data["config"] is not None:
            merged_config = dict(source.config or {})
            merged_config.update(update_data["config"])

            source_type = str(update_data.get("type") or source.type or "POSTGRES").upper()
            if source_type == "POSTGRES":
                current_password = merged_config.get("password")
                if not current_password and source.pg_password:
                    current_password = decrypt_value(source.pg_password)
                    merged_config["password"] = current_password
            elif source_type == "KAFKA":
                current_password = merged_config.get("sasl_password")
                if not current_password and source.config.get("sasl_password"):
                    current_password = decrypt_value(source.config["sasl_password"])
                    merged_config["sasl_password"] = current_password

            update_data["config"] = merged_config

        if "type" not in update_data:
            update_data["type"] = source.type

        update_data = self._flatten_source_payload(update_data)
        source = self.repository.update(source_id, **update_data)

        try:
            if self._is_postgres_source(source):
                self._update_source_table_list(source)
            else:
                self._ensure_kafka_group_id(source)
                self._sync_kafka_tables(source)
            self.db.commit()
            self.db.refresh(source)
        except Exception as e:
            logger.error("Failed to refresh source metadata: %s", e)

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
        try:
            if self._source_type(config) == "POSTGRES":
                logger.info(
                    "Testing PostgreSQL source connection",
                    extra={
                        "host": config.config.get("host"),
                        "port": config.config.get("port"),
                        "db": config.config.get("database"),
                    },
                )
                conn = psycopg2.connect(
                    host=config.config.get("host"),
                    port=config.config.get("port"),
                    dbname=config.config.get("database"),
                    user=config.config.get("username"),
                    password=config.config.get("password"),
                    connect_timeout=5,
                )
                conn.close()
                return True

            logger.info(
                "Testing Kafka source connection",
                extra={"bootstrap_servers": config.config.get("bootstrap_servers")},
            )
            admin = create_admin_client(
                {
                    "bootstrap.servers": config.config.get("bootstrap_servers"),
                    **(
                        {"security.protocol": config.config["security_protocol"]}
                        if config.config.get("security_protocol")
                        else {}
                    ),
                    **(
                        {"sasl.mechanism": config.config["sasl_mechanism"]}
                        if config.config.get("sasl_mechanism")
                        else {}
                    ),
                    **(
                        {"sasl.username": config.config["sasl_username"]}
                        if config.config.get("sasl_username")
                        else {}
                    ),
                    **(
                        {"sasl.password": config.config["sasl_password"]}
                        if config.config.get("sasl_password")
                        else {}
                    ),
                }
            )
            admin.list_topics(timeout=10)
            return True
        except ImportError:
            logger.error("Required source client dependency is not installed")
            return False
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

        config_payload = dict(source.config or {})
        if self._is_postgres_source(source) and source.pg_password:
            config_payload["password"] = decrypt_value(source.pg_password)
        if not self._is_postgres_source(source) and source.config.get("sasl_password"):
            config_payload["sasl_password"] = decrypt_value(source.config["sasl_password"])

        config = SourceConnectionTest(type=source.type, config=config_payload)

        return self.test_connection_config(config)

    def get_source_details(
        self, source_id: int, force_refresh: bool = False
    ) -> SourceDetailResponse:
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
                from app.infrastructure.redis import RedisClient
                import json

                cache_key = f"source_details:{source_id}"
                redis_client = RedisClient.get_instance()
                cached = redis_client.get(cache_key)

                if cached:
                    logger.info("Cache HIT for source details %s", source_id)
                    cached_data = json.loads(cached)
                    return SourceDetailResponse(**cached_data)
            except Exception as e:
                logger.warning("Cache read error for source %s: %s", source_id, e)

        # 1. Get Source
        source = self.get_source(source_id)

        runtime: dict[str, Any] = {"type": source.type}
        wal_monitor = None
        if self._is_postgres_source(source):
            if force_refresh:
                self._update_source_table_list(source)
                registered_tables = self._sync_publication_tables(source)
                self.db.add(source)
                self.db.commit()
                self.db.refresh(source)
            else:
                registered_tables = self._get_publication_tables(source)

            wal_monitor_repo = WALMonitorRepository(self.db)
            wal_monitor = wal_monitor_repo.get_by_source(source_id)
        else:
            kafka_metadata_error: str | None = None
            try:
                registered_tables = set(
                    self._sync_kafka_tables(source)
                    if force_refresh
                    else self.fetch_available_tables(source_id)
                )
            except ValueError as exc:
                kafka_metadata_error = str(exc)
                registered_tables = self._get_cached_kafka_tables(source.id)
                logger.warning(
                    "Using cached Kafka metadata for source %s after metadata fetch failed: %s",
                    source_id,
                    exc,
                )
            runtime.update(
                {
                    "bootstrap_servers": source.config.get("bootstrap_servers"),
                    "topic_prefix": source.config.get("topic_prefix"),
                    "topic_count": len(registered_tables),
                    "group_id": source.config.get("group_id")
                    or self._system_kafka_group_id(source.id),
                    "format": source.config.get("format", "PLAIN_JSON"),
                    "metadata_status": "stale" if kafka_metadata_error else "ready",
                    "metadata_error": kafka_metadata_error,
                }
            )

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
            runtime=runtime,
            tables=source_tables,
            destinations=destination_names,
        )

        # Cache the result for 30 seconds
        try:
            from app.infrastructure.redis import RedisClient
            import json

            cache_key = f"source_details:{source_id}"
            redis_client = RedisClient.get_instance()
            # Convert to dict for caching
            result_dict = result.dict()
            redis_client.setex(cache_key, 30, json.dumps(result_dict))
            logger.info("Cached source details for %s with 30s TTL", source_id)
        except Exception as e:
            logger.warning("Failed to cache source details for %s: %s", source_id, e)

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
                "Empty schema data for table %s version %s", table.table_name, version
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
        self._require_postgres_source(source)
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
        self._require_postgres_source(source)
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
            logger.error("Error fetching metadata for source %s: %s", source.name, e)
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
                p for p in pipelines if p.status in ["START", "REFRESH"]
            ]

            if not running_pipelines:
                logger.info("No running pipelines found for source %s", source_id)
                return

            logger.info(
                "Pausing %s running pipeline(s) for source %s",
                len(running_pipelines),
                source_id,
            )

            pipeline_service = PipelineService(self.db)
            for pipeline in running_pipelines:
                try:
                    pipeline_service.pause_pipeline(pipeline.id)
                    logger.info(
                        "Successfully paused pipeline %s (%s)",
                        pipeline.id,
                        pipeline.name,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to pause pipeline %s (%s): %s",
                        pipeline.id,
                        pipeline.name,
                        e,
                    )
                    # Continue pausing other pipelines even if one fails
                    continue

        except Exception as e:
            logger.error("Error pausing pipelines for source %s: %s", source_id, e)
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
                                "Skipping table %s: No schema columns found. Table may be empty or inaccessible.",
                                table_name,
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
                                "Added table %s with schema (%s columns)",
                                table_name,
                                len(schema_list),
                            )
                        except Exception as e:
                            # Likely IntegrityError if race condition
                            logger.warning("Skipping creation of %s: %s", table_name, e)
                            self.db.rollback()

                # 4. Fix existing tables without schemas
                for table in existing_tables:
                    # Only process tables still in publication
                    if table.table_name not in registered_tables:
                        continue

                    # Check if schema is missing or empty
                    if not table.schema_table or table.schema_table == {}:
                        logger.info(
                            "Found existing table %s without schema, fetching now...",
                            table.table_name,
                        )
                        try:
                            schema_list = monitor.fetch_table_schema(
                                conn_for_schema, table.table_name
                            )

                            if not schema_list:
                                logger.warning(
                                    "Could not fetch schema for %s. Table may be empty or inaccessible.",
                                    table.table_name,
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
                                    "Fixed table %s: Added schema and history (%s columns)",
                                    table.table_name,
                                    len(schema_list),
                                )
                            else:
                                # Update existing INITIAL_LOAD with correct schema
                                existing_history.schema_table_new = schema_dict
                                self.db.commit()
                                logger.info(
                                    "Fixed table %s: Updated schema (%s columns)",
                                    table.table_name,
                                    len(schema_list),
                                )
                        except Exception as e:
                            logger.error(
                                "Failed to fetch schema for existing table %s: %s",
                                table.table_name,
                                e,
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
                "Error syncing publication tables for source %s: %s", source.name, e
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
                "Failed to fetch publication tables for source %s: %s", source.name, e
            )
            # Fallback: return tables from local metadata
            table_repo = TableMetadataRepository(self.db)
            existing_tables = table_repo.get_by_source_id(source.id)
            return {t.table_name for t in existing_tables}

    def refresh_source_metadata(self, source_id: int) -> None:
        """Manually refresh source metadata."""
        source = self.get_source(source_id)

        if not self._is_postgres_source(source):
            try:
                self._sync_kafka_tables(source)
                self.db.commit()
                self.db.refresh(source)
            except ValueError:
                self.db.rollback()
                raise
            except Exception as e:
                self.db.rollback()
                logger.error(
                    "Failed to refresh Kafka source metadata for source %s: %s",
                    source_id,
                    e,
                )
                raise ValueError(f"Failed to refresh Kafka source metadata: {e}") from e
            try:
                redis_client = RedisClient.get_instance()
                redis_client.delete(f"source:{source_id}:tables")
            except Exception as e:
                logger.warning("Failed to invalidate cache for source %s: %s", source_id, e)
            return

        # Store previous state to detect external drops
        previous_publication_enabled = source.is_publication_enabled
        previous_replication_enabled = source.is_replication_enabled

        self._update_source_table_list(source)
        self._sync_publication_tables(source)
        self.db.commit()
        self.db.refresh(source)

        # Check if publication or replication was dropped externally
        if (previous_publication_enabled and not source.is_publication_enabled) or (
            previous_replication_enabled and not source.is_replication_enabled
        ):
            logger.warning(
                "Publication or replication slot dropped externally for source %s. Auto-pausing running pipelines.",
                source_id,
            )
            self._pause_running_pipelines_for_source(source_id)

        # Invalidate Available Tables Cache
        try:
            redis_client = RedisClient.get_instance()
            redis_client.delete(f"source:{source_id}:tables")
        except Exception as e:
            logger.warning("Failed to invalidate cache for source %s: %s", source_id, e)

    def create_publication(self, source_id: int, tables: List[str]) -> None:
        source = self.get_source(source_id)
        self._require_postgres_source(source)
        if not tables:
            raise ValueError("At least one table must be selected")

        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                tables_str = ", ".join([f'"{t}"' for t in tables])
                query = f"CREATE PUBLICATION {source.publication_name} FOR TABLE {tables_str} WITH (publish = 'insert, update, delete');"
                logger.info("Executing: %s", query)
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error("Failed to create publication: %s", e)
            raise ValueError(f"Failed to create publication: {str(e)}")

    def drop_publication(self, source_id: int) -> None:
        source = self.get_source(source_id)
        self._require_postgres_source(source)
        try:
            # Pause running pipelines first
            logger.info(
                "Auto-pausing running pipelines for source %s before dropping publication",
                source_id,
            )
            self._pause_running_pipelines_for_source(source_id)

            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f"DROP PUBLICATION IF EXISTS {source.publication_name};"
                logger.info("Executing: %s", query)
                cur.execute(query)
            conn.close()

            # Cleanup pipeline table sync configurations for this source
            # This will CASCADE delete associated tags via ondelete="CASCADE"
            from app.domain.models.pipeline import (
                Pipeline,
                PipelineDestination,
                PipelineDestinationTableSync,
            )
            from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList

            logger.info(
                "Cleaning up pipeline table sync configurations for source %s",
                source_id,
            )
            pipelines = (
                self.db.query(Pipeline).filter(Pipeline.source_id == source_id).all()
            )

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
                            PipelineDestinationTableSync.id
                            == PipelineDestinationTableSyncTag.pipelines_destination_table_sync_id,
                        )
                        .filter(
                            PipelineDestinationTableSync.pipeline_destination_id.in_(
                                pipeline_dest_ids
                            )
                        )
                        .distinct()
                        .all()
                    )
                    all_tag_ids.update([tag_id[0] for tag_id in tag_ids])

                    # Delete all table sync configurations for these destinations
                    # CASCADE will automatically delete associated tag associations
                    deleted_count = (
                        self.db.query(PipelineDestinationTableSync)
                        .filter(
                            PipelineDestinationTableSync.pipeline_destination_id.in_(
                                pipeline_dest_ids
                            )
                        )
                        .delete(synchronize_session=False)
                    )
                    logger.info(
                        "Deleted %s table sync configurations for pipeline %s",
                        deleted_count,
                        pipeline.id,
                    )

            self.db.commit()

            # Cleanup unused tags after deletion
            if all_tag_ids:
                logger.info("Checking %s tags for cleanup", len(all_tag_ids))
                for tag_id in all_tag_ids:
                    # Check if tag is still used
                    count = (
                        self.db.query(PipelineDestinationTableSyncTag)
                        .filter(PipelineDestinationTableSyncTag.tag_id == tag_id)
                        .count()
                    )

                    if count == 0:
                        # Tag is unused, delete it
                        tag = (
                            self.db.query(TagList).filter(TagList.id == tag_id).first()
                        )
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
            logger.error("Failed to drop publication: %s", e)
            self.db.rollback()
            raise ValueError(f"Failed to drop publication: {str(e)}")

    def create_replication_slot(self, source_id: int) -> None:
        source = self.get_source(source_id)
        self._require_postgres_source(source)
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                slot_name = source.replication_name
                # Check if exists first to avoid error? Or just try create
                # The user asked for specific query
                query = f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput');"
                logger.info("Executing: %s", query)
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error("Failed to create replication slot: %s", e)
            raise ValueError(f"Failed to create replication slot: {str(e)}")

    def drop_replication_slot(self, source_id: int) -> None:
        source = self.get_source(source_id)
        self._require_postgres_source(source)
        try:
            # Pause running pipelines first
            logger.info(
                "Auto-pausing running pipelines for source %s before dropping replication slot",
                source_id,
            )
            self._pause_running_pipelines_for_source(source_id)

            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                slot_name = source.replication_name
                query = f"SELECT pg_drop_replication_slot('{slot_name}');"
                logger.info("Executing: %s", query)
                cur.execute(query)
            conn.close()
            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error("Failed to drop replication slot: %s", e)
            raise ValueError(f"Failed to drop replication slot: {str(e)}")

    def register_table_to_publication(self, source_id: int, table_name: str) -> None:
        """
        Register a table to the creation publication.
        """
        source = self.get_source(source_id)
        if not self._is_postgres_source(source):
            table_repo = TableMetadataRepository(self.db)
            existing = table_repo.get_by_source_and_name(source_id, table_name)
            if not existing:
                table_repo.create(source_id=source_id, table_name=table_name, schema_table={})
            source.total_tables = len(table_repo.get_by_source_id(source_id))
            self.db.commit()
            return
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f'ALTER PUBLICATION {source.publication_name} ADD TABLE "{table_name}"'
                logger.info("Executing: %s", query)
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
                        "Triggering auto-provisioning for table %s on %s pipelines",
                        table_name,
                        len(pipelines),
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
                                            "Failed to auto-provision table %s for pipeline %s destination %s: %s",
                                            table_name,
                                            pipeline.id,
                                            pd.destination.name,
                                            exc,
                                        )

                        # Commit the ready_refresh changes
                        self.db.commit()
                        logger.info(
                            "Marked %s pipeline(s) as ready for refresh", len(pipelines)
                        )
                    else:
                        logger.warning(
                            "Metadata for table %s not found after refresh, skipping provisioning",
                            table_name,
                        )

            except Exception as e:
                logger.error("Auto-provisioning process failed: %s", e)
                # Don't raise here to avoid failing the registration itself if provisioning fails

        except Exception as e:
            logger.error("Failed to register table %s: %s", table_name, e)
            raise ValueError(f"Failed to register table: {str(e)}")

    def unregister_table_from_publication(
        self, source_id: int, table_name: str
    ) -> None:
        """
        Unregister (drop) a table from the publication.
        """
        source = self.get_source(source_id)
        if not self._is_postgres_source(source):
            table_repo = TableMetadataRepository(self.db)
            table_repo.delete_table(source_id, table_name)
            source.total_tables = len(table_repo.get_by_source_id(source_id))
            self.db.commit()
            return
        try:
            conn = self._get_connection(source)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                query = f'ALTER PUBLICATION {source.publication_name} DROP TABLE "{table_name}"'
                logger.info("Executing: %s", query)
                cur.execute(query)
            conn.close()

            # Cleanup Metadata for this table
            table_repo = TableMetadataRepository(self.db)
            table_repo.delete_table(source_id, table_name)

            self.refresh_source_metadata(source_id)
        except Exception as e:
            logger.error("Failed to unregister table %s: %s", table_name, e)
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
            logger.warning("Redis cache error: %s", e)

        if not self._is_postgres_source(source):
            try:
                tables = self._discover_kafka_tables(source)
                try:
                    import json

                    redis_client = RedisClient.get_instance()
                    redis_client.setex(cache_key, 300, json.dumps(tables))
                except Exception as e:
                    logger.warning("Failed to cache tables for source %s: %s", source_id, e)
                return tables
            except Exception as e:
                logger.error(
                    "Failed to fetch available Kafka topics for source %s: %s", source.name, e
                )
                raise ValueError(f"Failed to fetch topics: {str(e)}")

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
                logger.warning("Failed to cache tables for source %s: %s", source_id, e)

            return tables
        except Exception as e:
            logger.error(
                "Failed to fetch available tables for source %s: %s", source.name, e
            )
            raise ValueError(f"Failed to fetch tables: {str(e)}")

    def refresh_available_tables(self, source_id: int) -> List[str]:
        """
        Force refresh available tables from source and update cache.
        """
        source = self.get_source(source_id)
        cache_key = f"source:{source_id}:tables"

        if not self._is_postgres_source(source):
            try:
                tables = self._sync_kafka_tables(source)
                try:
                    import json

                    redis_client = RedisClient.get_instance()
                    redis_client.setex(cache_key, 300, json.dumps(tables))
                except Exception as e:
                    logger.error(
                        "Failed to update cache during refresh for source %s: %s",
                        source_id,
                        e,
                    )
                self.db.commit()
                return tables
            except ValueError:
                self.db.rollback()
                raise
            except Exception as e:
                self.db.rollback()
                logger.error(
                    "Failed to refresh Kafka topics for source %s: %s", source.name, e
                )
                raise ValueError(f"Failed to refresh Kafka topics: {e}") from e

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
                    "Failed to update cache during refresh for source %s: %s",
                    source_id,
                    e,
                )

            return tables
        except Exception as e:
            logger.error(
                "Failed to refresh table list for source %s: %s", source.name, e
            )
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
            logger.warning("Redis cache error: %s", e)

        schema_data = {}

        if not self._is_postgres_source(source):
            tables = self.fetch_available_tables(source_id)
            if table_name:
                tables = [table for table in tables if table_name.lower() in table.lower()]
            for table in tables:
                schema_data[table] = []
            return schema_data

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
                logger.warning("Failed to cache source schema: %s", e)

            return schema_data

        except Exception as e:
            logger.error("Failed to fetch source schema: %s", e)
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
        base_rep_name = original_source.replication_name or "replication_slot"
        base_pub_name = original_source.publication_name or "publication"

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
                config = dict(original_source.config or {})
                if self._is_postgres_source(original_source):
                    config["password"] = (
                        decrypt_value(original_source.pg_password)
                        if original_source.pg_password
                        else None
                    )
                    config["publication_name"] = attempt_pub_name
                    config["replication_name"] = attempt_rep_name

                source_data = SourceCreate(
                    name=new_name,
                    type=original_source.type,
                    config=config,
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
