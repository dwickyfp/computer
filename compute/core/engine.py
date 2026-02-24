"""
Debezium engine wrapper for running CDC pipelines.

Provides high-level interface for creating and running Debezium engines.
"""

import logging
from typing import Any, Optional

from pydbzengine import DebeziumJsonEngine

from config.config import get_config
from core.models import Pipeline, DestinationType
from core.event_handler import CDCEventHandler
from core.repository import (
    PipelineRepository,
    TableMetadataRepository,
    PipelineMetadataRepository,
)
from core.exceptions import PipelineException
from core.error_sanitizer import sanitize_for_db, sanitize_for_log
from core.dlq_manager import DLQManager
from core.dlq_recovery import DLQRecoveryWorker
from core.schema_validator import validate_pipeline_schemas
from sources.base import BaseSource
from sources.postgresql import PostgreSQLSource
from destinations.base import BaseDestination
from destinations.snowflake import SnowflakeDestination
from destinations.postgresql import PostgreSQLDestination

logger = logging.getLogger(__name__)


class PipelineEngine:
    """
    Debezium pipeline engine for running CDC pipelines.

    Manages the lifecycle of a single pipeline including:
    - Loading configuration from database
    - Creating source and destination instances
    - Running Debezium engine
    - Handling status updates
    """

    def __init__(self, pipeline_id: int):
        """
        Initialize pipeline engine.

        Args:
            pipeline_id: ID of pipeline to run
        """
        self._pipeline_id = pipeline_id
        self._pipeline: Optional[Pipeline] = None
        self._source: Optional[BaseSource] = None
        self._destinations: dict[int, BaseDestination] = {}
        self._engine: Optional[DebeziumJsonEngine] = None
        self._logger = logging.getLogger(f"{__name__}.Pipeline_{pipeline_id}")
        self._is_running = False

        # DLQ components
        self._dlq_manager: Optional[DLQManager] = None
        self._dlq_recovery_worker: Optional[DLQRecoveryWorker] = None

    def _load_pipeline(self) -> Pipeline:
        """Load pipeline configuration from database."""
        pipeline = PipelineRepository.get_by_id(
            self._pipeline_id, include_relations=True
        )

        if pipeline is None:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} not found",
                {"pipeline_id": self._pipeline_id},
            )

        if pipeline.source is None:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} has no source configured",
                {"pipeline_id": self._pipeline_id},
            )

        if not pipeline.destinations:
            raise PipelineException(
                f"Pipeline {self._pipeline_id} has no destinations configured",
                {"pipeline_id": self._pipeline_id},
            )

        return pipeline

    def _create_source(self, pipeline: Pipeline) -> BaseSource:
        """
        Create source instance based on configuration.

        Currently only PostgreSQL is supported.
        """
        # For now, all sources are PostgreSQL
        return PostgreSQLSource(pipeline.source)

    def _create_destination(
        self, destination_type: str, config: Any, source_config: Optional[Any] = None
    ) -> BaseDestination:
        """
        Create destination instance based on type.

        Args:
            destination_type: Type of destination (SNOWFLAKE, POSTGRES)
            config: Destination configuration model
            source_config: Optional source configuration (for PostgreSQL destinations)

        Returns:
            BaseDestination instance
        """
        if destination_type.upper() == DestinationType.SNOWFLAKE.value:
            # Get Snowflake timeout config from global config
            cfg = get_config()
            timeout_config = {
                "connect_timeout": cfg.snowflake.connect_timeout,
                "read_timeout": cfg.snowflake.read_timeout,
                "write_timeout": cfg.snowflake.write_timeout,
                "pool_timeout": cfg.snowflake.pool_timeout,
                "batch_timeout_base": cfg.snowflake.batch_timeout_base,
                "batch_timeout_max": cfg.snowflake.batch_timeout_max,
            }
            return SnowflakeDestination(config, timeout_config=timeout_config)
        elif destination_type.upper() == DestinationType.POSTGRES.value:
            return PostgreSQLDestination(config, source_config=source_config)
        else:
            raise PipelineException(
                f"Unsupported destination type: {destination_type}",
                {"destination_type": destination_type},
            )

    def _get_table_include_list(self, source_id: int) -> list[str]:
        """
        Get list of tables to include in CDC.

        Tables are loaded from table_metadata_list for this source.

        Args:
            source_id: Source ID

        Returns:
            List of table names
        """
        tables = TableMetadataRepository.get_table_names_for_source(source_id)

        if not tables:
            self._logger.warning(
                f"No tables found in table_metadata_list for source {source_id}"
            )

        return tables

    def initialize(self) -> None:
        """
        Initialize pipeline engine.

        Loads configuration and creates source/destination instances.
        Each destination is initialized independently - if one fails during init,
        others can still be used. If all fail, pipeline still runs and uses DLQ.
        """
        self._pipeline = self._load_pipeline()
        self._source = self._create_source(self._pipeline)

        # Create and initialize destination instances independently
        successful_destinations = 0
        failed_destinations = 0

        for pd in self._pipeline.destinations:
            if not pd.destination:
                self._logger.warning(
                    f"Pipeline destination {pd.id} has no destination config, skipping"
                )
                continue

            try:
                dest = self._create_destination(
                    pd.destination.type,
                    pd.destination,
                    source_config=self._pipeline.source,
                )

                # Try to initialize, but keep destination object even if it fails
                try:
                    dest.initialize()
                    successful_destinations += 1

                    # Clear any previous initialization errors
                    from core.repository import PipelineDestinationRepository

                    if pd.is_error:
                        PipelineDestinationRepository.update_error(pd.id, False)
                        self._logger.info(
                            f"Cleared error state for destination {pd.destination.name}"
                        )

                except Exception as init_error:
                    # Log initialization error but keep destination object for DLQ/recovery
                    log_msg = f"Failed to initialize destination {pd.destination.name}: {sanitize_for_log(init_error)}"
                    self._logger.warning(log_msg, exc_info=True)
                    failed_destinations += 1

                    # Update error state in database with sanitized message
                    from core.repository import PipelineDestinationRepository

                    db_error_msg = sanitize_for_db(
                        init_error, pd.destination.name, pd.destination.type
                    )
                    PipelineDestinationRepository.update_error(
                        pd.id, True, db_error_msg
                    )

                # Add destination to registry regardless of initialization status
                # This allows DLQ recovery worker to track and retry connection
                self._destinations[pd.destination_id] = dest

            except Exception as e:
                # Failed to even create destination object
                log_msg = f"Failed to create destination {pd.destination.name}: {sanitize_for_log(e)}"
                self._logger.error(log_msg, exc_info=True)
                failed_destinations += 1

                # Update error state in database with sanitized message
                from core.repository import PipelineDestinationRepository

                db_error_msg = sanitize_for_db(
                    e, pd.destination.name, pd.destination.type
                )
                PipelineDestinationRepository.update_error(pd.id, True, db_error_msg)

        # Log status but don't fail if no destinations initialized
        # Pipeline will use DLQ for all writes until destinations recover
        if successful_destinations == 0:
            self._logger.warning(
                f"Pipeline {self._pipeline.name} starting with NO working destinations. "
                f"All {failed_destinations} destination(s) failed to initialize. "
                f"CDC events will be stored in DLQ until destinations recover."
            )
        else:
            self._logger.info(
                f"Pipeline {self._pipeline.name} initialized: "
                f"{successful_destinations} destination(s) ready, "
                f"{failed_destinations} destination(s) failed"
            )

        # Initialize DLQ manager
        config = get_config()
        self._dlq_manager = DLQManager(
            redis_url=config.dlq.redis_url,
            key_prefix=config.dlq.key_prefix,
            max_stream_length=config.dlq.max_stream_length,
            consumer_group=config.dlq.consumer_group,
        )
        self._logger.info(f"DLQ manager initialized with Redis")

    def _clean_offset_on_start(self, offset_file: str) -> None:
        """
        Delete offset file before starting pipeline.

        This ensures the pipeline always starts fresh from the current replication
        slot position, preventing LSN mismatch errors when:
        - Replication slot was dropped and recreated
        - CHECKPOINT advanced WAL beyond stored offset
        - Pipeline is started after being paused

        The offset is cleared on every start (PAUSE->START or REFRESH->START).
        Debezium will use snapshot.mode=recovery to catch up from current position.
        """
        from pathlib import Path

        offset_path = Path(offset_file)

        # Delete offset file if it exists
        if offset_path.exists():
            try:
                offset_path.unlink()
                self._logger.info(f"Deleted offset file for fresh start: {offset_file}")
            except Exception as e:
                self._logger.warning(
                    f"Could not delete offset file: {e}. Debezium will proceed anyway."
                )
        else:
            self._logger.debug(f"No offset file to delete: {offset_file}")

    def run(self) -> None:
        """
        Run the pipeline engine.

        Starts Debezium engine and begins processing CDC events.
        """
        if self._pipeline is None:
            self.initialize()

        config = get_config()

        # Get table include list
        table_list = self._get_table_include_list(self._pipeline.source_id)

        if not table_list:
            raise PipelineException(
                "No tables configured for this pipeline",
                {"pipeline_id": self._pipeline_id},
            )

        # Validate replication setup before starting
        # This catches missing publication/replication slot issues early
        if hasattr(self._source, "validate_replication_setup"):
            is_valid, error_msg = self._source.validate_replication_setup(
                self._pipeline.name
            )
            if not is_valid:
                self._logger.error(f"Replication setup validation failed: {error_msg}")
                raise PipelineException(
                    f"Replication setup validation failed: {error_msg}",
                    {
                        "pipeline_id": self._pipeline_id,
                        "pipeline_name": self._pipeline.name,
                    },
                )

        # Validate schema compatibility between source and destinations (B2)
        # Only warns on issues; errors are logged but don't block pipeline start
        try:
            schema_result = validate_pipeline_schemas(self._pipeline, table_list)
            if schema_result.issues:
                for issue in schema_result.issues:
                    if issue.severity == "ERROR":
                        self._logger.warning(
                            f"Schema compatibility ERROR: {issue.message} "
                            f"(table={issue.table_name}, column={issue.column_name})"
                        )
                    else:
                        self._logger.info(
                            f"Schema compatibility WARNING: {issue.message}"
                        )

                error_count = sum(1 for i in schema_result.issues if i.severity == "ERROR")
                warn_count = sum(1 for i in schema_result.issues if i.severity == "WARNING")
                self._logger.info(
                    f"Schema validation: {schema_result.tables_checked} tables checked, "
                    f"{error_count} errors, {warn_count} warnings"
                )
        except Exception as e:
            self._logger.warning(
                f"Schema validation skipped due to error: {e}",
                exc_info=True,
            )

        # Build Debezium properties
        offset_file = config.debezium.get_offset_file(self._pipeline.name)

        # Delete offset file to ensure fresh start
        # This prevents LSN mismatch errors and ensures pipeline starts from current position
        self._clean_offset_on_start(offset_file)

        props = self._source.build_debezium_props(
            pipeline_name=self._pipeline.name,
            table_include_list=table_list,
            offset_file=offset_file,
        )

        # Create event handler with DLQ manager
        handler = CDCEventHandler(
            pipeline=self._pipeline,
            destinations=self._destinations,
            dlq_manager=self._dlq_manager,
        )

        # Start DLQ recovery worker
        if self._dlq_manager:
            config = get_config()
            check_interval = config.dlq.get("check_interval", 30)
            batch_size = config.dlq.get("batch_size", 100)

            self._dlq_recovery_worker = DLQRecoveryWorker(
                pipeline=self._pipeline,
                destinations=self._destinations,
                dlq_manager=self._dlq_manager,
                check_interval=check_interval,
                batch_size=batch_size,
                max_retry_count=config.dlq.max_retry_count,
                max_age_days=config.dlq.max_age_days,
            )
            self._dlq_recovery_worker.start()
            self._logger.info("DLQ recovery worker started")

        # Update metadata
        PipelineMetadataRepository.upsert(self._pipeline_id, "RUNNING")

        self._logger.info(f"Starting pipeline {self._pipeline.name}")
        self._is_running = True

        try:
            # Create and run Debezium engine
            self._engine = DebeziumJsonEngine(properties=props, handler=handler)
            self._engine.run()
        except Exception as e:
            self._logger.error(
                f"Pipeline {self._pipeline.name} failed: {sanitize_for_log(e)}"
            )
            db_error_msg = sanitize_for_db(e, self._pipeline.name, "PIPELINE")
            PipelineMetadataRepository.upsert(self._pipeline_id, "ERROR", db_error_msg)
            raise
        finally:
            self._is_running = False

    def stop(self) -> None:
        """Stop the pipeline engine."""
        self._is_running = False

        # CRITICAL: Stop all Python threads BEFORE stopping Debezium/JPype
        # This prevents GIL conflicts during JPype shutdown

        # Stop DLQ recovery worker FIRST
        if self._dlq_recovery_worker:
            try:
                self._dlq_recovery_worker.stop()
                self._logger.info("DLQ recovery worker stopped")
            except Exception as e:
                self._logger.warning(f"Error stopping DLQ recovery worker: {e}")
            self._dlq_recovery_worker = None

        # Close destinations (stops Snowflake async thread) SECOND
        for dest in self._destinations.values():
            try:
                dest.close()
            except Exception as e:
                self._logger.warning(f"Error closing destination: {e}")

        self._destinations.clear()

        # Close DLQ manager THIRD
        if self._dlq_manager:
            try:
                self._dlq_manager.close_all()
            except Exception as e:
                self._logger.warning(f"Error closing DLQ manager: {e}")
            self._dlq_manager = None

        # Update metadata
        if self._pipeline:
            PipelineMetadataRepository.upsert(self._pipeline_id, "PAUSED")

        self._logger.info(f"Pipeline {self._pipeline_id} stopped")

    @property
    def is_running(self) -> bool:
        """Check if pipeline is running."""
        return self._is_running


def run_pipeline(pipeline_id: int) -> None:
    """
    Convenience function to run a pipeline.

    Args:
        pipeline_id: ID of pipeline to run
    """
    engine = PipelineEngine(pipeline_id)
    engine.run()
