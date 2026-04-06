"""
Debezium engine wrapper for running CDC pipelines.

Provides high-level interface for creating and running Debezium engines.
"""

import logging
import os
import threading
from pathlib import Path
from typing import Any, Optional

from config.config import get_config
from core.models import Pipeline, DestinationType
from core.record_router import RecordRouter
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
from sources.postgresql import PostgreSQLSource
from sources.postgres_runner import PostgresSourceRunner
from sources.kafka_runner import KafkaSourceRunner
from destinations.base import BaseDestination
from destinations.snowflake import SnowflakeDestination
from destinations.postgresql import PostgreSQLDestination
from destinations.kafka import KafkaDestination

logger = logging.getLogger(__name__)


def _ensure_jvm_started() -> None:
    """
    Pre-start the JVM with the correct classpath before pydbzengine's lazy init.

    pydbzengine/_jvm.py starts the JVM on first access of DebeziumJsonEngine.run(),
    but it relies on JPype internally appending org.jpype.jar to the classpath.
    On some Windows + JDK combinations this internal append does not propagate to
    System.getProperty("java.class.path"), causing JPypeContext.java to throw:
      "Can't find org.jpype.jar support library"

    The fix: start the JVM ourselves with org.jpype.jar explicitly in CLASS_PATHS.
    pydbzengine/_jvm.py checks `if not jpype.isJVMStarted()` and skips its own
    startJVM call when the JVM is already running.
    """
    try:
        import jpype
        import importlib.util

        if jpype.isJVMStarted():
            return

        # Locate org.jpype.jar relative to the jpype package directory
        jpype_pkg_dir = Path(jpype.__file__).parent
        org_jpype_jar = jpype_pkg_dir.parent / "org.jpype.jar"
        if not org_jpype_jar.exists():
            logger.warning(
                "org.jpype.jar not found at %s — letting pydbzengine handle JVM startup",
                org_jpype_jar,
            )
            return

        # Locate Debezium JARs from pydbzengine's bundled libs
        spec = importlib.util.find_spec("pydbzengine")
        if spec is None or spec.origin is None:
            logger.warning(
                "pydbzengine not found — letting default JVM startup proceed"
            )
            return

        dbz_libs_dir = Path(spec.origin).parent / "debezium" / "libs"
        class_paths = [str(j) for j in dbz_libs_dir.glob("*.jar")]

        # Append pydbzengine config dir
        dbz_conf_dir = Path(spec.origin).parent / "config"
        class_paths.append(dbz_conf_dir.as_posix())

        # Also append compute/config if present (mirrors pydbzengine/_jvm.py behaviour)
        local_conf = Path.cwd() / "config"
        if local_conf.is_dir():
            class_paths.append(local_conf.as_posix())

        # Explicitly include org.jpype.jar so it appears in -Djava.class.path
        class_paths.append(str(org_jpype_jar))

        jvm_path = jpype.getDefaultJVMPath()
        if isinstance(jvm_path, bytes):
            jvm_path = jvm_path.decode("utf-8")

        # C3/R3: Add configurable JVM heap size to prevent unbounded memory growth
        jvm_max_heap = os.getenv("JVM_MAX_HEAP", "16G")
        jvm_args = [f"-Xmx{jvm_max_heap}"]

        logger.debug(
            "Pre-starting JVM with explicit org.jpype.jar on classpath "
            "(heap max=%s)",
            jvm_max_heap,
        )
        jpype.startJVM(jvm_path, *jvm_args, classpath=class_paths)
        logger.debug("JVM pre-started successfully (heap max=%s)", jvm_max_heap)

    except Exception as exc:
        logger.warning(
            "JVM pre-start failed (%s) — falling back to pydbzengine default startup",
            exc,
        )


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
        self._source = None
        self._destinations: dict[int, BaseDestination] = {}
        self._source_runner = None
        self._logger = logging.getLogger(f"{__name__}.Pipeline_{pipeline_id}")
        self._is_running = False

        # C4/R5: Shutdown synchronization — event handler checks this before writing
        self._shutdown_event = threading.Event()

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

    def _create_source(self, pipeline: Pipeline):
        """Create the typed source helper used by the runner."""
        if not pipeline.source:
            raise PipelineException("Pipeline source is not loaded")
        if pipeline.source.is_postgres:
            return PostgreSQLSource(pipeline.source)
        if pipeline.source.is_kafka:
            return pipeline.source
        raise PipelineException(
            f"Unsupported source type: {pipeline.source.source_type}",
            {"source_type": pipeline.source.source_type},
        )

    def _create_source_runner(self):
        """Create the runtime runner for the configured source."""
        if self._pipeline.source.is_postgres:
            offset_file = get_config().debezium.get_offset_file(self._pipeline.name)
            return PostgresSourceRunner(
                source=self._source,
                offset_file=offset_file,
                shutdown_event=self._shutdown_event,
            )
        if self._pipeline.source.is_kafka:
            return KafkaSourceRunner(self._pipeline.source)
        raise PipelineException(
            f"Unsupported source type: {self._pipeline.source.source_type}",
            {"source_type": self._pipeline.source.source_type},
        )

    def _create_destination(
        self, destination_type: str, config: Any, source_config: Optional[Any] = None
    ) -> BaseDestination:
        """
        Create destination instance based on type.

        Args:
            destination_type: Type of destination (SNOWFLAKE, POSTGRES, KAFKA)
            config: Destination configuration model
            source_config: Optional source configuration (for PostgreSQL destinations)

        Returns:
            BaseDestination instance
        """
        destination_type = (
            destination_type.strip() if destination_type else destination_type
        )
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
        elif destination_type.upper() == DestinationType.KAFKA.value:
            return KafkaDestination(config)
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
        self._source_runner = self._create_source_runner()

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

        self._source_runner.validate(self._pipeline.name, table_list)

        if self._pipeline.source and self._pipeline.source.is_postgres:
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
            except Exception as e:
                self._logger.warning(
                    f"Schema validation skipped due to error: {e}",
                    exc_info=True,
                )

        if self._pipeline.source and self._pipeline.source.is_postgres:
            offset_file = config.debezium.get_offset_file(self._pipeline.name)
            self._clean_offset_on_start(offset_file)

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
            if self._pipeline.source and self._pipeline.source.is_postgres:
                _ensure_jvm_started()

            router = RecordRouter(
                pipeline=self._pipeline,
                destinations=self._destinations,
                dlq_manager=self._dlq_manager,
                shutdown_event=self._shutdown_event,
            )
            self._source_runner.run(
                pipeline_name=self._pipeline.name,
                table_include_list=table_list,
                router=router,
                stop_event=self._shutdown_event,
            )
        except Exception as e:
            self._logger.error(
                f"Pipeline {self._pipeline.name} failed: {sanitize_for_log(e)}"
            )
            db_error_msg = sanitize_for_db(e, self._pipeline.name, "PIPELINE")
            PipelineMetadataRepository.upsert(self._pipeline_id, "ERROR", db_error_msg)
            raise
        finally:
            self._is_running = False

    def stop(self, set_status: bool = True) -> None:
        """Stop the pipeline engine.

        Args:
            set_status: When True (default) write PAUSED status to the DB.
                        Pass False when the caller has already written an ERROR
                        status so this method does not overwrite it. (BUG-8)
        """
        self._is_running = False

        # C4/R5: Signal event handler to stop writing BEFORE closing destinations.
        # This prevents the race condition where event handler tries to write
        # to a destination that has already been closed.
        self._shutdown_event.set()

        if self._source_runner:
            try:
                self._source_runner.stop()
            except Exception as exc:
                self._logger.warning(f"Failed to stop source runner cleanly: {exc}")

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

        # Update metadata — only if caller has not already set an error status
        if set_status and self._pipeline:
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
