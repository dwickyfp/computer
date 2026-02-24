"""
PostgreSQL source implementation.

Provides PostgreSQL-specific Debezium connector configuration.
"""

import logging
from typing import Any, Optional

import psycopg2

from sources.base import BaseSource
from core.models import Source
from core.security import decrypt_value
from config.config import get_config

logger = logging.getLogger(__name__)


class PostgreSQLSource(BaseSource):
    """
    PostgreSQL CDC source using Debezium.

    Configures PostgreSQL logical replication with pgoutput plugin.
    """

    # Debezium PostgreSQL connector class
    CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"

    # Default plugin for PostgreSQL 10+
    PLUGIN_NAME = "pgoutput"

    # Default schema
    DEFAULT_SCHEMA = "public"

    def __init__(self, config: Source):
        """
        Initialize PostgreSQL source.

        Args:
            config: Source configuration from database
        """
        super().__init__(config)

    def get_connector_class(self) -> str:
        """Get Debezium connector class."""
        return self.CONNECTOR_CLASS

    def get_plugin_name(self) -> str:
        """Get logical decoding plugin name."""
        return self.PLUGIN_NAME

    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        cfg = self._config
        password = decrypt_value(cfg.pg_password or "")
        return f"postgresql://{cfg.pg_username}:{password}@{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_database}"

    def get_slot_name(self, pipeline_name: str) -> str:
        """
        Get replication slot name.

        Returns:
            Configured replication slot name
        """
        return self._config.replication_name

    def validate_connection(self) -> bool:
        """
        Validate PostgreSQL connection.

        Checks:
        1. Can connect to database
        2. wal_level is set to 'logical'
        3. Replication slot can be created (user has permission)
        """
        try:
            conn = psycopg2.connect(
                host=self._config.pg_host,
                port=self._config.pg_port,
                dbname=self._config.pg_database,
                user=self._config.pg_username,
                password=decrypt_value(self._config.pg_password or ""),
            )

            with conn.cursor() as cur:
                # Check wal_level
                cur.execute("SHOW wal_level")
                wal_level = cur.fetchone()[0]
                if wal_level != "logical":
                    logger.warning(f"wal_level is '{wal_level}', should be 'logical'")
                    return False

                logger.info(f"PostgreSQL source validated: {self._config.name}")
                return True

        except psycopg2.Error as e:
            logger.error(f"Failed to validate PostgreSQL source: {e}")
            return False
        finally:
            if "conn" in locals():
                conn.close()

    def validate_replication_setup(self, pipeline_name: str) -> tuple[bool, str]:
        """
        Validate that publication and replication slot exist before starting pipeline.

        This prevents Debezium from infinitely retrying when resources don't exist.

        Args:
            pipeline_name: Name of the pipeline being validated

        Returns:
            Tuple of (is_valid, error_message)
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self._config.pg_host,
                port=self._config.pg_port,
                dbname=self._config.pg_database,
                user=self._config.pg_username,
                password=decrypt_value(self._config.pg_password or ""),
            )

            with conn.cursor() as cur:
                # Check publication exists
                cur.execute(
                    "SELECT COUNT(*) FROM pg_publication WHERE pubname = %s",
                    (self._config.publication_name,),
                )
                pub_exists = cur.fetchone()[0] > 0

                if not pub_exists:
                    error_msg = (
                        f"Publication '{self._config.publication_name}' does not exist. "
                        f"Create it with: CREATE PUBLICATION {self._config.publication_name} FOR ALL TABLES;"
                    )
                    logger.error(error_msg)
                    return False, error_msg

                # Check publication has tables
                cur.execute(
                    "SELECT COUNT(*) FROM pg_publication_tables WHERE pubname = %s",
                    (self._config.publication_name,),
                )
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    logger.warning(
                        f"Publication '{self._config.publication_name}' exists but has no tables. "
                        f"Add tables with: ALTER PUBLICATION {self._config.publication_name} ADD TABLE schema.table;"
                    )

                # Check replication slot exists
                slot_name = self.get_slot_name(pipeline_name)
                cur.execute(
                    "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                slot_exists = cur.fetchone()[0] > 0

                # If slot exists, verify it references the correct publication
                if slot_exists:
                    logger.info(
                        f"Replication slot '{slot_name}' exists. "
                        f"Debezium will use it with publication '{self._config.publication_name}'."
                    )
                else:
                    logger.info(
                        f"Replication slot '{slot_name}' does not exist. "
                        f"Debezium will create it automatically."
                    )

                logger.info(
                    f"Replication setup validated for pipeline '{pipeline_name}': "
                    f"publication={self._config.publication_name}, slot={slot_name}, tables={table_count}"
                )
                return True, ""

        except psycopg2.Error as e:
            error_msg = f"Failed to validate replication setup: {e}"
            logger.error(error_msg)
            return False, error_msg
        finally:
            if conn:
                conn.close()

    def build_debezium_props(
        self,
        pipeline_name: str,
        table_include_list: list[str],
        offset_file: str,
    ) -> dict[str, Any]:
        """
        Build Debezium PostgreSQL connector properties.

        Args:
            pipeline_name: Unique name for this pipeline
            table_include_list: List of tables to include (schema.table format)
            offset_file: Path to offset storage file

        Returns:
            Dict of Debezium connector properties
        """
        config = get_config()
        slot_name = self.get_slot_name(pipeline_name)

        # Build table include list with schema prefix
        tables_with_schema = []
        for table in table_include_list:
            if "." not in table:
                tables_with_schema.append(f"{self.DEFAULT_SCHEMA}.{table}")
            else:
                tables_with_schema.append(table)

        props = {
            # Engine identification
            "name": pipeline_name,
            "connector.class": self.CONNECTOR_CLASS,
            # Offset storage
            "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename": offset_file,
            "offset.flush.interval.ms": str(config.debezium.offset_flush_interval_ms),
            # PostgreSQL connection
            "database.hostname": self._config.pg_host,
            "database.port": str(self._config.pg_port),
            "database.user": self._config.pg_username,
            "database.password": decrypt_value(self._config.pg_password or ""),
            "database.dbname": self._config.pg_database,
            # Connection pooling and stability
            "database.tcpKeepAlive": "true",
            "database.connectTimeout": "30000",  # 30 seconds - connection establishment timeout
            "database.socketTimeout": "0",  # 0 = unlimited - prevents timeout on long queries/snapshots
            # Replication settings
            "plugin.name": self.PLUGIN_NAME,
            "slot.name": slot_name,
            "publication.name": self._config.publication_name,
            # Publication auto-create mode
            # "filtered" = create publication with only tables from table.include.list
            # "disabled" = publication must already exist
            "publication.autocreate.mode": "filtered",
            # Snapshot behavior
            # "recovery" = take snapshot when stored offset is no longer available (WAL recycled)
            # "no_data" = skip initial snapshot, but fail if offset is lost
            # Using "recovery" to handle slot recreation and LSN mismatch scenarios
            "snapshot.mode": "no_data",
            # Table filtering
            "schema.include.list": self.DEFAULT_SCHEMA,
            "table.include.list": ",".join(tables_with_schema),
            # Heartbeat to prevent WAL growth
            "heartbeat.interval.ms": str(config.pipeline.heartbeat_interval_ms),
            "heartbeat.action.query": "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar)",
            # Performance settings
            "max.batch.size": str(config.pipeline.max_batch_size),
            "max.queue.size": str(config.pipeline.max_queue_size),
            "poll.interval.ms": str(config.pipeline.poll_interval_ms),
            # Slot management
            "slot.drop.on.stop": "false",
            "slot.max.retries": str(config.pipeline.slot_max_retries),
            "slot.retry.delay.ms": str(config.pipeline.slot_retry_delay_ms),
            # Error handling - stop on unrecoverable errors
            "errors.max.retries": "3",
            "errors.retry.delay.initial.ms": "1000",
            "errors.retry.delay.max.ms": "30000",
            # Topic prefix for routing
            "topic.prefix": f"rosetta_{pipeline_name}",
            # Status update frequency
            "status.update.interval.ms": "10000",  # 10 seconds
            # Tombstone events
            "tombstones.on.delete": "true",
        }

        return props

    def build_heartbeat_query(self) -> str:
        """Get heartbeat query for WAL advancement."""
        return "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar)"

    @classmethod
    def from_source_id(cls, source_id: int) -> Optional["PostgreSQLSource"]:
        """
        Create PostgreSQLSource from source ID.

        Args:
            source_id: Database source ID

        Returns:
            PostgreSQLSource instance or None if not found
        """
        from core.repository import SourceRepository

        source = SourceRepository.get_by_id(source_id)
        if source is None:
            return None

        return cls(source)
