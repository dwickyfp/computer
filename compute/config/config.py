"""
Configuration management for Rosetta Compute Engine.

Loads environment variables and provides configuration access.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from functools import lru_cache
from core.security import decrypt_value
import logging

# Try to load dotenv if available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration for Rosetta config DB."""

    host: str = "localhost"
    port: int = 5432
    database: str = "rosetta"
    user: str = "postgres"
    password: str = "postgres"

    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def dsn(self) -> dict:
        """Get connection parameters as dict for psycopg2."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": decrypt_value(self.password),
        }


@dataclass
class DebeziumConfig:
    """Debezium engine configuration."""

    offset_storage_path: str = "./tmp/offsets"
    offset_flush_interval_ms: int = 10000

    def get_offset_file(self, pipeline_name: str) -> str:
        """Get offset file path for a specific pipeline."""
        path = Path(self.offset_storage_path)
        path.mkdir(parents=True, exist_ok=True)
        return str(path / f"{pipeline_name}.dat")


@dataclass
class PipelineConfig:
    """Pipeline processing configuration."""

    max_batch_size: int = 2048
    max_queue_size: int = 8192
    poll_interval_ms: int = 500
    slot_max_retries: int = 6
    slot_retry_delay_ms: int = 10000
    heartbeat_interval_ms: int = 10000

    def load_from_database(self):
        """Load batch configuration from database."""
        try:
            from core.database import get_db_connection, return_db_connection

            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    # Get batch size
                    cursor.execute(
                        "SELECT config_value FROM rosetta_setting_configuration WHERE config_key = %s",
                        ("PIPELINE_MAX_BATCH_SIZE",),
                    )
                    result = cursor.fetchone()
                    if result:
                        self.max_batch_size = int(result[0])

                    # Get queue size
                    cursor.execute(
                        "SELECT config_value FROM rosetta_setting_configuration WHERE config_key = %s",
                        ("PIPELINE_MAX_QUEUE_SIZE",),
                    )
                    result = cursor.fetchone()
                    if result:
                        self.max_queue_size = int(result[0])

            finally:
                return_db_connection(conn)
        except Exception as e:
            logger.warning(
                f"Failed to load batch config from database, using defaults: {e}"
            )


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class ServerConfig:
    """API Server configuration."""

    host: str = "0.0.0.0"
    port: int = 8001


@dataclass
class DLQConfig:
    """Dead Letter Queue configuration (Redis Streams backend)."""

    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "rosetta:dlq"
    max_stream_length: int = 100000  # MAXLEN cap per stream
    consumer_group: str = "dlq_recovery"
    check_interval: int = 30  # seconds between recovery attempts
    batch_size: int = 100  # number of messages to process per batch
    max_retry_count: int = 10  # max retries before discarding
    max_age_days: int = 7  # max age before purging
    block_ms: int = 2000  # XREADGROUP block timeout

    def get(self, key: str, default=None):
        """Dict-like access for compatibility."""
        return getattr(self, key, default)


@dataclass
class SnowflakeConfig:
    """Snowflake destination timeout configuration."""

    connect_timeout: float = 30.0  # Connection establishment timeout
    read_timeout: float = 300.0  # Read response timeout (increased from 120s)
    write_timeout: float = 60.0  # Write request timeout
    pool_timeout: float = 10.0  # Connection pool timeout
    batch_timeout_base: int = 300  # Base timeout for write_batch (increased from 120s)
    batch_timeout_max: int = (
        600  # Maximum timeout for write_batch (increased from 300s)
    )


@dataclass
class Config:
    """
    Central configuration for Rosetta Compute Engine.

    Loads configuration from environment variables with sensible defaults.
    """

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    debezium: DebeziumConfig = field(default_factory=DebeziumConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    dlq: DLQConfig = field(default_factory=DLQConfig)
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            database=DatabaseConfig(
                host=os.getenv("ROSETTA_DB_HOST", "localhost"),
                port=int(os.getenv("ROSETTA_DB_PORT", "5432")),
                database=os.getenv("ROSETTA_DB_NAME", "rosetta"),
                user=os.getenv("ROSETTA_DB_USER", "postgres"),
                password=os.getenv("ROSETTA_DB_PASSWORD", "postgres"),
            ),
            debezium=DebeziumConfig(
                offset_storage_path=os.getenv(
                    "DEBEZIUM_OFFSET_STORAGE_PATH", "./tmp/offsets"
                ),
                offset_flush_interval_ms=int(
                    os.getenv("DEBEZIUM_OFFSET_FLUSH_INTERVAL_MS", "60000")
                ),
            ),
            pipeline=PipelineConfig(
                max_batch_size=int(os.getenv("PIPELINE_MAX_BATCH_SIZE", "2048")),
                max_queue_size=int(os.getenv("PIPELINE_MAX_QUEUE_SIZE", "8192")),
                poll_interval_ms=int(os.getenv("PIPELINE_POLL_INTERVAL_MS", "500")),
            ),
            logging=LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO"),
                format=os.getenv(
                    "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                ),
            ),
            server=ServerConfig(
                host=os.getenv("SERVER_HOST", "0.0.0.0"),
                port=int(os.getenv("SERVER_PORT", "8001")),
            ),
            dlq=DLQConfig(
                redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                key_prefix=os.getenv("DLQ_KEY_PREFIX", "rosetta:dlq"),
                max_stream_length=int(os.getenv("DLQ_MAX_STREAM_LENGTH", "100000")),
                consumer_group=os.getenv("DLQ_CONSUMER_GROUP", "dlq_recovery"),
                check_interval=int(os.getenv("DLQ_CHECK_INTERVAL", "30")),
                batch_size=int(os.getenv("DLQ_BATCH_SIZE", "100")),
                max_retry_count=int(os.getenv("DLQ_MAX_RETRY_COUNT", "10")),
                max_age_days=int(os.getenv("DLQ_MAX_AGE_DAYS", "7")),
                block_ms=int(os.getenv("DLQ_BLOCK_MS", "2000")),
            ),
            snowflake=SnowflakeConfig(
                connect_timeout=float(os.getenv("SNOWFLAKE_CONNECT_TIMEOUT", "30.0")),
                read_timeout=float(os.getenv("SNOWFLAKE_READ_TIMEOUT", "300.0")),
                write_timeout=float(os.getenv("SNOWFLAKE_WRITE_TIMEOUT", "60.0")),
                pool_timeout=float(os.getenv("SNOWFLAKE_POOL_TIMEOUT", "10.0")),
                batch_timeout_base=int(
                    os.getenv("SNOWFLAKE_BATCH_TIMEOUT_BASE", "300")
                ),
                batch_timeout_max=int(os.getenv("SNOWFLAKE_BATCH_TIMEOUT_MAX", "600")),
            ),
        )


@lru_cache(maxsize=1)
def get_config() -> Config:
    """
    Get singleton configuration instance.

    Uses lru_cache to ensure only one Config instance exists.
    Uses environment variables or defaults - no database dependency.
    """
    config = Config.from_env()
    return config
