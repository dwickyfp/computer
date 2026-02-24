"""
Worker settings configuration.

Pydantic-based settings with environment variable support.
"""

from functools import lru_cache
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Worker service configuration."""

    # Database (Config DB - shared with backend)
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5433/postgres",
        description="Config database URL",
    )

    # Connection Pool
    db_pool_size: int = Field(default=10, ge=1, le=50, description="DB pool size")
    db_max_overflow: int = Field(
        default=10, ge=0, le=50, description="DB pool overflow"
    )
    db_pool_timeout: int = Field(
        default=30, ge=5, le=120, description="DB pool timeout"
    )
    db_pool_recycle: int = Field(
        default=1800, ge=300, description="DB pool recycle time"
    )

    # Redis (cache, same db as backend)
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis URL for caching",
    )

    # Celery
    celery_broker_url: str = Field(
        default="redis://localhost:6379/1",
        description="Celery broker URL (Redis db 1)",
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/2",
        description="Celery result backend (Redis db 2)",
    )

    # Security (must match backend)
    credential_encryption_key: str = Field(
        ...,
        min_length=32,
        description="Master key for credential encryption (AES-256-GCM)",
    )

    # Worker Behavior
    # Concurrency must be >= duckdb_max_concurrent + linked_task_max_parallel_steps
    # + headroom to prevent deadlocks between linked task orchestration threads
    # and child flow task execution threads.
    worker_concurrency: int = Field(
        default=10, ge=1, le=32, description="Number of concurrent worker threads"
    )
    task_soft_time_limit: int = Field(
        default=120, ge=10, le=600, description="Soft time limit for tasks (seconds)"
    )
    task_hard_time_limit: int = Field(
        default=180, ge=30, le=900, description="Hard time limit for tasks (seconds)"
    )
    preview_row_limit: int = Field(
        default=500, ge=1, le=50000, description="Max rows for preview queries"
    )
    duckdb_memory_limit: str = Field(
        default="2GB", description="DuckDB memory limit per query"
    )
    duckdb_threads: int = Field(
        default=4, ge=1, le=16, description="DuckDB threads per query"
    )

    # DuckDB Concurrency
    duckdb_max_concurrent: int = Field(
        default=4, ge=1, le=16, description="Max concurrent DuckDB connections"
    )

    # Redis Connection Pool
    redis_max_connections: int = Field(
        default=20, ge=5, le=50, description="Redis connection pool max connections"
    )

    # Memory Management
    memory_warning_mb: int = Field(
        default=4096, ge=512, description="Memory warning threshold in MB"
    )

    # Linked Task
    linked_task_max_parallel_steps: int = Field(
        default=3, ge=1, le=8, description="Max parallel steps in linked task"
    )

    # Health API Server
    server_host: str = Field(default="0.0.0.0", description="Health API server host")
    server_port: int = Field(
        default=8002, ge=1024, le=65535, description="Health API server port"
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format: json or text")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @property
    def database_connection_string(self) -> str:
        """Get sync database connection string."""
        db_url = self.database_url
        if db_url.startswith("postgresql+asyncpg://"):
            db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        elif db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")
        return db_url


@lru_cache()
def get_settings() -> WorkerSettings:
    """Get cached settings instance."""
    return WorkerSettings()


settings = get_settings()
