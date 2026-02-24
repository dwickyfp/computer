"""
Core configuration module using Pydantic Settings.

Handles all application configuration with validation and type safety.
"""

from functools import lru_cache
from typing import Any, List

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    """
    Application settings with validation.

    All settings are loaded from environment variables or .env file.
    Pydantic ensures type safety and validation at startup.
    """

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"

    # Application Configuration
    app_name: str = Field(
        default="Rosetta ETL Platform", description="Application name"
    )
    app_version: str = Field(default="1.0.0", description="Application version")
    app_env: str = Field(
        default="development",
        description="Environment: development, staging, production",
    )
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level")

    # API Configuration
    api_v1_prefix: str = Field(default="/api/v1", description="API v1 route prefix")
    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")

    # Database Configuration
    database_url: str = Field(..., description="PostgreSQL connection URL")
    db_pool_size: int = Field(
        default=5, ge=1, le=100, description="Database connection pool size"
    )
    db_max_overflow: int = Field(
        default=5,
        ge=0,
        le=50,
        description="Maximum overflow connections beyond pool_size",
    )
    db_pool_timeout: int = Field(
        default=30, ge=5, le=120, description="Seconds to wait for connection from pool"
    )
    db_pool_recycle: int = Field(
        default=3600, ge=300, description="Seconds before recycling connections"
    )
    db_echo: bool = Field(
        default=False, description="Echo SQL statements (for debugging)"
    )
    db_pool_pre_ping: bool = Field(
        default=True, description="Test connections before using them"
    )
    db_pool_use_lifo: bool = Field(
        default=True,
        description="Use LIFO for connection pool (better for connection reuse)",
    )

    # Security
    secret_key: str = Field(
        ..., min_length=32, description="Secret key for JWT and encryption"
    )
    credential_encryption_key: str = Field(
        ...,
        min_length=32,
        description="Master key for credential encryption (AES-256-GCM)",
    )
    api_key: str = Field(default="", description="Optional API key for authentication")
    cors_origins: List[str] = Field(
        default=["http://localhost:3000"], description="Allowed CORS origins"
    )

    # WAL Monitoring Configuration
    wal_monitor_enabled: bool = Field(
        default=True, description="Enable WAL monitoring background task"
    )
    wal_monitor_interval_seconds: int = Field(
        default=60, ge=60, le=3600, description="Interval between WAL checks (seconds)"
    )
    wal_monitor_timeout_seconds: int = Field(
        default=60, ge=5, le=300, description="Timeout for WAL query execution"
    )
    wal_monitor_max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum retry attempts for failed WAL checks",
    )

    # Background Tasks
    background_task_enabled: bool = Field(
        default=True, description="Enable background task scheduler"
    )
    scheduler_timezone: str = Field(
        default="UTC", description="Timezone for task scheduler"
    )

    # Redis Configuration
    redis_url: str = Field(
        default="redis://localhost:6379/0", description="Redis connection URL"
    )

    # Worker / Celery Configuration
    worker_enabled: bool = Field(
        default=False,
        description="Enable async worker mode for heavy tasks (preview, etc.)",
    )
    celery_broker_url: str = Field(
        default="redis://localhost:6379/1",
        description="Celery broker URL (Redis db 1)",
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/2",
        description="Celery result backend URL (Redis db 2)",
    )

    # Logging
    log_format: str = Field(default="json", description="Log format: json or text")
    log_file_path: str = Field(default="logs/app.log", description="Path to log file")
    log_file_max_bytes: int = Field(
        default=10485760, description="Maximum log file size in bytes"
    )
    log_file_backup_count: int = Field(
        default=5, description="Number of backup log files to keep"
    )

    # Metrics & Monitoring
    enable_metrics: bool = Field(
        default=True, description="Enable Prometheus metrics endpoint"
    )
    metrics_port: int = Field(
        default=9090, ge=1024, le=65535, description="Port for metrics server"
    )

    # Rate Limiting
    rate_limit_enabled: bool = Field(
        default=True, description="Enable API rate limiting"
    )
    rate_limit_per_minute: int = Field(
        default=60, ge=1, le=1000, description="Maximum requests per minute per client"
    )

    # External Services
    compute_node_url: str = Field(
        default="http://0.0.0.0:8001", description="URL for the compute node API"
    )
    worker_health_url: str = Field(
        default="http://0.0.0.0:8002", description="URL for the worker health API"
    )

    @validator("app_env")
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of allowed values."""
        allowed = ["development", "staging", "production"]
        if v.lower() not in allowed:
            raise ValueError(f"app_env must be one of {allowed}")
        return v.lower()

    @validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.app_env == "development"

    @property
    def database_connection_string(self) -> str:
        """
        Get the database connection string with psycopg2 driver.

        Converts asyncpg URLs to psycopg2 format.
        """
        db_url = self.database_url
        # Convert async URL to sync URL for psycopg2
        if db_url.startswith("postgresql+asyncpg://"):
            db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        elif db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")
        return db_url

    def get_sqlalchemy_engine_config(self) -> dict[str, Any]:
        """
        Get SQLAlchemy engine configuration.

        Returns optimized settings for async connection pooling with safeguards.
        """
        return {
            "pool_size": self.db_pool_size,
            "max_overflow": self.db_max_overflow,
            "pool_timeout": self.db_pool_timeout,
            "pool_recycle": self.db_pool_recycle,
            "pool_pre_ping": self.db_pool_pre_ping,
            "pool_use_lifo": self.db_pool_use_lifo,
            "echo": self.db_echo,
            "echo_pool": False,  # Disabled: setup_logging() handles pool log levels
            "future": True,
        }


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses LRU cache to ensure settings are loaded only once.
    This is the recommended pattern for FastAPI dependency injection.
    """
    return Settings()


# Global settings instance (use get_settings() for dependency injection)
settings = get_settings()
