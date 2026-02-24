"""
Configuration Service.

Manages application configuration settings.
"""

from sqlalchemy.orm import Session
from sqlalchemy import text
from app.domain.repositories.configuration_repo import ConfigurationRepository
from app.domain.schemas.configuration import WALThresholds, BatchConfiguration
from zoneinfo import ZoneInfo


class ConfigurationService:
    """Service for managing configuration settings."""

    def __init__(self, db: Session):
        """Initialize service."""
        self.repo = ConfigurationRepository(db)

    def get_wal_thresholds(self) -> WALThresholds:
        """
        Get WAL monitoring thresholds from configuration.

        Note: Thresholds in database are stored in MB and converted to bytes here.

        Returns:
            WAL thresholds configuration with values in bytes
        """
        # Get values in MB from database
        warning_mb = int(
            self.repo.get_value("WAL_MONITORING_THRESHOLD_WARNING", "3000")
        )
        error_mb = int(self.repo.get_value("WAL_MONITORING_THRESHOLD_ERROR", "6000"))
        enable_webhook = self.repo.get_value(
            "ENABLE_ALERT_NOTIFICATION_WEBHOOK", "FALSE"
        ).upper() == "TRUE"
        webhook_url = self.repo.get_value("ALERT_NOTIFICATION_WEBHOOK_URL", "")
        notification_iteration = int(
            self.repo.get_value("NOTIFICATION_ITERATION_DEFAULT", "3")
        )
        enable_telegram = self.repo.get_value(
            "ENABLE_ALERT_NOTIFICATION_TELEGRAM", "FALSE"
        ).upper() == "TRUE"
        telegram_bot_token = self.repo.get_value("ALERT_NOTIFICATION_TELEGRAM_KEY", "")
        telegram_chat_id = self.repo.get_value("ALERT_NOTIFICATION_TELEGRAM_GROUP_ID", "")

        # Convert MB to bytes (1 MB = 1024 * 1024 bytes)
        warning_bytes = warning_mb * 1024 * 1024
        error_bytes = error_mb * 1024 * 1024

        return WALThresholds(
            warning=warning_bytes,
            error=error_bytes,
            enable_webhook=enable_webhook,
            webhook_url=webhook_url,
            notification_iteration=notification_iteration,
            enable_telegram=enable_telegram,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
        )

    def get_value(self, key: str, default: str = "") -> str:
        """
        Get configuration value by key.

        Args:
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value
        """
        return self.repo.get_value(key, default)

    def set_value(self, key: str, value: str):
        """
        Set configuration value.

        Args:
            key: Configuration key
            value: Configuration value
        """
        return self.repo.set_value(key, value)

    def get_batch_configuration(self) -> BatchConfiguration:
        """
        Get batch configuration settings.

        Returns:
            Batch configuration with max_batch_size and max_queue_size
        """
        max_batch_size = int(self.repo.get_value("PIPELINE_MAX_BATCH_SIZE", "4096"))
        max_queue_size = int(self.repo.get_value("PIPELINE_MAX_QUEUE_SIZE", "16384"))

        return BatchConfiguration(
            max_batch_size=max_batch_size, max_queue_size=max_queue_size
        )

    def update_batch_configuration(
        self, config: BatchConfiguration
    ) -> BatchConfiguration:
        """
        Update batch configuration settings and mark all pipelines for refresh.

        Args:
            config: New batch configuration values

        Returns:
            Updated batch configuration
        """
        from datetime import datetime, timezone

        self.repo.set_value("PIPELINE_MAX_BATCH_SIZE", str(config.max_batch_size))
        self.repo.set_value("PIPELINE_MAX_QUEUE_SIZE", str(config.max_queue_size))

        # Mark all pipelines to be restarted with new configuration and update last_refresh_at
        self.repo.db.execute(
            text(
                "UPDATE pipelines SET ready_refresh = TRUE, last_refresh_at = :now WHERE status = 'START'"
            ),
            {"now": datetime.now(ZoneInfo("Asia/Jakarta"))},
        )
        self.repo.db.commit()

        return config
