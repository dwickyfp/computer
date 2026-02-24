"""
Configuration schemas.

Defines schemas for configuration settings.
"""

from pydantic import BaseModel, Field


class ConfigurationBase(BaseModel):
    """Base configuration schema."""
    
    config_key: str = Field(..., description="Configuration key")
    config_value: str = Field(..., description="Configuration value")


class ConfigurationCreate(ConfigurationBase):
    """Schema for creating configuration."""
    pass


class ConfigurationUpdate(BaseModel):
    """Schema for updating configuration."""
    
    config_value: str = Field(..., description="New configuration value")


class ConfigurationResponse(ConfigurationBase):
    """Schema for configuration response."""
    
    id: int
    created_at: str
    updated_at: str
    
    class Config:
        from_attributes = True


class WALThresholds(BaseModel):
    """WAL monitoring threshold configuration."""
    
    warning: int = Field(..., description="Warning threshold in bytes")
    error: int = Field(..., description="Error threshold in bytes")
    enable_webhook: bool = Field(default=False, description="Enable or disable webhook notifications")
    webhook_url: str = Field(default="", description="Webhook URL for alerts")
    notification_iteration: int = Field(default=3, description="Number of iterations before sending notification")
    enable_telegram: bool = Field(default=False, description="Enable or disable Telegram notifications")
    telegram_bot_token: str = Field(default="", description="Telegram bot token")
    telegram_chat_id: str = Field(default="", description="Telegram chat ID or group ID")


class TestNotificationRequest(BaseModel):
    """Schema for test notification request."""
    
    webhook_url: str = Field(default=None, description="Optional webhook URL to test")
    telegram_bot_token: str = Field(default=None, description="Optional Telegram bot token to test")
    telegram_chat_id: str = Field(default=None, description="Optional Telegram chat ID to test")


class BatchConfiguration(BaseModel):
    """Batch processing configuration."""
    
    max_batch_size: int = Field(..., description="Maximum batch size for CDC processing", ge=1024, le=16384)
    max_queue_size: int = Field(..., description="Maximum queue size for CDC processing", ge=2048, le=65536)