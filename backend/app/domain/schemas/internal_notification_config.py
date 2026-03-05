"""
Internal Notification Config schemas.

Defines Pydantic v1 schemas for internal notification configuration.
"""

from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel, Field, validator


class InternalNotificationConfigBase(BaseModel):
    """Shared fields for internal notification config."""

    name: str = Field(..., description="Human-readable label for this config")
    is_enabled: bool = Field(True, description="Whether this config is active")
    base_url: str = Field(..., description="Base URL of the internal notification API")
    requester: str = Field(..., description="Requester identifier passed to the API")
    menu_code: str = Field(..., description="Menu code passed to the API")
    company_group_id: int = Field(1, description="Company group ID passed to the API")
    mail_from_code: str = Field(..., description="Mail-from code passed to the API")
    mail_to: str = Field(
        ...,
        description="Comma-separated list of recipient email addresses",
    )
    subject: str = Field(..., description="Email subject passed to the API")


class InternalNotificationConfigCreate(InternalNotificationConfigBase):
    """Schema for creating a new internal notification config."""

    pass


class InternalNotificationConfigUpdate(BaseModel):
    """Schema for updating an existing internal notification config (all fields optional)."""

    name: Optional[str] = None
    is_enabled: Optional[bool] = None
    base_url: Optional[str] = None
    requester: Optional[str] = None
    menu_code: Optional[str] = None
    company_group_id: Optional[int] = None
    mail_from_code: Optional[str] = None
    mail_to: Optional[str] = None
    subject: Optional[str] = None


class InternalNotificationConfigResponse(InternalNotificationConfigBase):
    """Schema for reading an internal notification config."""

    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class InternalNotificationGlobalToggleRequest(BaseModel):
    """Request body for toggling the global internal notification switch."""

    is_active: bool = Field(
        ..., description="Enable or disable all internal notifications globally"
    )


class InternalNotificationGlobalStatusResponse(BaseModel):
    """Response for the global enable flag."""

    is_active: bool
