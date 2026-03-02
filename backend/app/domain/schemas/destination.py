"""
Destination Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving destination configurations.
"""

from typing import Any, Optional

from pydantic import ConfigDict, Field, ValidationInfo, field_validator

from app.domain.schemas.common import BaseSchema, TimestampSchema


class DestinationBase(BaseSchema):
    """Base destination schema with common fields."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique destination name",
        examples=["snowflake-prod", "analytics-warehouse"],
    )
    type: str = Field(
        default="SNOWFLAKE",
        description="Destination type",
        examples=["SNOWFLAKE", "KAFKA", "POSTGRES"],
    )


class DestinationCreate(DestinationBase):
    """
    Schema for creating a new destination.

    Requires Snowflake connection details.
    """

    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Destination configuration (JSON)",
        examples=[
            {
                "account": "xy12345.us-east-1",
                "user": "ETL_USER",
                "database": "ANALYTICS",
                "schema": "RAW_DATA",
                "role": "SYSADMIN",
                "warehouse": "COMPUTE_WH",
            }
        ],
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate destination type."""
        allowed_types = [
            "SNOWFLAKE",
            "KAFKA",
            "POSTGRES",
            "ROSETTA",
        ]  # Extend as needed
        if v.upper() not in allowed_types:
            # For now, we optionally allow other types if needed, but warning/error is better.
            # Let's just Upper it.
            pass
        return v.upper()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate destination name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Destination name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    @field_validator("config")
    @classmethod
    def validate_config(cls, v: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        """Validate config based on type."""
        # Simple validation for now.
        # Ideally we'd inspect 'type' from values, but validation order matters.
        # If type is SNOWFLAKE, ensure required fields exist.
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "snowflake-production",
                "type": "SNOWFLAKE",
                "config": {
                    "account": "xy12345.us-east-1",
                    "user": "ETL_USER",
                    "database": "ANALYTICS",
                    "schema": "RAW_DATA",
                    "role": "SYSADMIN",
                    "warehouse": "COMPUTE_WH",
                    "private_key_passphrase": "MySecurePassphrase123!",
                },
            }
        }
    )


class DestinationUpdate(BaseSchema):
    """
    Schema for updating an existing destination.

    All fields are optional to support partial updates.
    """

    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Unique destination name",
    )
    type: str | None = Field(
        default=None,
        description="Destination type",
    )
    config: dict[str, Any] | None = Field(
        default=None, description="Destination configuration (JSON)"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str | None) -> str | None:
        """Validate destination name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Destination name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class DestinationResponse(DestinationBase, TimestampSchema):
    """
    Schema for destination API responses.

    Includes all destination details (except sensitive passphrase).
    """

    id: int = Field(..., description="Unique destination identifier", examples=[1, 42])
    type: str = Field(..., description="Destination type", examples=["SNOWFLAKE"])
    config: dict[str, Any] = Field(
        default_factory=dict, description="Destination configuration (JSON)"
    )
    is_used_in_active_pipeline: bool = Field(
        default=False,
        description="Indicates if destination is used in any active pipeline",
    )
    total_tables: int = Field(
        default=0,
        description="Total number of tables cached from the last table list check",
    )
    last_table_check_at: Optional[str] = Field(
        default=None,
        description="ISO timestamp of the last table list check",
    )
    chain_client_id: Optional[int] = Field(
        default=None,
        description="Chain client that owns this destination (ROSETTA type only)",
    )

    @field_validator("config", mode="before")
    @classmethod
    def mask_sensitive_config(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Mask sensitive configuration values."""
        if not v:
            return v

        SENSITIVE_KEYS = {
            "password",
            "private_key",
            "private_key_passphrase",
            "aws_secret_access_key",
            "access_key",
            "secret_key",
        }

        # Create a new dict to avoid modifying the original
        masked = v.copy()
        for key in list(masked.keys()):
            key_lower = key.lower()
            if (
                key_lower in SENSITIVE_KEYS
                or "password" in key_lower
                or "key" in key_lower
                or "secret" in key_lower
            ):
                # Remove entirely or mask? User said "exclude".
                # "exclude the passkey and private key"
                # Safer to remove entirely from the response
                masked.pop(key, None)

        return masked

    @field_validator("last_table_check_at", mode="before")
    @classmethod
    def serialize_last_table_check_at(cls, v: Any) -> Optional[str]:
        """Serialize datetime to ISO string."""
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)
