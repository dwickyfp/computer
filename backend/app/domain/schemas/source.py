"""
Source schemas.

The public contract is typed via ``type`` + ``config``. PostgreSQL runtime
metadata remains exposed for the operational UI, while Kafka uses the same
shape with different config validation.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from app.domain.schemas.common import BaseSchema, TimestampSchema

_POSTGRES_CONFIG_KEYS = {
    "host",
    "port",
    "database",
    "username",
    "password",
    "publication_name",
    "replication_name",
}
_KAFKA_CONFIG_KEYS = {
    "bootstrap_servers",
    "topic_prefix",
    "group_id",
    "security_protocol",
    "sasl_mechanism",
    "sasl_username",
    "sasl_password",
    "ssl_ca_location",
    "ssl_certificate_location",
    "ssl_key_location",
    "auto_offset_reset",
    "format",
}
_SENSITIVE_CONFIG_KEYS = {
    "password",
    "sasl_password",
    "ssl_key_password",
    "secret_key",
    "private_key",
    "private_key_passphrase",
}


def _normalize_legacy_source_data(data: Any) -> dict[str, Any]:
    """Normalize ORM or dict input into the typed source response shape."""
    if isinstance(data, dict):
        result = dict(data)
    else:
        result = {
            "id": getattr(data, "id", None),
            "name": getattr(data, "name", None),
            "type": getattr(data, "type", "POSTGRES"),
            "config": getattr(data, "config", None),
            "is_publication_enabled": getattr(data, "is_publication_enabled", False),
            "is_replication_enabled": getattr(data, "is_replication_enabled", False),
            "last_check_replication_publication": getattr(
                data, "last_check_replication_publication", None
            ),
            "total_tables": getattr(data, "total_tables", 0),
            "created_at": getattr(data, "created_at", None),
            "updated_at": getattr(data, "updated_at", None),
        }

        for attr in (
            "pg_host",
            "pg_port",
            "pg_database",
            "pg_username",
            "publication_name",
            "replication_name",
        ):
            result[attr] = getattr(data, attr, None)

    source_type = str(result.get("type") or "POSTGRES").upper()
    config = dict(result.get("config") or {})
    if not config and source_type == "POSTGRES":
        config = {
            "host": result.get("pg_host"),
            "port": result.get("pg_port"),
            "database": result.get("pg_database"),
            "username": result.get("pg_username"),
            "publication_name": result.get("publication_name"),
            "replication_name": result.get("replication_name"),
        }
    result["type"] = source_type
    result["config"] = config
    return result


def _mask_config(config: dict[str, Any]) -> dict[str, Any]:
    masked = dict(config or {})
    for key in list(masked.keys()):
        lowered = key.lower()
        if lowered in _SENSITIVE_CONFIG_KEYS or "password" in lowered:
            masked.pop(key, None)
    return masked


def _validate_config(source_type: str, config: dict[str, Any], require_secret: bool) -> dict[str, Any]:
    source_type = source_type.upper()
    config = dict(config or {})

    if source_type == "POSTGRES":
        required = ["host", "database", "username", "publication_name", "replication_name"]
        missing = [key for key in required if not config.get(key)]
        if require_secret and not config.get("password"):
            missing.append("password")
        if missing:
            raise ValueError(
                f"POSTGRES source config is missing required fields: {', '.join(sorted(set(missing)))}"
            )
        if "port" not in config or config.get("port") in (None, ""):
            config["port"] = 5432
    elif source_type == "KAFKA":
        required = ["bootstrap_servers", "topic_prefix"]
        missing = [key for key in required if not config.get(key)]
        if missing:
            raise ValueError(
                f"KAFKA source config is missing required fields: {', '.join(missing)}"
            )
        group_id = str(config.get("group_id") or "").strip()
        if group_id:
            config["group_id"] = group_id
        else:
            config.pop("group_id", None)
        config.setdefault("auto_offset_reset", "earliest")
        fmt = str(config.get("format") or "PLAIN_JSON").upper()
        if fmt not in {"PLAIN_JSON", "DEBEZIUM_JSON"}:
            raise ValueError("KAFKA source format must be one of ['PLAIN_JSON', 'DEBEZIUM_JSON']")
        config["format"] = fmt
    else:
        raise ValueError("type must be one of ['POSTGRES', 'KAFKA']")

    return config


class SourceBase(BaseSchema):
    """Base typed source schema."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique source name",
        examples=["production-db", "orders-cdc"],
    )
    type: str = Field(
        default="POSTGRES",
        description="Source type",
        examples=["POSTGRES", "KAFKA"],
    )
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Typed source configuration",
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Source name must contain only alphanumeric characters, hyphens, and underscores"
            )
        return value.lower()

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        value = value.upper()
        if value not in {"POSTGRES", "KAFKA"}:
            raise ValueError("type must be one of ['POSTGRES', 'KAFKA']")
        return value


class SourceCreate(SourceBase):
    """Create a typed source."""

    @model_validator(mode="after")
    def validate_typed_config(self) -> "SourceCreate":
        self.config = _validate_config(self.type, self.config, require_secret=True)
        return self

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "orders-postgres",
                "type": "POSTGRES",
                "config": {
                    "host": "postgres.example.com",
                    "port": 5432,
                    "database": "orders",
                    "username": "replication_user",
                    "password": "SecurePassword123!",
                    "publication_name": "dbz_publication",
                    "replication_name": "dbz_replication_slot",
                },
            }
        }
    )


class SourceConnectionTest(BaseSchema):
    """Connection test for a typed source."""

    type: str = Field(default="POSTGRES", description="Source type")
    config: dict[str, Any] = Field(default_factory=dict, description="Source config")

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        value = value.upper()
        if value not in {"POSTGRES", "KAFKA"}:
            raise ValueError("type must be one of ['POSTGRES', 'KAFKA']")
        return value

    @model_validator(mode="after")
    def validate_typed_config(self) -> "SourceConnectionTest":
        self.config = _validate_config(self.type, self.config, require_secret=True)
        return self


class SourceUpdate(BaseSchema):
    """Update a typed source."""

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="Unique source name"
    )
    type: str | None = Field(default=None, description="Source type")
    config: dict[str, Any] | None = Field(default=None, description="Typed source config")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Source name must contain only alphanumeric characters, hyphens, and underscores"
            )
        return value.lower()

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str | None) -> str | None:
        if value is None:
            return value
        value = value.upper()
        if value not in {"POSTGRES", "KAFKA"}:
            raise ValueError("type must be one of ['POSTGRES', 'KAFKA']")
        return value

    @model_validator(mode="after")
    def validate_config(self) -> "SourceUpdate":
        if self.config is not None:
            source_type = self.type or "POSTGRES"
            self.config = _validate_config(source_type, self.config, require_secret=False)
        return self


class SourceResponse(TimestampSchema):
    """Typed source response."""

    id: int = Field(..., description="Unique source identifier", examples=[1, 42])
    name: str = Field(..., description="Source name")
    type: str = Field(..., description="Source type")
    config: dict[str, Any] = Field(default_factory=dict, description="Masked source config")
    is_publication_enabled: bool = Field(
        default=False, description="Whether publication is enabled"
    )
    is_replication_enabled: bool = Field(
        default=False, description="Whether replication is enabled"
    )
    last_check_replication_publication: Optional[datetime] = Field(
        default=None, description="Last timestamp of replication/publication check"
    )
    total_tables: int = Field(default=0, description="Total registered tables")

    @model_validator(mode="before")
    @classmethod
    def normalize_input(cls, data: Any) -> Any:
        return _normalize_legacy_source_data(data)

    @field_validator("config", mode="before")
    @classmethod
    def mask_sensitive_config(cls, value: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
        source_type = (info.data or {}).get("type", "POSTGRES")
        config = dict(value or {})
        if source_type == "POSTGRES":
            config = {
                key: config.get(key)
                for key in (
                    "host",
                    "port",
                    "database",
                    "username",
                    "publication_name",
                    "replication_name",
                )
                if config.get(key) is not None
            }
        return _mask_config(config)

    model_config = ConfigDict(from_attributes=True)


class PublicationCreateRequest(BaseModel):
    tables: list[str] = Field(
        ..., min_length=1, description="List of tables to include in publication"
    )
