"""
Connection factory for Flow Task input nodes.

Resolves source/destination configs into DuckDB ATTACH strings.
Supports: POSTGRES, SNOWFLAKE (key-pair auth).

Designed as an extensible registry — add new adapters without changing
the compiler or executor.
"""

from __future__ import annotations

import os
import tempfile
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

# Thread-local storage for tracking temp files created during ATTACH operations
_thread_local = threading.local()


def _track_temp_file(path: str) -> None:
    """Track a temp file for later cleanup (per-thread)."""
    if not hasattr(_thread_local, "temp_files"):
        _thread_local.temp_files = []
    _thread_local.temp_files.append(path)


def cleanup_temp_files() -> None:
    """Remove all temp files created by the current thread's ATTACH operations."""
    files = getattr(_thread_local, "temp_files", [])
    for f in files:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except Exception:
            pass
    _thread_local.temp_files = []


# ─── Adapter base + registry ───────────────────────────────────────────────────

class BaseConnectionAdapter(ABC):
    """Abstract base for DuckDB connection adapters."""

    @abstractmethod
    def get_attach_sql(
        self,
        alias: str,
        config: Dict[str, Any],
        read_only: bool = True,
    ) -> Tuple[str, Optional[str]]:
        """
        Return (ATTACH SQL, optional setup SQL).

        The ATTACH SQL installs the connection using DuckDB's ATTACH syntax.
        Setup SQL (if any) should be executed before the ATTACH (e.g., INSTALL extensions).
        """
        ...

    @abstractmethod
    def get_extension_name(self) -> Optional[str]:
        """Return the DuckDB extension name required (or None)."""
        ...


class PostgresAdapter(BaseConnectionAdapter):
    """
    DuckDB postgres extension adapter.

    Config keys (from sources table config column or direct kwargs):
      host, port, database (or dbname), user (or username), password
    """

    def get_extension_name(self) -> str:
        return "postgres"

    def get_attach_sql(
        self,
        alias: str,
        config: Dict[str, Any],
        read_only: bool = True,
    ) -> Tuple[str, Optional[str]]:
        host = config.get("host") or config.get("pg_host", "localhost")
        port = config.get("port") or config.get("pg_port", 5432)
        database = config.get("database") or config.get("pg_database", "")
        user = config.get("user") or config.get("username") or config.get("pg_username", "")
        password = config.get("password") or config.get("pg_password", "")

        dsn = (
            f"host={host} port={port} dbname={database} "
            f"user={user} password={password}"
        )
        ro_flag = ", READ_ONLY" if read_only else ""
        attach_sql = f"ATTACH '{dsn}' AS {alias} (TYPE postgres{ro_flag})"
        return attach_sql, None


class SnowflakeAdapter(BaseConnectionAdapter):
    """
    DuckDB Snowflake extension adapter — key-pair authentication.

    Config keys (from destinations table config JSONB):
      account, user, database, warehouse, schema (optional)
      private_key_content (PEM string) or private_key_path
      private_key_passphrase (optional)
    """

    def get_extension_name(self) -> str:
        return "snowflake"

    def get_attach_sql(
        self,
        alias: str,
        config: Dict[str, Any],
        read_only: bool = True,
    ) -> Tuple[str, Optional[str]]:
        from app.core.security import decrypt_value

        account = config.get("account", "")
        user = config.get("user", "")
        database = config.get("database", "")
        warehouse = config.get("warehouse", "")

        # Handle private key — prefer content over path.
        # The config JSON may store it as "private_key_content" (worker standard)
        # or as "private_key" (backend/pipeline standard) — accept both.
        private_key_content: Optional[str] = (
            config.get("private_key_content") or config.get("private_key")
        )
        private_key_path: Optional[str] = config.get("private_key_path")
        passphrase: Optional[str] = (
            config.get("private_key_passphrase") or config.get("passphrase")
        )

        # Decrypt if the value looks encrypted (base64 ciphertext)
        if private_key_content and not private_key_content.strip().startswith("-----"):
            try:
                private_key_content = decrypt_value(private_key_content)
            except Exception:
                pass  # Already plaintext

        if not private_key_content and not private_key_path:
            raise ValueError(
                "Snowflake adapter requires private_key_content or private_key_path "
                "in destination config"
            )

        # If we have content, write to a temp file (DuckDB needs a path for key-pair)
        if private_key_content and not private_key_path:
            tmp = tempfile.NamedTemporaryFile(
                suffix=".p8", delete=False, mode="w", prefix="rsa_"
            )
            tmp.write(private_key_content)
            tmp.close()
            private_key_path = tmp.name
            _track_temp_file(tmp.name)

        conn_str_parts = [
            f"account={account}",
            f"user={user}",
            f"auth_type=key_pair",
            f"private_key={private_key_path}",
            f"database={database}",
            f"warehouse={warehouse}",
        ]
        if passphrase:
            conn_str_parts.append(f"private_key_passphrase={passphrase}")

        conn_str = ";".join(conn_str_parts)
        ro_flag = ", READ_ONLY" if read_only else ""
        attach_sql = f"ATTACH '{conn_str}' AS {alias} (TYPE snowflake{ro_flag})"

        return attach_sql, None


# ─── Registry ─────────────────────────────────────────────────────────────────

_ADAPTER_REGISTRY: Dict[str, type] = {
    "POSTGRES": PostgresAdapter,
    "SNOWFLAKE": SnowflakeAdapter,
    # Future: "BIGQUERY": BigQueryAdapter, "MYSQL": MySQLAdapter
}


def register_adapter(source_type: str, adapter_class: type) -> None:
    """Register a new connection adapter. Allows extension without modifying this file."""
    _ADAPTER_REGISTRY[source_type.upper()] = adapter_class


# ─── Factory ──────────────────────────────────────────────────────────────────

class SourceConnectionFactory:
    """
    Resolves a source/destination record into a DuckDB ATTACH SQL string.

    Fetches credentials from the config database, decrypts them, and
    delegates to the appropriate adapter.
    """

    @staticmethod
    def get_adapter(source_type: str) -> BaseConnectionAdapter:
        adapter_cls = _ADAPTER_REGISTRY.get(source_type.upper())
        if not adapter_cls:
            raise ValueError(
                f"Unsupported source_type '{source_type}'. "
                f"Registered: {list(_ADAPTER_REGISTRY.keys())}"
            )
        return adapter_cls()

    @staticmethod
    def build_attach_sql_from_source(
        source_id: int,
        alias: str,
        read_only: bool = True,
    ) -> Tuple[str, Optional[str], str]:
        """
        Build ATTACH SQL from a sources table record.

        Returns (attach_sql, setup_sql_or_None, extension_name).
        """
        from app.core.database import get_db_session
        from app.core.security import decrypt_value

        # Fetch source config
        with get_db_session() as db:
            from sqlalchemy import text
            row = db.execute(
                text("SELECT pg_host, pg_port, pg_database, pg_username, pg_password "
                     "FROM sources WHERE id = :id"),
                {"id": source_id},
            ).fetchone()

        if not row:
            raise ValueError(f"Source {source_id} not found")

        config = {
            "host": row.pg_host,
            "port": row.pg_port,
            "database": row.pg_database,
            "user": row.pg_username,
            "password": decrypt_value(row.pg_password) if row.pg_password else "",
        }
        adapter = SourceConnectionFactory.get_adapter("POSTGRES")
        attach_sql, setup_sql = adapter.get_attach_sql(alias, config, read_only=read_only)
        return attach_sql, setup_sql, adapter.get_extension_name()

    @staticmethod
    def build_attach_sql_from_destination(
        destination_id: int,
        alias: str,
        read_only: bool = True,
    ) -> Tuple[str, Optional[str], str]:
        """
        Build ATTACH SQL from a destinations table record.

        Returns (attach_sql, setup_sql_or_None, extension_name).
        """
        from app.core.database import get_db_session
        from app.core.security import decrypt_value
        import json

        with get_db_session() as db:
            from sqlalchemy import text
            row = db.execute(
                text("SELECT type, config FROM destinations WHERE id = :id"),
                {"id": destination_id},
            ).fetchone()

        if not row:
            raise ValueError(f"Destination {destination_id} not found")

        dest_type = row.type.upper()
        config_raw = row.config if isinstance(row.config, dict) else json.loads(row.config)

        # Decrypt password fields
        for key in ("password", "private_key_content", "private_key", "private_key_passphrase", "passphrase"):
            if key in config_raw and config_raw[key]:
                try:
                    config_raw[key] = decrypt_value(config_raw[key])
                except Exception:
                    pass  # Already plaintext

        adapter = SourceConnectionFactory.get_adapter(dest_type)
        attach_sql, setup_sql = adapter.get_attach_sql(alias, config_raw, read_only=read_only)
        return attach_sql, setup_sql, adapter.get_extension_name()

    @staticmethod
    def get_destination_schema(destination_id: int) -> Optional[str]:
        """
        Return the schema configured for a destination (e.g. 'BRONZE' for Snowflake).
        Returns None if not found or not configured.
        """
        import json as _json

        from app.core.database import get_db_session

        with get_db_session() as db:
            from sqlalchemy import text
            row = db.execute(
                text("SELECT config FROM destinations WHERE id = :id"),
                {"id": destination_id},
            ).fetchone()

        if not row:
            return None

        config_raw = row.config if isinstance(row.config, dict) else _json.loads(row.config)
        return config_raw.get("schema") or config_raw.get("schema_name") or None
