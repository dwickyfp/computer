"""
Destination table list executor.

Connects to a destination (PostgreSQL or Snowflake) and fetches the list of
available tables, then persists the result back to the config database.
"""

import json
from datetime import datetime, timezone
from typing import Any

from app.core.database import get_db_session
from app.core.security import decrypt_value

import structlog

logger = structlog.get_logger(__name__)


def _fetch_tables_postgres(config: dict[str, Any]) -> list[str]:
    """Fetch tables from a PostgreSQL destination."""
    import psycopg2

    conn = psycopg2.connect(
        host=config.get("host"),
        port=config.get("port", 5432),
        dbname=config.get("database"),
        user=config.get("user"),
        password=decrypt_value(config.get("password") or ""),
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_tables_snowflake(config: dict[str, Any]) -> list[str]:
    """Fetch tables from a Snowflake destination."""
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    conn_params: dict[str, Any] = {
        "user": config.get("user"),
        "account": config.get("account"),
        "role": config.get("role"),
        "warehouse": config.get("warehouse"),
        "database": config.get("database"),
        "schema": config.get("schema"),
        "client_session_keep_alive": False,
        "application": "Rosetta_ETL",
    }

    if config.get("private_key"):
        private_key_str = config["private_key"].strip().replace("\\n", "\n")
        passphrase = None
        if config.get("private_key_passphrase"):
            passphrase = decrypt_value(config["private_key_passphrase"]).encode()
        p_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=passphrase,
            backend=default_backend(),
        )
        conn_params["private_key"] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    elif config.get("password"):
        conn_params["password"] = decrypt_value(config["password"])

    ctx = snowflake.connector.connect(**conn_params)
    cs = ctx.cursor()
    try:
        cs.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME;
            """
        )
        return [row[0] for row in cs.fetchall()]
    finally:
        cs.close()
        ctx.close()


def execute_destination_table_list(destination_id: int) -> dict[str, Any]:
    """
    Fetch and persist the table list for a destination.

    1. Load destination config from config DB.
    2. Connect to the actual destination and fetch table names.
    3. Persist list_tables, total_tables, last_table_check_at back to config DB.

    Args:
        destination_id: Destination identifier.

    Returns:
        Dict with destination_id, total_tables, tables, error keys.
    """
    logger.info("Starting destination table list fetch", destination_id=destination_id)

    with get_db_session() as db:
        from sqlalchemy import text

        row = db.execute(
            text("SELECT id, type, config FROM destinations WHERE id = :id"),
            {"id": destination_id},
        ).fetchone()

        if row is None:
            raise ValueError(f"Destination {destination_id} not found")

        dest_type: str = row.type.upper()
        config: dict[str, Any] = row.config if isinstance(row.config, dict) else json.loads(row.config)

    # Fetch tables (outside DB session to avoid long-held connections)
    try:
        if dest_type == "POSTGRES":
            tables = _fetch_tables_postgres(config)
        elif dest_type == "SNOWFLAKE":
            tables = _fetch_tables_snowflake(config)
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
    except Exception as exc:
        logger.error(
            "Failed to fetch tables from destination",
            destination_id=destination_id,
            dest_type=dest_type,
            error=str(exc),
        )
        return {
            "destination_id": destination_id,
            "total_tables": 0,
            "tables": [],
            "error": str(exc),
        }

    # Persist results back to config DB
    now = datetime.now(timezone.utc)
    with get_db_session() as db:
        from sqlalchemy import text

        db.execute(
            text(
                """
                UPDATE destinations
                SET list_tables = :list_tables,
                    total_tables = :total_tables,
                    last_table_check_at = :last_table_check_at,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                "id": destination_id,
                "list_tables": json.dumps(tables),
                "total_tables": len(tables),
                "last_table_check_at": now,
                "updated_at": now,
            },
        )

    # Cache in Redis so Flow Task input nodes see fresh data immediately
    try:
        from app.core.redis_client import RedisClient

        redis_client = RedisClient.get_instance()
        if redis_client:
            cache_key = f"destination:{destination_id}:tables"
            redis_client.setex(cache_key, 600, json.dumps(tables))  # 10 min TTL
            logger.info(
                "Destination table list cached in Redis",
                destination_id=destination_id,
                cache_key=cache_key,
            )
    except Exception as e:
        logger.warning(
            "Failed to cache destination tables in Redis",
            destination_id=destination_id,
            error=str(e),
        )

    logger.info(
        "Destination table list updated",
        destination_id=destination_id,
        total_tables=len(tables),
    )

    return {
        "destination_id": destination_id,
        "total_tables": len(tables),
        "tables": tables,
        "error": None,
    }
