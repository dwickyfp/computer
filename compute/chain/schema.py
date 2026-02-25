"""
Chain schema manager for table schema discovery and storage.

Manages schema definitions for virtual chain tables, enabling
schema negotiation between Rosetta instances.
"""

import json
import logging
from typing import Any, Optional

from core.database import get_db_connection, return_db_connection

logger = logging.getLogger(__name__)


class ChainSchemaManager:
    """
    Manages schema definitions for chain tables.

    Reads/writes to the rosetta_chain_tables table for schema
    discovery and auto-creation on the receiving side.
    """

    def get_table_schema(
        self, table_name: str, chain_client_id: Optional[int] = None
    ) -> Optional[dict[str, Any]]:
        """
        Get schema for a chain table.

        Args:
            table_name: Table name to look up
            chain_client_id: Optional client ID to scope the lookup

        Returns:
            Schema dict or None if not found
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                if chain_client_id:
                    cursor.execute(
                        "SELECT schema_json FROM rosetta_chain_tables "
                        "WHERE chain_client_id = %s AND table_name = %s",
                        (chain_client_id, table_name),
                    )
                else:
                    cursor.execute(
                        "SELECT schema_json FROM rosetta_chain_tables "
                        "WHERE table_name = %s LIMIT 1",
                        (table_name,),
                    )
                row = cursor.fetchone()
                if row:
                    schema = row[0]
                    if isinstance(schema, str):
                        return json.loads(schema)
                    return schema
                return None
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None
        finally:
            if conn:
                return_db_connection(conn)

    def upsert_table_schema(
        self,
        table_name: str,
        schema_json: dict[str, Any],
        chain_client_id: Optional[int] = None,
        source_chain_id: Optional[str] = None,
    ) -> bool:
        """
        Create or update a chain table schema.

        Auto-creates the entry if it doesn't exist (auto-accept mode).

        When chain_client_id is provided (local pipeline), lookups are scoped
        by (chain_client_id, table_name).  When it is None (cross-instance
        registration from a remote Rosetta), lookups are scoped by
        (source_chain_id, table_name) instead, avoiding FK violations.

        Returns:
            True if successful
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                schema_str = json.dumps(schema_json)

                if chain_client_id is not None:
                    # ── Local pipeline path ──────────────────────────────
                    cursor.execute(
                        "SELECT id FROM rosetta_chain_tables "
                        "WHERE chain_client_id = %s AND table_name = %s",
                        (chain_client_id, table_name),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        cursor.execute(
                            "UPDATE rosetta_chain_tables "
                            "SET schema_json = %s::jsonb, "
                            "    source_chain_id = COALESCE(%s, source_chain_id), "
                            "    last_synced_at = NOW(), "
                            "    updated_at = NOW() "
                            "WHERE chain_client_id = %s AND table_name = %s",
                            (schema_str, source_chain_id, chain_client_id, table_name),
                        )
                    else:
                        cursor.execute(
                            "INSERT INTO rosetta_chain_tables "
                            "(chain_client_id, table_name, schema_json, "
                            " source_chain_id, last_synced_at) "
                            "VALUES (%s, %s, %s::jsonb, %s, NOW())",
                            (chain_client_id, table_name, schema_str, source_chain_id),
                        )
                    logger.info(
                        f"Upserted schema for chain table {table_name} "
                        f"(client {chain_client_id})"
                    )
                else:
                    # ── Cross-instance registration path ─────────────────
                    # chain_client_id is unknown (remote sender's local ID is
                    # meaningless in this DB).  Use source_chain_id as the key.
                    cursor.execute(
                        "SELECT id FROM rosetta_chain_tables "
                        "WHERE chain_client_id IS NULL AND table_name = %s "
                        "  AND source_chain_id = %s",
                        (table_name, source_chain_id),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        cursor.execute(
                            "UPDATE rosetta_chain_tables "
                            "SET schema_json = %s::jsonb, "
                            "    last_synced_at = NOW(), "
                            "    updated_at = NOW() "
                            "WHERE chain_client_id IS NULL "
                            "  AND table_name = %s AND source_chain_id = %s",
                            (schema_str, table_name, source_chain_id),
                        )
                    else:
                        cursor.execute(
                            "INSERT INTO rosetta_chain_tables "
                            "(chain_client_id, table_name, schema_json, "
                            " source_chain_id, last_synced_at) "
                            "VALUES (NULL, %s, %s::jsonb, %s, NOW())",
                            (table_name, schema_str, source_chain_id),
                        )
                    logger.info(
                        f"Upserted cross-instance schema for table {table_name} "
                        f"(source {source_chain_id})"
                    )

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"Failed to upsert schema for chain table {table_name}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                return_db_connection(conn)

    def list_tables(
        self, chain_client_id: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """
        List all chain tables, optionally filtered by client ID.

        Returns list of table info dicts.
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                if chain_client_id:
                    cursor.execute(
                        "SELECT id, chain_client_id, table_name, schema_json, "
                        "       source_chain_id, record_count, last_synced_at "
                        "FROM rosetta_chain_tables "
                        "WHERE chain_client_id = %s "
                        "ORDER BY table_name",
                        (chain_client_id,),
                    )
                else:
                    cursor.execute(
                        "SELECT id, chain_client_id, table_name, schema_json, "
                        "       source_chain_id, record_count, last_synced_at "
                        "FROM rosetta_chain_tables "
                        "ORDER BY table_name"
                    )

                rows = cursor.fetchall()
                tables = []
                for row in rows:
                    schema = row[3]
                    if isinstance(schema, str):
                        schema = json.loads(schema)
                    tables.append(
                        {
                            "id": row[0],
                            "chain_client_id": row[1],
                            "table_name": row[2],
                            "schema_json": schema,
                            "source_chain_id": row[4],
                            "record_count": row[5] or 0,
                            "last_synced_at": (row[6].isoformat() if row[6] else None),
                        }
                    )
                return tables

        except Exception as e:
            logger.error(f"Failed to list chain tables: {e}")
            return []
        finally:
            if conn:
                return_db_connection(conn)

    def list_databases(self) -> list[dict[str, Any]]:
        """
        List all logical databases from the local catalog.

        Used to serve remote Rosetta instances with available destinations.
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name, description, created_at "
                    "FROM catalog_databases "
                    "ORDER BY name"
                )

                rows = cursor.fetchall()
                databases = []
                for row in rows:
                    databases.append(
                        {
                            "id": row[0],
                            "name": row[1],
                            "description": row[2] or "",
                            "created_at": (row[3].isoformat() if row[3] else None),
                        }
                    )
                return databases

        except Exception as e:
            logger.error(f"Failed to list catalog databases: {e}")
            return []
        finally:
            if conn:
                return_db_connection(conn)

    def update_record_count(
        self, chain_client_id: int, table_name: str, count: int
    ) -> None:
        """Update the record count for a chain table."""
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE rosetta_chain_tables "
                    "SET record_count = %s, updated_at = NOW() "
                    "WHERE chain_client_id = %s AND table_name = %s",
                    (count, chain_client_id, table_name),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to update record count for {table_name}: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                return_db_connection(conn)
