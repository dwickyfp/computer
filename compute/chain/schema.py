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


_PG_TYPE_MAP = {
    "int2": "SMALLINT",
    "int4": "INTEGER",
    "int8": "BIGINT",
    "float4": "REAL",
    "float8": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "varchar": "CHARACTER VARYING",
    "bpchar": "CHARACTER",
    "timestamptz": "TIMESTAMP WITH TIME ZONE",
    "timestamp": "TIMESTAMP WITHOUT TIME ZONE",
    "jsonb": "JSONB",
    "json": "JSON",
    "uuid": "UUID",
    "text": "TEXT",
    "bytea": "BYTEA",
    "numeric": "NUMERIC",
    "date": "DATE",
    "time": "TIME WITHOUT TIME ZONE",
    "timetz": "TIME WITH TIME ZONE",
}


def _resolve_pg_type(raw_type: str) -> str:
    """Resolve a raw PG/Debezium type token to an uppercase canonical PG type."""
    t = (raw_type or "TEXT").strip()
    # Already upper-case canonical (e.g. "CHARACTER VARYING") — keep as-is
    if " " in t:
        return t.upper()
    lower = t.lower()
    return _PG_TYPE_MAP.get(lower, t.upper())


def normalize_schema_json(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize any incoming schema dict into the schema_monitor-compatible
    format stored in ``table_metadata_list.schema_table``.

    Handles three common input shapes:

    1. **Already normalized** — values are dicts containing ``column_name``.
       Passed through with type resolution.

    2. **Minimal inferred** — ``{col: {"type": "TEXT"}}`` produced by
       ``chain_engine._register_chain_table()``.  Maps ``type`` →
       ``real_data_type`` / ``data_type``.

    3. **Debezium struct schema** — ``{"type": "struct", "fields": [...]}``.  
       Each field has ``field`` (name) and ``type`` (Debezium logical type).

    Output format per column::

        {
            "column_name": str,
            "real_data_type": str,   # uppercase canonical PG type
            "data_type": str,
            "is_nullable": bool,
            "is_primary_key": bool,
            "has_default": bool,
            "default_value": None,
            "numeric_precision": None,
            "numeric_scale": None,
        }
    """
    if not raw or not isinstance(raw, dict):
        return {}

    # ── Debezium struct: {"type": "struct", "fields": [...]}
    if raw.get("type") == "struct" and "fields" in raw:
        result: dict[str, Any] = {}
        for field in raw["fields"]:
            col_name = field.get("field") or field.get("name") or ""
            if not col_name:
                continue
            raw_type = field.get("type", "TEXT")
            pg_type = _resolve_pg_type(raw_type)
            result[col_name] = {
                "column_name": col_name,
                "real_data_type": pg_type,
                "data_type": pg_type,
                "is_nullable": field.get("optional", True),
                "is_primary_key": False,
                "has_default": field.get("default") is not None,
                "default_value": str(field["default"]) if field.get("default") is not None else None,
                "numeric_precision": None,
                "numeric_scale": None,
            }
        return result

    # ── Dict of column entries
    result = {}
    for key, val in raw.items():
        if not isinstance(val, dict):
            # Bare string value or unexpected — skip
            continue

        # Already normalized: has column_name key
        if "column_name" in val:
            col_name = val["column_name"] or key
            raw_type = val.get("real_data_type") or val.get("data_type") or val.get("type", "TEXT")
            pg_type = _resolve_pg_type(raw_type)
            result[col_name] = {
                "column_name": col_name,
                "real_data_type": pg_type,
                "data_type": pg_type,
                "is_nullable": val.get("is_nullable", True),
                "is_primary_key": val.get("is_primary_key", False),
                "has_default": val.get("has_default", False),
                "default_value": val.get("default_value"),
                "numeric_precision": val.get("numeric_precision"),
                "numeric_scale": val.get("numeric_scale"),
            }
            continue

        # Minimal inferred: {col: {"type": "TEXT"}} or {col: {"type": "...", "is_primary_key": bool}}
        col_name = key
        raw_type = val.get("type") or val.get("data_type") or val.get("real_data_type", "TEXT")
        pg_type = _resolve_pg_type(raw_type)
        result[col_name] = {
            "column_name": col_name,
            "real_data_type": pg_type,
            "data_type": pg_type,
            "is_nullable": val.get("is_nullable", True),
            "is_primary_key": val.get("is_primary_key", False),
            "has_default": False,
            "default_value": None,
            "numeric_precision": None,
            "numeric_scale": None,
        }

    return result


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
                # Normalize before persisting so every row is in the standard format
                schema_json = normalize_schema_json(schema_json)
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
                    #
                    # Before inserting a NULL row, check if a direct-linked row
                    # already exists for this table (created by _register_chain_table
                    # in the compute engine).  If it does, update that row instead to
                    # avoid duplicate entries showing in the Data Explorer.
                    direct_client_id = None
                    if source_chain_id and source_chain_id.isdigit():
                        direct_client_id = int(source_chain_id)
                        cursor.execute(
                            "SELECT id FROM rosetta_chain_tables "
                            "WHERE chain_client_id = %s AND table_name = %s",
                            (direct_client_id, table_name),
                        )
                        direct_existing = cursor.fetchone()
                        if direct_existing:
                            cursor.execute(
                                "UPDATE rosetta_chain_tables "
                                "SET schema_json = %s::jsonb, "
                                "    last_synced_at = NOW(), "
                                "    updated_at = NOW() "
                                "WHERE chain_client_id = %s AND table_name = %s",
                                (schema_str, direct_client_id, table_name),
                            )
                            logger.info(
                                f"Updated direct-linked schema for chain table {table_name} "
                                f"via cross-instance registration (source {source_chain_id})"
                            )
                            conn.commit()
                            return True

                    # No direct row — proceed with NULL chain_client_id row
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
        List all outbound catalog tables along with their logical database name.
        
        This exposes the local node's data catalog to remote Rosetta instances
        so they can discover what datasets are available to be streamed.
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # We return the local catalog_tables (outbound available data)
                # rather than rosetta_chain_tables (inbound received data).
                # We join catalog_databases to provide the database_name.
                cursor.execute(
                    "SELECT t.id, d.name AS database_name, t.table_name, t.schema_json, t.created_at "
                    "FROM catalog_tables t "
                    "JOIN catalog_databases d ON t.database_id = d.id "
                    "ORDER BY d.name, t.table_name"
                )

                rows = cursor.fetchall()
                tables = []
                for row in rows:
                    schema = row[3]
                    if isinstance(schema, str):
                        try:
                            schema = json.loads(schema)
                        except json.JSONDecodeError:
                            schema = {}
                    tables.append(
                        {
                            "id": row[0],
                            "database_name": row[1],
                            "table_name": row[2],
                            "schema_json": schema or {},
                            "created_at": (row[4].isoformat() if row[4] else None),
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
