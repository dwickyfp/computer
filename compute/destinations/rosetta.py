"""
Rosetta Chain destination — streams CDC data to a remote Rosetta instance.

Serializes CDCRecords to Arrow IPC and POSTs to the remote instance's
/chain/ingest endpoint. Schema is auto-synced via /chain/schema.
"""

import io
import json
import logging
from typing import Any, Optional

import httpx
import pyarrow as pa

from destinations.base import BaseDestination, CDCRecord
from core.models import Destination, PipelineDestinationTableSync
from core.exceptions import DestinationException
from core.security import decrypt_value

logger = logging.getLogger(__name__)


class RosettaDestination(BaseDestination):
    """
    Destination that streams CDC data to another Rosetta instance via Arrow IPC.

    Flow:
    1. Serialize CDCRecords to Arrow IPC RecordBatch
    2. POST to remote /chain/ingest with proper headers
    3. Auto-push schema to /chain/schema on first write per table
    """

    REQUIRED_CONFIG = ["url", "chain_key"]

    def __init__(self, config: Destination, chain_id: Optional[str] = None):
        """
        Initialize Rosetta destination.

        Args:
            config: Destination configuration (url, chain_key)
            chain_id: Unique chain identifier for stream naming
        """
        super().__init__(config)
        self._chain_id = chain_id or str(config.id)
        self._client: Optional[httpx.Client] = None
        self._synced_schemas: set[str] = set()
        self._cached_chain_key: Optional[str] = None
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate required configuration keys."""
        cfg = self._config.config
        missing = [k for k in self.REQUIRED_CONFIG if k not in cfg]
        if missing:
            raise DestinationException(
                f"Missing required Rosetta Chain config: {missing}",
                {"destination_id": self._config.id},
            )

    @property
    def base_url(self) -> str:
        """Get remote Rosetta compute URL including port."""
        url = self._config.config["url"].rstrip("/")
        port = self._config.config.get("port")
        if port:
            # Strip any existing port from the URL, then append the configured one
            # e.g. "http://172.16.122.180" + port 8001 → "http://172.16.122.180:8001"
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(url if "://" in url else f"http://{url}")
            netloc = parsed.hostname or parsed.netloc
            url = urlunparse(
                (
                    parsed.scheme or "http",
                    f"{netloc}:{port}",
                    parsed.path or "",
                    "",
                    "",
                    "",
                )
            )
        return url.rstrip("/")

    @property
    def chain_key(self) -> str:
        """Get decrypted chain key.

        Reads the key fresh from the destination config (or falls back to
        the rosetta_chain_clients table).  The result is cached in-memory
        so that we only hit the DB once per pipeline process lifetime, but
        ``refresh_chain_key()`` can force a re-read after a key rotation.
        """
        if self._cached_chain_key is not None:
            return self._cached_chain_key

        encrypted = self._config.config.get("chain_key", "")
        decrypted = decrypt_value(encrypted)

        # decrypt_value silently returns the original value on failure.
        # If the result still looks like a base64 blob (same as input),
        # try fetching the key from the chain client record instead.
        if decrypted == encrypted and encrypted:
            try:
                from core.database import get_db_connection, return_db_connection

                conn = get_db_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT c.chain_key FROM rosetta_chain_clients c "
                            "JOIN destinations d ON d.chain_client_id = c.id "
                            "WHERE d.id = %s",
                            (self._config.id,),
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            decrypted = decrypt_value(row[0])
                finally:
                    return_db_connection(conn)
            except Exception as e:
                self._logger.warning(
                    f"Could not read chain_key from chain_clients table: {e}"
                )

        self._cached_chain_key = decrypted
        return decrypted

    def refresh_chain_key(self) -> None:
        """Force the next ``chain_key`` access to re-read from the database."""
        self._cached_chain_key = None

    def initialize(self) -> None:
        """Initialize HTTP client for remote Rosetta connection.

        Note: X-Chain-Key is NOT set as a default header here — it is
        injected per-request via ``_auth_headers()`` so that key
        rotations take effect without restarting the pipeline process.
        """
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "X-Chain-ID": self._chain_id,
            },
        )
        self._is_initialized = True
        self._logger.info(f"Rosetta destination initialized: {self.base_url}")

    def _auth_headers(self) -> dict[str, str]:
        """Return per-request auth headers with the current chain key."""
        return {"X-Chain-Key": self.chain_key}

    def write_batch(
        self,
        records: list[CDCRecord],
        table_sync: PipelineDestinationTableSync,
    ) -> int:
        """
        Write a batch of CDC records to the remote Rosetta instance.

        Serializes records to Arrow IPC and POSTs to /chain/ingest.

        Args:
            records: CDC records to send
            table_sync: Table sync configuration

        Returns:
            Number of records successfully sent
        """
        if not records:
            return 0

        if not self._client:
            raise DestinationException(
                "Rosetta destination not initialized",
                {"destination_id": self._config.id},
            )

        table_name = records[0].table_name

        # Ensure schema is synced for this table
        if table_name not in self._synced_schemas:
            if records[0].schema:
                self._push_schema(table_name, records[0].schema)
            self._synced_schemas.add(table_name)

        # Group records by operation type for efficient streaming
        ipc_buffer = self._records_to_arrow_ipc(records)

        try:
            response = self._client.post(
                "/chain/ingest",
                content=ipc_buffer,
                headers={
                    **self._auth_headers(),
                    "Content-Type": "application/vnd.apache.arrow.stream",
                    "X-Table-Name": table_name,
                    "X-Operation-Type": "mixed",
                },
            )
            response.raise_for_status()

            result = response.json()
            ingested = result.get("records_ingested", len(records))
            self._logger.debug(
                f"Sent {ingested} records for {table_name} to {self.base_url}"
            )
            return ingested

        except httpx.HTTPStatusError as e:
            # If we get 401, the key may have been rotated — clear the
            # cached key so the next attempt re-reads from DB.
            if e.response.status_code == 401:
                self.refresh_chain_key()
            raise DestinationException(
                f"Remote Rosetta rejected data: {e.response.status_code} {e.response.text}",
                {"destination_id": self._config.id, "table": table_name},
            )
        except httpx.ConnectError as e:
            raise DestinationException(
                f"Cannot connect to remote Rosetta at {self.base_url}: {e}",
                {"destination_id": self._config.id},
            )
        except Exception as e:
            raise DestinationException(
                f"Failed to send data to remote Rosetta: {e}",
                {"destination_id": self._config.id, "table": table_name},
            )

    def _records_to_arrow_ipc(self, records: list[CDCRecord]) -> bytes:
        """
        Serialize CDC records to Arrow IPC stream format.

        Each record becomes a row with all value columns plus
        __operation and __key_json metadata columns.

        Args:
            records: CDC records to serialize

        Returns:
            Arrow IPC stream bytes
        """
        if not records:
            return b""

        # Collect all column names from values
        all_columns: set[str] = set()
        for r in records:
            if r.value:
                all_columns.update(r.value.keys())

        all_columns = sorted(all_columns)

        # Build column arrays
        columns: dict[str, list] = {col: [] for col in all_columns}
        operations: list[str] = []
        key_jsons: list[str] = []

        for r in records:
            operations.append(r.operation)
            key_jsons.append(json.dumps(r.key) if r.key else "{}")
            for col in all_columns:
                columns[col].append(r.value.get(col) if r.value else None)

        # Build Arrow arrays — use string type for all value columns.
        # Explicitly convert every non-None value to str so PyArrow doesn't
        # choke on bools, dicts, lists, Decimals, etc.
        arrays = []
        field_names = []

        for col in all_columns:
            str_values = [str(v) if v is not None else None for v in columns[col]]
            arrays.append(pa.array(str_values, type=pa.string()))
            field_names.append(col)

        # Add metadata columns
        arrays.append(pa.array(operations, type=pa.string()))
        field_names.append("__operation")
        arrays.append(pa.array(key_jsons, type=pa.string()))
        field_names.append("__key_json")

        batch = pa.RecordBatch.from_arrays(arrays, names=field_names)

        # Serialize to IPC stream
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()

        return sink.getvalue().to_pybytes()

    def _push_schema(self, table_name: str, schema: dict[str, Any]) -> None:
        """
        Push table schema to the remote Rosetta instance.

        Args:
            table_name: Table name
            schema: Debezium schema dict
        """
        if not self._client:
            return

        try:
            response = self._client.post(
                "/chain/schema",
                json={
                    "table_name": table_name,
                    "schema_json": schema,
                    "chain_client_id": self._config.id,
                    "source_chain_id": self._chain_id,
                },
                headers=self._auth_headers(),
            )
            if response.status_code == 200:
                self._logger.info(f"Schema synced for {table_name}")
            else:
                self._logger.warning(
                    f"Schema sync returned {response.status_code} for {table_name}"
                )
        except Exception as e:
            self._logger.warning(f"Failed to sync schema for {table_name}: {e}")

    def create_table_if_not_exists(
        self,
        table_name: str,
        schema: dict[str, Any],
    ) -> bool:
        """
        Ensure remote table schema exists by pushing schema.

        Args:
            table_name: Table name
            schema: Table schema dict

        Returns:
            True if schema was created/updated
        """
        if not self._client:
            return False

        try:
            # Check if schema already exists
            response = self._client.get(
                f"/chain/schema/{table_name}",
                headers=self._auth_headers(),
            )
            if response.status_code == 200:
                self._synced_schemas.add(table_name)
                return False  # Already exists

            # Push new schema
            self._push_schema(table_name, schema)
            self._synced_schemas.add(table_name)
            return True
        except Exception as e:
            self._logger.warning(f"Failed to check/create schema for {table_name}: {e}")
            return False

    def test_connection(self) -> bool:
        """
        Test connection to remote Rosetta instance.

        Returns:
            True if remote instance is reachable and chain is healthy
        """
        try:
            client = httpx.Client(
                base_url=self.base_url,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
            response = client.get(
                "/chain/health",
                headers={"X-Chain-Key": self.chain_key},
            )
            client.close()
            return response.status_code == 200
        except Exception:
            return False

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
        self._is_initialized = False
        self._synced_schemas.clear()
        self._logger.info("Rosetta destination closed")
