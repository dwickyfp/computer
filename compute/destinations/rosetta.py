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
        """Get remote Rosetta compute URL."""
        url = self._config.config["url"]
        return url.rstrip("/")

    @property
    def chain_key(self) -> str:
        """Get decrypted chain key."""
        encrypted = self._config.config["chain_key"]
        return decrypt_value(encrypted)

    def initialize(self) -> None:
        """Initialize HTTP client for remote Rosetta connection."""
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "X-Chain-Key": self.chain_key,
                "X-Chain-ID": self._chain_id,
            },
        )
        self._is_initialized = True
        self._logger.info(f"Rosetta destination initialized: {self.base_url}")

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

        # Build Arrow arrays — use string type for all value columns
        # The remote side will cast based on schema
        arrays = []
        field_names = []

        for col in all_columns:
            arrays.append(pa.array(columns[col], type=pa.string()))
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
            response = self._client.get(f"/chain/schema/{table_name}")
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
