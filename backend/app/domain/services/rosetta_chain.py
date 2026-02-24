"""
Rosetta Chain service - business logic for inter-instance streaming.

Manages chain key generation, client registration, connectivity testing,
and table discovery between Rosetta instances.
"""

import secrets
import time
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.core.security import encrypt_value, decrypt_value
from app.domain.models.rosetta_chain import (
    RosettaChainClient,
    RosettaChainConfig,
    RosettaChainTable,
)
from app.domain.repositories.rosetta_chain import (
    RosettaChainClientRepository,
    RosettaChainConfigRepository,
    RosettaChainTableRepository,
)
from app.domain.schemas.rosetta_chain import (
    ChainClientCreate,
    ChainClientResponse,
    ChainClientTestResponse,
    ChainClientUpdate,
    ChainKeyResponse,
    ChainTableResponse,
)

logger = get_logger(__name__)


class RosettaChainService:
    """Service for managing Rosetta Chain connections."""

    def __init__(self, db: Session):
        self.db = db
        self._config_repo = RosettaChainConfigRepository(db)
        self._client_repo = RosettaChainClientRepository(db)
        self._table_repo = RosettaChainTableRepository(db)

    # ─── Chain Key Management ───────────────────────────────────────────────

    def get_chain_key(self) -> Optional[ChainKeyResponse]:
        """Get the current chain key (masked)."""
        config = self._config_repo.get()
        if not config:
            return None

        # Decrypt and mask the key
        try:
            raw_key = decrypt_value(config.chain_key)
            masked = f"{'*' * max(0, len(raw_key) - 8)}{raw_key[-8:]}"
        except Exception:
            masked = "********"

        return ChainKeyResponse(
            chain_key_masked=masked,
            is_active=config.is_active,
            created_at=config.created_at,
        )

    def get_chain_key_raw(self) -> Optional[str]:
        """Get the raw (decrypted) chain key for display once after generation."""
        config = self._config_repo.get()
        if not config:
            return None
        try:
            return decrypt_value(config.chain_key)
        except Exception:
            return None

    def generate_chain_key(self) -> str:
        """Generate a new chain key. Returns the raw key (show once)."""
        raw_key = secrets.token_urlsafe(32)
        encrypted_key = encrypt_value(raw_key)
        self._config_repo.upsert(chain_key=encrypted_key, is_active=True)
        return raw_key

    def set_chain_active(self, is_active: bool) -> Optional[ChainKeyResponse]:
        """Toggle chain ingestion active state."""
        config = self._config_repo.set_active(is_active)
        if not config:
            return None
        return self.get_chain_key()

    # ─── Client Management ──────────────────────────────────────────────────

    def get_clients(self) -> list[ChainClientResponse]:
        """Get all registered chain clients."""
        clients = self._client_repo.get_all()
        return [ChainClientResponse.from_orm(c) for c in clients]

    def get_client(self, client_id: int) -> ChainClientResponse:
        """Get a specific chain client."""
        client = self._client_repo.get_by_id(client_id)
        return ChainClientResponse.from_orm(client)

    def create_client(self, data: ChainClientCreate) -> ChainClientResponse:
        """Register a new remote Rosetta instance."""
        # Encrypt the chain key before storing
        encrypted_key = encrypt_value(data.chain_key)

        client = self._client_repo.create(
            name=data.name,
            url=data.url,
            port=data.port,
            chain_key=encrypted_key,
            is_active=True,
        )
        return ChainClientResponse.from_orm(client)

    def update_client(
        self, client_id: int, data: ChainClientUpdate
    ) -> ChainClientResponse:
        """Update a remote Rosetta instance registration."""
        update_data = data.dict(exclude_unset=True)

        # If chain_key is being updated, encrypt it
        if "chain_key" in update_data and update_data["chain_key"] is not None:
            update_data["chain_key"] = encrypt_value(update_data["chain_key"])

        client = self._client_repo.update(client_id, **update_data)
        return ChainClientResponse.from_orm(client)

    def delete_client(self, client_id: int) -> None:
        """Remove a remote Rosetta instance (cascades to tables)."""
        self._client_repo.delete(client_id)

    # ─── Connectivity Testing ───────────────────────────────────────────────

    def test_client_connection(self, client_id: int) -> ChainClientTestResponse:
        """Test connectivity to a remote Rosetta instance."""
        client = self._client_repo.get_by_id(client_id)

        try:
            raw_key = decrypt_value(client.chain_key)
        except Exception:
            return ChainClientTestResponse(
                success=False,
                message="Failed to decrypt chain key",
                latency_ms=None,
            )

        url = f"http://{client.url}:{client.port}/chain/health"
        start = time.monotonic()

        try:
            with httpx.Client(timeout=10.0) as http:
                resp = http.get(url, headers={"X-Chain-Key": raw_key})
                latency = (time.monotonic() - start) * 1000

                if resp.status_code == 200:
                    self._client_repo.update_last_connected(client_id)
                    return ChainClientTestResponse(
                        success=True,
                        message="Connection successful",
                        latency_ms=round(latency, 2),
                    )
                else:
                    return ChainClientTestResponse(
                        success=False,
                        message=f"Remote returned status {resp.status_code}",
                        latency_ms=round(latency, 2),
                    )
        except httpx.ConnectError:
            return ChainClientTestResponse(
                success=False,
                message=f"Cannot reach {client.url}:{client.port}",
                latency_ms=None,
            )
        except httpx.TimeoutException:
            return ChainClientTestResponse(
                success=False,
                message="Connection timed out",
                latency_ms=None,
            )
        except Exception as e:
            return ChainClientTestResponse(
                success=False,
                message=f"Connection failed: {str(e)}",
                latency_ms=None,
            )

    # ─── Table Discovery ────────────────────────────────────────────────────

    def get_client_tables(self, client_id: int) -> list[ChainTableResponse]:
        """Get tables available on a chain client."""
        tables = self._table_repo.get_by_client(client_id)
        return [ChainTableResponse.from_orm(t) for t in tables]

    def sync_client_tables(self, client_id: int) -> list[ChainTableResponse]:
        """
        Fetch and sync table list from a remote Rosetta instance.

        Calls the remote /chain/tables endpoint and upserts the results.
        """
        client = self._client_repo.get_by_id(client_id)

        try:
            raw_key = decrypt_value(client.chain_key)
        except Exception:
            logger.error(f"Failed to decrypt chain key for client {client_id}")
            return []

        url = f"http://{client.url}:{client.port}/chain/tables"

        try:
            with httpx.Client(timeout=15.0) as http:
                resp = http.get(url, headers={"X-Chain-Key": raw_key})

                if resp.status_code != 200:
                    logger.error(
                        f"Failed to fetch tables from {client.name}: "
                        f"status {resp.status_code}"
                    )
                    return self.get_client_tables(client_id)

                remote_tables = resp.json()

                # Upsert each remote table
                for table_info in remote_tables:
                    self._table_repo.upsert(
                        chain_client_id=client_id,
                        table_name=table_info["table_name"],
                        table_schema=table_info.get("schema_json", {}),
                        source_chain_id=table_info.get("source_chain_id"),
                    )

                # Update last connected
                self._client_repo.update_last_connected(client_id)

        except Exception as e:
            logger.error(f"Failed to sync tables from {client.name}: {e}")

        return self.get_client_tables(client_id)
