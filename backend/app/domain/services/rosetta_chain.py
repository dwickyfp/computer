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
from app.domain.models.destination import Destination
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

        # Auto-create a linked ROSETTA destination so it appears in the
        # pipeline "Add Destination" list without manual setup.
        self._create_linked_destination(client, encrypted_key)

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

        # Sync the linked destination with the updated connection details
        self._sync_linked_destination(client)

        return ChainClientResponse.from_orm(client)

    def delete_client(self, client_id: int) -> None:
        """Remove a remote Rosetta instance (cascades to tables)."""
        # Delete the linked ROSETTA destination first so pipelines aren't
        # left with a dangling destination reference.
        self._delete_linked_destination(client_id)
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

    # ─── Linked Destination Helpers ────────────────────────────────────────

    def _destination_name(self, client_name: str) -> str:
        """Canonical name for the auto-created ROSETTA destination."""
        return f"chain-{client_name}"

    def _create_linked_destination(
        self, client: RosettaChainClient, encrypted_key: str
    ) -> None:
        """
        Create a ROSETTA Destination record linked to this chain client.

        The destination appears automatically in the pipeline
        'Add Destination' modal without any extra manual step.
        """
        try:
            dest_name = self._destination_name(client.name)
            # Guard: don't create duplicates (e.g. if retried)
            existing = (
                self.db.query(Destination).filter_by(chain_client_id=client.id).first()
            )
            if existing:
                return

            dest = Destination(
                name=dest_name,
                type="ROSETTA",
                config={
                    "url": client.url,
                    "port": client.port,
                    "chain_key": encrypted_key,
                },
                list_tables=[],
                total_tables=0,
                chain_client_id=client.id,
            )
            self.db.add(dest)
            self.db.commit()
            logger.info(
                f"Auto-created ROSETTA destination '{dest_name}' "
                f"for chain client '{client.name}'"
            )
        except Exception as e:
            self.db.rollback()
            logger.warning(
                f"Could not auto-create destination for chain client "
                f"'{client.name}': {e}"
            )

    def _sync_linked_destination(self, client: RosettaChainClient) -> None:
        """Keep the linked ROSETTA destination in sync with the chain client."""
        try:
            dest = (
                self.db.query(Destination).filter_by(chain_client_id=client.id).first()
            )
            if not dest:
                # Destination doesn't exist yet — create it now (handles clients
                # registered before this feature was added).
                self._create_linked_destination(client, client.chain_key)
                return

            # Sync name, URL and port; keep chain_key unchanged (already encrypted)
            new_name = self._destination_name(client.name)
            dest.name = new_name
            dest.config = {
                **dest.config,
                "url": client.url,
                "port": client.port,
            }
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.warning(
                f"Could not sync destination for chain client " f"'{client.name}': {e}"
            )

    def _delete_linked_destination(self, client_id: int) -> None:
        """Delete the ROSETTA destination linked to this chain client."""
        try:
            dest = (
                self.db.query(Destination).filter_by(chain_client_id=client_id).first()
            )
            if dest:
                self.db.delete(dest)
                self.db.commit()
                logger.info(
                    f"Deleted linked ROSETTA destination '{dest.name}' "
                    f"for chain client id={client_id}"
                )
        except Exception as e:
            self.db.rollback()
            logger.warning(
                f"Could not delete destination for chain client " f"id={client_id}: {e}"
            )

    def sync_all_destinations(self) -> int:
        """
        Ensure every chain client has a linked ROSETTA destination.

        Idempotent — safe to call repeatedly. Returns the number of
        destinations newly created.
        """
        clients = self._client_repo.get_all()
        created = 0
        for client in clients:
            existing = (
                self.db.query(Destination).filter_by(chain_client_id=client.id).first()
            )
            if not existing:
                self._create_linked_destination(client, client.chain_key)
                created += 1
        return created
