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
    RosettaChainDatabaseRepository,
)
from app.domain.schemas.rosetta_chain import (
    ChainClientCreate,
    ChainClientResponse,
    ChainClientTestResponse,
    ChainClientUpdate,
    ChainKeyResponse,
    ChainTableResponse,
    RosettaChainDatabaseResponse,
)

logger = get_logger(__name__)


class RosettaChainService:
    """Service for managing Rosetta Chain connections."""

    def __init__(self, db: Session):
        self.db = db
        self._config_repo = RosettaChainConfigRepository(db)
        self._client_repo = RosettaChainClientRepository(db)
        self._table_repo = RosettaChainTableRepository(db)
        self._database_repo = RosettaChainDatabaseRepository(db)

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
        raw_key = f"sk_rst_{secrets.token_urlsafe(32)}"
        encrypted_key = encrypt_value(raw_key)
        self._config_repo.upsert(chain_key=encrypted_key, is_active=True)

        # IMPORTANT: Commit to DB BEFORE notifying the compute process.
        # Otherwise the compute re-reads the OLD key because the
        # transaction hasn't been committed yet.
        self.db.commit()

        # Notify the local compute process to invalidate its cached key
        # so incoming requests are validated against the new key immediately.
        self._invalidate_compute_key_cache()

        return raw_key

    def _invalidate_compute_key_cache(self) -> None:
        """Tell the local compute node to drop its cached chain key."""
        try:
            from app.core.config import get_settings

            settings = get_settings()
            url = f"{settings.compute_node_url.rstrip('/')}/chain/invalidate-key-cache"
            resp = httpx.post(url, timeout=5.0)
            if resp.status_code == 200:
                logger.info("Compute key cache invalidated successfully")
            else:
                logger.warning(
                    f"Compute key cache invalidation returned {resp.status_code}"
                )
        except Exception as e:
            logger.warning(
                f"Could not notify compute to invalidate key cache: {e}. "
                "The cache will expire automatically within 60 seconds."
            )

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
            source_chain_id=data.source_chain_id or None,
        )

        # Commit client first so _create_linked_destination's rollback
        # (on failure) doesn't undo the client record.
        self.db.commit()

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
            raw_preview = update_data["chain_key"]
            logger.info(
                f"Storing new chain_key for client id={client_id}: "
                f"first 8 chars='{raw_preview[:8]}...', length={len(raw_preview)}"
            )
            update_data["chain_key"] = encrypt_value(raw_preview)

        client = self._client_repo.update(client_id, **update_data)

        # IMPORTANT: commit the client update FIRST so that a failure
        # in _sync_linked_destination (which does its own rollback on
        # error) does NOT silently roll back the chain_key change.
        self.db.commit()

        logger.info(
            f"Chain client '{client.name}' (id={client_id}) updated. "
            f"chain_key {'CHANGED' if 'chain_key' in update_data else 'unchanged'}."
        )

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

        # ── Sanity check: detect obviously wrong keys ───────────────
        import re

        _ip_like = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", raw_key)
        if _ip_like:
            logger.error(
                f"Chain key for client '{client.name}' (id={client_id}) "
                f"decrypted to an IP address ({raw_key[:12]}...). "
                f"This usually means the URL was accidentally saved in "
                f"the chain_key field. Please update the client with the "
                f"correct chain key from the remote Rosetta instance."
            )
            return ChainClientTestResponse(
                success=False,
                message=(
                    f"The stored chain key appears to be an IP address "
                    f"({raw_key[:12]}...), not a valid chain key. "
                    f"Please update this client with the correct key "
                    f"from the remote Rosetta's Chain Key page."
                ),
                latency_ms=None,
            )

        logger.debug(
            f"Testing connection to '{client.name}' — key first 8 chars: "
            f"{raw_key[:8]}..., key length: {len(raw_key)}"
        )

        url = self._build_client_url(client, "/chain/health")
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
                elif resp.status_code == 401:
                    return ChainClientTestResponse(
                        success=False,
                        message=(
                            f"Authentication failed (401). The chain key "
                            f"stored for this client does not match the "
                            f"remote instance's key. Regenerate or re-enter "
                            f"the key. (sent key first 8: {raw_key[:8]}...)"
                        ),
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

    def get_client_tables_by_database(
        self, client_id: int, database_id: int
    ) -> list[ChainTableResponse]:
        """Get tables for a specific database on a chain client."""
        tables = self._table_repo.get_by_database(client_id, database_id)
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

        url = self._build_client_url(client, "/chain/tables")

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

                # Get the client's current databases so we can map database_name -> database_id
                client_dbs = self._database_repo.get_by_client(client_id)
                db_name_to_id = {db.name: db.id for db in client_dbs}

                # Upsert each remote table
                for table_info in remote_tables:
                    db_name = table_info.get("database_name")
                    database_id = db_name_to_id.get(db_name) if db_name else None

                    self._table_repo.upsert(
                        chain_client_id=client_id,
                        table_name=table_info["table_name"],
                        table_schema=table_info.get("schema_json", {}),
                        source_chain_id=table_info.get("source_chain_id"),
                        database_id=database_id,
                    )

                # Update last connected
                self._client_repo.update_last_connected(client_id)

        except Exception as e:
            logger.error(f"Failed to sync tables from {client.name}: {e}")

        return self.get_client_tables(client_id)

    # ─── Database Discovery ──────────────────────────────────────────────────

    def get_client_databases(
        self, client_id: int
    ) -> list[RosettaChainDatabaseResponse]:
        """Get databases available on a chain client."""
        databases = self._database_repo.get_by_client(client_id)
        return [RosettaChainDatabaseResponse.from_orm(d) for d in databases]

    def sync_client_databases(
        self, client_id: int
    ) -> list[RosettaChainDatabaseResponse]:
        """
        Fetch and sync database list from a remote Rosetta instance.

        Calls the remote /chain/databases endpoint and upserts the results.
        """
        client = self._client_repo.get_by_id(client_id)

        try:
            raw_key = decrypt_value(client.chain_key)
        except Exception:
            logger.error(f"Failed to decrypt chain key for client {client_id}")
            return []

        url = self._build_client_url(client, "/chain/databases")

        try:
            with httpx.Client(timeout=15.0) as http:
                resp = http.get(url, headers={"X-Chain-Key": raw_key})

                if resp.status_code != 200:
                    logger.error(
                        f"Failed to fetch databases from {client.name}: "
                        f"status {resp.status_code}"
                    )
                    return self.get_client_databases(client_id)

                remote_databases = resp.json()

                # Upsert each remote database
                for db_info in remote_databases:
                    self._database_repo.upsert(
                        chain_client_id=client_id,
                        name=db_info["name"],
                    )

                # Remove stale local databases that no longer exist on the remote chain
                remote_names = {db_info["name"] for db_info in remote_databases}
                existing_dbs = self._database_repo.get_by_client(client_id)
                for db in existing_dbs:
                    if db.name not in remote_names:
                        logger.info(
                            f"Removing stale database '{db.name}' "
                            f"for client id={client_id} (no longer on remote)"
                        )
                        self.db.delete(db)
                self.db.flush()

                self._client_repo.update_last_connected(client_id)

        except Exception as e:
            logger.error(f"Failed to sync databases from {client.name}: {e}")

        return self.get_client_databases(client_id)

    def register_catalog_table(self, client_id: int, payload: dict) -> dict:
        """
        Register a table schema to a remote Rosetta instance's catalog.

        Acts as a proxy: Rosetta A -> Rosetta B.
        """
        client = self._client_repo.get_by_id(client_id)

        try:
            raw_key = decrypt_value(client.chain_key)
        except Exception:
            raise Exception("Failed to decrypt chain key")

        url = self._build_client_url(client, "/chain/schema")

        # Strip any local chain_client_id — it references this instance's DB
        # and is meaningless in the remote DB (causes FK violation).
        # Use the client name as source_chain_id so the remote can identify us.
        forwarded_payload = {k: v for k, v in payload.items() if k != "chain_client_id"}
        forwarded_payload.setdefault("source_chain_id", client.name)

        with httpx.Client(timeout=10.0) as http:
            resp = http.post(
                url, headers={"X-Chain-Key": raw_key}, json=forwarded_payload
            )

            if resp.status_code != 200:
                logger.error(
                    f"Catalog registration failed on {client.name}: {resp.text}"
                )
                raise Exception(f"Remote registration failed: {resp.status_code}")

            return resp.json()

    # ─── Linked Destination Helpers ────────────────────────────────────────

    def _build_client_url(self, client: RosettaChainClient, path: str) -> str:
        """Sanitize client URL and build full endpoints URL."""
        base_url = client.url.replace("http://", "").replace("https://", "").strip("/")
        if ":" in base_url:
            base_url = base_url.split(":")[0]
        return f"http://{base_url}:{client.port}{path}"

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
        """Keep the linked ROSETTA destination in sync with the chain client.

        This runs in its own implicit transaction (the caller already
        committed the client update).  A failure here is logged but
        never rolls back the client record itself.
        """
        try:
            dest = (
                self.db.query(Destination).filter_by(chain_client_id=client.id).first()
            )
            if not dest:
                # Destination doesn't exist yet — create it now (handles clients
                # registered before this feature was added).
                self._create_linked_destination(client, client.chain_key)
                return

            # Sync name, URL, port **and** chain_key so that key rotations
            # propagate to the compute engine without a manual restart.
            new_name = self._destination_name(client.name)
            dest.name = new_name
            dest.config = {
                **(dest.config or {}),
                "url": client.url,
                "port": client.port,
                "chain_key": client.chain_key,  # already encrypted
            }
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.warning(
                f"Could not sync destination for chain client '{client.name}': {e}. "
                f"The client record itself was already saved."
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
