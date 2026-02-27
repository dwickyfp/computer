"""
Rosetta Chain service - business logic for inter-instance streaming.

Manages chain key generation, client registration, connectivity testing,
and table discovery between Rosetta instances.
"""

import time
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.rosetta_chain import (
    RosettaChainClient,
    RosettaChainTable,
)
from app.domain.models.destination import Destination
from app.domain.repositories.rosetta_chain import (
    RosettaChainClientRepository,
    RosettaChainTableRepository,
    RosettaChainDatabaseRepository,
)
from app.domain.schemas.rosetta_chain import (
    ChainClientCreate,
    ChainClientResponse,
    ChainClientTestResponse,
    ChainClientUpdate,
    ChainTableResponse,
    RosettaChainDatabaseResponse,
)

logger = get_logger(__name__)


class RosettaChainService:
    """Service for managing Rosetta Chain connections."""

    def __init__(self, db: Session):
        self.db = db
        self._client_repo = RosettaChainClientRepository(db)
        self._table_repo = RosettaChainTableRepository(db)
        self._database_repo = RosettaChainDatabaseRepository(db)

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
        client = self._client_repo.create(
            name=data.name,
            url=data.url,
            port=data.port,
            is_active=True,
            source_chain_id=data.source_chain_id or None,
        )

        # Commit client first so _create_linked_destination's rollback
        # (on failure) doesn't undo the client record.
        self.db.commit()

        # Auto-create a linked ROSETTA destination so it appears in the
        # pipeline "Add Destination" list without manual setup.
        self._create_linked_destination(client)

        return ChainClientResponse.from_orm(client)

    def update_client(
        self, client_id: int, data: ChainClientUpdate
    ) -> ChainClientResponse:
        """Update a remote Rosetta instance registration."""
        update_data = data.dict(exclude_unset=True)

        client = self._client_repo.update(client_id, **update_data)

        # Commit before syncing destination.
        self.db.commit()

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

        url = self._build_client_url(client, "/chain/health")
        start = time.monotonic()

        try:
            with httpx.Client(timeout=10.0) as http:
                resp = http.get(url)
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

        url = self._build_client_url(client, "/chain/tables")

        try:
            with httpx.Client(timeout=15.0) as http:
                resp = http.get(url)

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

        url = self._build_client_url(client, "/chain/databases")

        try:
            with httpx.Client(timeout=15.0) as http:
                resp = http.get(url)

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

        url = self._build_client_url(client, "/chain/schema")

        # Strip any local chain_client_id — it references this instance's DB
        # and is meaningless in the remote DB (causes FK violation).
        # Use the client name as source_chain_id so the remote can identify us.
        forwarded_payload = {k: v for k, v in payload.items() if k != "chain_client_id"}
        forwarded_payload.setdefault("source_chain_id", client.name)

        with httpx.Client(timeout=10.0) as http:
            resp = http.post(url, json=forwarded_payload)

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

    def _create_linked_destination(self, client: RosettaChainClient) -> None:
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
                # Destination doesn't exist yet — create it now.
                self._create_linked_destination(client)
                return

            # Sync name, URL and port.
            new_name = self._destination_name(client.name)
            dest.name = new_name
            dest.config = {
                **(dest.config or {}),
                "url": client.url,
                "port": client.port,
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
                self._create_linked_destination(client)
                created += 1
        return created
