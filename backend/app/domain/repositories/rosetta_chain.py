"""
Rosetta Chain repository for chain configuration, clients, and tables.

Provides data access for inter-instance streaming configuration.
"""

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.rosetta_chain import (
    RosettaChainClient,
    RosettaChainConfig,
    RosettaChainTable,
    RosettaChainDatabase,
)
from app.domain.repositories.base import BaseRepository

logger = get_logger(__name__)


class RosettaChainConfigRepository:
    """Repository for chain instance configuration (single-row table)."""

    def __init__(self, db: Session):
        self.db = db

    def get(self) -> Optional[RosettaChainConfig]:
        """Get the chain config (there should be at most one row)."""
        stmt = select(RosettaChainConfig).limit(1)
        return self.db.execute(stmt).scalar_one_or_none()

    def upsert(self, chain_key: str, is_active: bool = True) -> RosettaChainConfig:
        """Create or update the chain config."""
        existing = self.get()
        if existing:
            existing.chain_key = chain_key
            existing.is_active = is_active
            existing.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
            self.db.flush()
            self.db.refresh(existing)
            return existing
        else:
            config = RosettaChainConfig(
                chain_key=chain_key,
                is_active=is_active,
            )
            self.db.add(config)
            self.db.flush()
            self.db.refresh(config)
            return config

    def set_active(self, is_active: bool) -> Optional[RosettaChainConfig]:
        """Toggle the active state."""
        existing = self.get()
        if existing:
            existing.is_active = is_active
            existing.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
            self.db.flush()
            self.db.refresh(existing)
        return existing


class RosettaChainClientRepository(BaseRepository[RosettaChainClient]):
    """Repository for remote Rosetta instance registrations."""

    def __init__(self, db: Session):
        super().__init__(RosettaChainClient, db)

    def get_active_clients(self) -> list[RosettaChainClient]:
        """Get all active chain clients."""
        stmt = (
            select(RosettaChainClient)
            .where(RosettaChainClient.is_active.is_(True))
            .order_by(RosettaChainClient.name)
        )
        return list(self.db.execute(stmt).scalars().all())

    def update_last_connected(self, client_id: int) -> None:
        """Update the last_connected_at timestamp."""
        stmt = select(RosettaChainClient).where(RosettaChainClient.id == client_id)
        client = self.db.execute(stmt).scalar_one_or_none()
        if client:
            client.last_connected_at = datetime.now(ZoneInfo("Asia/Jakarta"))
            self.db.flush()


class RosettaChainTableRepository:
    """Repository for virtual chain tables."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_client(self, chain_client_id: int) -> list[RosettaChainTable]:
        """Get all tables for a specific chain client.

        Returns tables that are either directly linked (chain_client_id = client_id)
        or cross-instance registered (chain_client_id IS NULL, source_chain_id = str(client_id)).
        The latter happens when the sender pushes schema via /chain/schema without a local
        FK match on the receiver side.

        Results are deduplicated by table_name — direct rows (chain_client_id IS NOT NULL)
        take precedence over cross-instance NULL rows so the same table never appears twice.
        """
        stmt = (
            select(RosettaChainTable)
            .where(
                or_(
                    RosettaChainTable.chain_client_id == chain_client_id,
                    (
                        RosettaChainTable.chain_client_id.is_(None)
                        & (RosettaChainTable.source_chain_id == str(chain_client_id))
                    ),
                )
            )
            .order_by(RosettaChainTable.table_name)
        )
        rows = list(self.db.execute(stmt).scalars().all())

        # Deduplicate by table_name — prefer direct-linked rows over NULL rows
        seen: dict[str, RosettaChainTable] = {}
        for t in rows:
            if t.table_name not in seen or t.chain_client_id is not None:
                seen[t.table_name] = t
        return list(seen.values())

    def get_by_client_and_name(
        self, chain_client_id: int, table_name: str
    ) -> Optional[RosettaChainTable]:
        """Get a specific table by client and name."""
        stmt = select(RosettaChainTable).where(
            RosettaChainTable.chain_client_id == chain_client_id,
            RosettaChainTable.table_name == table_name,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_database(
        self, chain_client_id: int, database_id: int
    ) -> list[RosettaChainTable]:
        """Get all tables for a specific database under a chain client."""
        stmt = (
            select(RosettaChainTable)
            .where(
                RosettaChainTable.chain_client_id == chain_client_id,
                RosettaChainTable.database_id == database_id,
            )
            .order_by(RosettaChainTable.table_name)
        )
        return list(self.db.execute(stmt).scalars().all())

    def upsert(
        self,
        chain_client_id: int,
        table_name: str,
        table_schema: dict,
        source_chain_id: Optional[str] = None,
        database_id: Optional[int] = None,
    ) -> RosettaChainTable:
        """Create or update a chain table entry."""
        existing = self.get_by_client_and_name(chain_client_id, table_name)
        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        if existing:
            existing.table_schema = table_schema
            if source_chain_id:
                existing.source_chain_id = source_chain_id
            if database_id is not None:
                existing.database_id = database_id
            existing.last_synced_at = now
            existing.updated_at = now
            self.db.flush()
            self.db.refresh(existing)
            return existing
        else:
            table = RosettaChainTable(
                chain_client_id=chain_client_id,
                table_name=table_name,
                table_schema=table_schema,
                source_chain_id=source_chain_id,
                database_id=database_id,
                last_synced_at=now,
            )
            self.db.add(table)
            self.db.flush()
            self.db.refresh(table)
            return table

    def delete_by_client(self, chain_client_id: int) -> int:
        """Delete all tables for a client. Returns count deleted."""
        tables = self.get_by_client(chain_client_id)
        count = len(tables)
        for table in tables:
            self.db.delete(table)
        self.db.flush()
        return count

    def update_record_count(
        self, chain_client_id: int, table_name: str, count: int
    ) -> None:
        """Update the record count for a table."""
        table = self.get_by_client_and_name(chain_client_id, table_name)
        if table:
            table.record_count = count
            table.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
            self.db.flush()


class RosettaChainDatabaseRepository:
    """Repository for virtual chain databases."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_client(self, chain_client_id: int) -> list[RosettaChainDatabase]:
        """Get all databases for a specific chain client."""
        stmt = (
            select(RosettaChainDatabase)
            .where(RosettaChainDatabase.chain_client_id == chain_client_id)
            .order_by(RosettaChainDatabase.name)
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_by_client_and_name(
        self, chain_client_id: int, name: str
    ) -> Optional[RosettaChainDatabase]:
        """Get a specific database by client and name."""
        stmt = select(RosettaChainDatabase).where(
            RosettaChainDatabase.chain_client_id == chain_client_id,
            RosettaChainDatabase.name == name,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def upsert(
        self,
        chain_client_id: int,
        name: str,
    ) -> RosettaChainDatabase:
        """Create or update a chain database entry."""
        existing = self.get_by_client_and_name(chain_client_id, name)
        now = datetime.now(ZoneInfo("Asia/Jakarta"))

        if existing:
            existing.updated_at = now
            self.db.flush()
            self.db.refresh(existing)
            return existing
        else:
            database = RosettaChainDatabase(
                chain_client_id=chain_client_id,
                name=name,
            )
            self.db.add(database)
            self.db.flush()
            self.db.refresh(database)
            return database

    def delete_by_client(self, chain_client_id: int) -> int:
        """Delete all databases for a client. Returns count deleted."""
        databases = self.get_by_client(chain_client_id)
        count = len(databases)
        for database in databases:
            self.db.delete(database)
        self.db.flush()
        return count
