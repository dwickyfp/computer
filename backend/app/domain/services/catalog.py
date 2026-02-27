"""
Service for Rosetta Catalog architecture.
Handles business logic for database and table management,
and schema registration from remote clients.
"""

from typing import List, Optional
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.domain.repositories.catalog import (
    CatalogDatabaseRepository,
    CatalogTableRepository,
)
from app.domain.schemas.catalog import (
    CatalogDatabaseCreate,
    CatalogDatabaseResponse,
    CatalogTableResponse,
    SchemaRegistrationRequest,
)


class CatalogService:
    """Service for managing the Data Lake / Warehouse catalog."""

    def __init__(self, db: Session):
        self.db = db
        self.db_repo = CatalogDatabaseRepository(db)
        self.table_repo = CatalogTableRepository(db)

    # ─── Database Management ──────────────────────────────────────────────────

    def list_databases(self) -> List[CatalogDatabaseResponse]:
        dbs = self.db_repo.get_all()
        return [CatalogDatabaseResponse.from_orm(d) for d in dbs]

    def get_database(self, db_id: int) -> CatalogDatabaseResponse:
        db_obj = self.db_repo.get_by_id(db_id)
        if not db_obj:
            raise HTTPException(status_code=404, detail="Database not found")
        return CatalogDatabaseResponse.from_orm(db_obj)

    def create_database(self, data: CatalogDatabaseCreate) -> CatalogDatabaseResponse:
        existing = self.db_repo.get_by_name(data.name)
        if existing:
            raise HTTPException(
                status_code=400, detail="Database with this name already exists"
            )
        db_obj = self.db_repo.create(name=data.name, description=data.description)
        return CatalogDatabaseResponse.from_orm(db_obj)

    def delete_database(self, db_id: int) -> None:
        success = self.db_repo.delete(db_id)
        if not success:
            raise HTTPException(status_code=404, detail="Database not found")

    def update_database(
        self, db_id: int, name: str | None = None, description: str | None = None
    ) -> CatalogDatabaseResponse:
        if name is not None:
            existing = self.db_repo.get_by_name(name)
            if existing and existing.id != db_id:
                raise HTTPException(
                    status_code=400, detail="A database with that name already exists"
                )
        obj = self.db_repo.update(
            db_id,
            **{
                k: v
                for k, v in {"name": name, "description": description}.items()
                if v is not None
            },
        )
        if not obj:
            raise HTTPException(status_code=404, detail="Database not found")
        return CatalogDatabaseResponse.from_orm(obj)

    # ─── Table Discovery ──────────────────────────────────────────────────────

    def list_tables(self, database_id: int) -> List[CatalogTableResponse]:
        """List tables with Redis caching for instant discovery."""
        import json
        import redis
        from app.core.config import settings

        cache_key = f"rosetta:catalog:db:{database_id}:tables"
        redis_client = None
        try:
            # We use a short timeout for the cache connection to not block
            redis_client = redis.Redis.from_url(
                settings.redis_url, decode_responses=True, socket_timeout=1.0
            )
            cached = redis_client.get(cache_key)
            if cached:
                data = json.loads(cached)
                return [CatalogTableResponse(**t) for t in data]
        except Exception:
            pass  # fallback to DB

        tables = self.table_repo.get_by_database(database_id)
        responses = [CatalogTableResponse.from_orm(t) for t in tables]

        if redis_client:
            try:
                # Cache for 10 seconds
                encoded = json.dumps([t.dict() for t in responses], default=str)
                redis_client.setex(cache_key, 10, encoded)
            except Exception:
                pass

        return responses

    def get_table(self, table_id: int) -> CatalogTableResponse:
        table_obj = self.table_repo.get_by_id(table_id)
        if not table_obj:
            raise HTTPException(status_code=404, detail="Table not found")
        return CatalogTableResponse.from_orm(table_obj)

    def delete_table(self, table_id: int) -> None:
        success = self.table_repo.delete(table_id)
        if not success:
            raise HTTPException(status_code=404, detail="Table not found")

    # ─── Schema Registration (Handshake) ──────────────────────────────────────

    def register_schema(self, req: SchemaRegistrationRequest) -> CatalogTableResponse:
        """
        Handle registration request from Rosetta A.
        Upserts the table definition into the specified database.
        """
        # Resolve target database
        db_obj = self.db_repo.get_by_name(req.database_name)
        if not db_obj:
            # Auto-create database if it doesn't exist
            db_obj = self.db_repo.create(
                name=req.database_name, description="Auto-created via registration"
            )

        # Check if table already exists in this DB
        table_obj = self.table_repo.get_by_name(db_obj.id, req.table_name)

        if table_obj:
            # Updating existing schema
            table_obj = self.table_repo.update_schema(
                table_id=table_obj.id,
                schema_json=req.schema_definition,
                source_chain_id=req.source_chain_id,
            )
        else:
            # Creation of new stream
            stream_name = f"rosetta:catalog:{db_obj.name}:{req.table_name}"
            table_obj = self.table_repo.create(
                database_id=db_obj.id,
                table_name=req.table_name,
                schema_json=req.schema_definition,
                stream_name=stream_name,
                source_chain_id=req.source_chain_id,
            )

        return CatalogTableResponse.from_orm(table_obj)
