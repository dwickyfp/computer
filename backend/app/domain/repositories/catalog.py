"""
Repository for Rosetta Catalog architecture.
"""

from typing import List, Optional
from sqlalchemy.orm import Session

from app.domain.models.catalog import CatalogDatabase, CatalogTable


class CatalogDatabaseRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_all(self) -> List[CatalogDatabase]:
        return self.db.query(CatalogDatabase).order_by(CatalogDatabase.name).all()

    def get_by_id(self, db_id: int) -> Optional[CatalogDatabase]:
        return (
            self.db.query(CatalogDatabase).filter(CatalogDatabase.id == db_id).first()
        )

    def get_by_name(self, name: str) -> Optional[CatalogDatabase]:
        return (
            self.db.query(CatalogDatabase).filter(CatalogDatabase.name == name).first()
        )

    def create(self, name: str, description: Optional[str] = None) -> CatalogDatabase:
        db_obj = CatalogDatabase(name=name, description=description)
        self.db.add(db_obj)
        self.db.commit()
        self.db.refresh(db_obj)
        return db_obj

    def delete(self, db_id: int) -> bool:
        obj = self.get_by_id(db_id)
        if obj:
            self.db.delete(obj)
            self.db.commit()
            return True
        return False

    def update(self, db_id: int, **kwargs) -> Optional[CatalogDatabase]:
        obj = self.get_by_id(db_id)
        if obj:
            for key, value in kwargs.items():
                if value is not None:
                    setattr(obj, key, value)
            self.db.commit()
            self.db.refresh(obj)
        return obj


class CatalogTableRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_database(self, database_id: int) -> List[CatalogTable]:
        return (
            self.db.query(CatalogTable)
            .filter(CatalogTable.database_id == database_id)
            .order_by(CatalogTable.table_name)
            .all()
        )

    def get_by_id(self, table_id: int) -> Optional[CatalogTable]:
        return self.db.query(CatalogTable).filter(CatalogTable.id == table_id).first()

    def get_by_name(self, database_id: int, table_name: str) -> Optional[CatalogTable]:
        return (
            self.db.query(CatalogTable)
            .filter(
                CatalogTable.database_id == database_id,
                CatalogTable.table_name == table_name,
            )
            .first()
        )

    def create(
        self,
        database_id: int,
        table_name: str,
        schema_json: dict,
        stream_name: str,
        source_chain_id: Optional[str] = None,
    ) -> CatalogTable:
        table_obj = CatalogTable(
            database_id=database_id,
            table_name=table_name,
            schema_json=schema_json,
            stream_name=stream_name,
            source_chain_id=source_chain_id,
            status="ACTIVE",  # Assuming active on creation/registration
        )
        self.db.add(table_obj)
        self.db.commit()
        self.db.refresh(table_obj)
        return table_obj

    def update_schema(
        self, table_id: int, schema_json: dict, source_chain_id: Optional[str] = None
    ) -> CatalogTable:
        table = self.get_by_id(table_id)
        if table:
            table.schema_json = schema_json
            if source_chain_id:
                table.source_chain_id = source_chain_id
            self.db.commit()
            self.db.refresh(table)
        return table

    def delete(self, table_id: int) -> bool:
        obj = self.get_by_id(table_id)
        if obj:
            self.db.delete(obj)
            self.db.commit()
            return True
        return False
