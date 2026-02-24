from typing import Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.domain.models.history_schema_evolution import HistorySchemaEvolution
from app.domain.repositories.base import BaseRepository


class HistorySchemaEvolutionRepository(BaseRepository[HistorySchemaEvolution]):
    def __init__(self, db: Session):
        super().__init__(HistorySchemaEvolution, db)

    def get_by_table_and_version(self, table_id: int, version: int) -> Optional[HistorySchemaEvolution]:
        return (
            self.db.query(HistorySchemaEvolution)
            .filter(
                HistorySchemaEvolution.table_metadata_list_id == table_id,
                HistorySchemaEvolution.version_schema == version
            )
            .first()
        )
