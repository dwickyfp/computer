"""
Preset repository.
"""

from typing import List
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.domain.models.preset import Preset
from app.domain.repositories.base import BaseRepository

class PresetRepository(BaseRepository[Preset]):
    """Preset repository."""

    def __init__(self, db: Session):
        super().__init__(Preset, db)

    def get_by_source_id(self, source_id: int) -> List[Preset]:
        """Get all presets for a source."""
        query = select(Preset).where(Preset.source_id == source_id)
        result = self.db.execute(query)
        return list(result.scalars().all())
