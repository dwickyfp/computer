"""
Preset service.
"""

from typing import List
from sqlalchemy.orm import Session
from app.domain.models.preset import Preset
from app.domain.schemas.preset import PresetCreate
from app.domain.repositories.preset import PresetRepository

class PresetService:
    """Preset service."""

    def __init__(self, db: Session):
        self.db = db
        self.repository = PresetRepository(db)

    def create_preset(self, source_id: int, preset_data: PresetCreate) -> Preset:
        """Create a new preset."""
        preset = self.repository.create(
            source_id=source_id,
            name=preset_data.name,
            table_names=preset_data.table_names
        )
        return preset

    def get_presets(self, source_id: int) -> List[Preset]:
        """Get all presets for a source."""
        return self.repository.get_by_source_id(source_id)

    def delete_preset(self, preset_id: int) -> None:
        """Delete a preset."""
        self.repository.delete(preset_id)

    def update_preset(self, preset_id: int, preset_data: PresetCreate) -> Preset:
        """Update a preset."""
        # We can update name or table_names.
        return self.repository.update(
            preset_id,
            name=preset_data.name,
            table_names=preset_data.table_names
        )
