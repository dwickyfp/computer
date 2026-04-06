"""
Internal Notification Config repository.

Handles CRUD operations for internal notification endpoint configurations.
"""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.models.internal_notification_config import InternalNotificationConfig


class InternalNotificationConfigRepository:
    """Repository for InternalNotificationConfig CRUD operations."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_all(self) -> List[InternalNotificationConfig]:
        """Return all configs ordered by id."""
        stmt = select(InternalNotificationConfig).order_by(
            InternalNotificationConfig.id
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_enabled(self) -> List[InternalNotificationConfig]:
        """Return only enabled configs (used by notification sender)."""
        stmt = (
            select(InternalNotificationConfig)
            .where(InternalNotificationConfig.is_enabled == True)
            .order_by(InternalNotificationConfig.id)
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_by_id(self, config_id: int) -> Optional[InternalNotificationConfig]:
        """Return a single config by primary key."""
        stmt = select(InternalNotificationConfig).where(
            InternalNotificationConfig.id == config_id
        )
        return self.db.execute(stmt).scalars().first()

    def create(self, data: dict) -> InternalNotificationConfig:
        """Create a new config record."""
        obj = InternalNotificationConfig(**data)
        self.db.add(obj)
        self.db.commit()
        self.db.refresh(obj)
        return obj

    def update(
        self, config_id: int, data: dict
    ) -> Optional[InternalNotificationConfig]:
        """Update an existing config record (partial update supported)."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        obj = self.get_by_id(config_id)
        if not obj:
            return None

        for field, value in data.items():
            if value is not None:
                setattr(obj, field, value)

        obj.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
        self.db.commit()
        self.db.refresh(obj)
        return obj

    def delete(self, config_id: int) -> bool:
        """Delete a config record. Returns True if deleted, False if not found."""
        obj = self.get_by_id(config_id)
        if not obj:
            return False
        self.db.delete(obj)
        self.db.commit()
        return True

    def set_enabled(
        self, config_id: int, is_enabled: bool
    ) -> Optional[InternalNotificationConfig]:
        """Toggle the is_enabled flag for a single config."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        obj = self.get_by_id(config_id)
        if not obj:
            return None
        obj.is_enabled = is_enabled
        obj.updated_at = datetime.now(ZoneInfo("Asia/Jakarta"))
        self.db.commit()
        self.db.refresh(obj)
        return obj
