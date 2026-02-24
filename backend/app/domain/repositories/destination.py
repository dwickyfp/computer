"""
Destination repository for data access operations.

Extends base repository with destination-specific queries.
"""

from sqlalchemy.orm import Session

from app.domain.models.destination import Destination
from app.domain.repositories.base import BaseRepository


class DestinationRepository(BaseRepository[Destination]):
    """
    Repository for Destination entity.

    Provides data access methods for Snowflake destination configurations.
    """

    def __init__(self, db: Session):
        """Initialize destination repository."""
        super().__init__(Destination, db)
