"""
Base repository implementing common CRUD operations.

Provides generic repository pattern implementation for all models.
"""

from datetime import datetime
from typing import Any, Generic, List, Optional, Type, TypeVar
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.exceptions import DatabaseError, DuplicateEntityError, EntityNotFoundError
from app.core.logging import get_logger
from app.domain.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)

logger = get_logger(__name__)


class BaseRepository(Generic[ModelType]):
    """
    Generic repository for database operations.

    Implements common CRUD operations following the Repository pattern.
    Provides a clean abstraction over SQLAlchemy for data access.
    """

    def __init__(self, model: Type[ModelType], db: Session):
        """
        Initialize repository.

        Args:
            model: SQLAlchemy model class
            db: Database session
        """
        self.model = model
        self.db = db

    def create(self, **kwargs: Any) -> ModelType:
        """
        Create a new entity.

        Args:
            **kwargs: Entity attributes

        Returns:
            Created entity

        Raises:
            DuplicateEntityError: If unique constraint is violated
            DatabaseError: If database operation fails
        """
        try:
            entity = self.model(**kwargs)
            self.db.add(entity)
            self.db.flush()
            self.db.refresh(entity)

            logger.info(
                f"Created {self.model.__name__}", extra={"entity_id": entity.id}
            )

            return entity

        except IntegrityError as e:
            self.db.rollback()
            logger.warning(f"Duplicate {self.model.__name__}", extra={"error": str(e)})
            raise DuplicateEntityError(
                entity_type=self.model.__name__,
                field="name",
                value=kwargs.get("name", "unknown"),
            ) from e
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                f"Failed to create {self.model.__name__}", extra={"error": str(e)}
            )
            raise DatabaseError(f"Failed to create {self.model.__name__}") from e

    def get_by_id(self, entity_id: int) -> ModelType:
        """
        Get entity by ID.

        Args:
            entity_id: Entity identifier

        Returns:
            Entity instance

        Raises:
            EntityNotFoundError: If entity not found
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(
                select(self.model).where(self.model.id == entity_id)
            )
            entity = result.scalar_one_or_none()

            if entity is None:
                raise EntityNotFoundError(
                    entity_type=self.model.__name__, entity_id=entity_id
                )

            return entity

        except EntityNotFoundError:
            raise
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to get {self.model.__name__}",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            raise DatabaseError(f"Failed to get {self.model.__name__}") from e

    def get_by_name(self, name: str) -> Optional[ModelType]:
        """
        Get entity by name.

        Args:
            name: Entity name

        Returns:
            Entity instance or None if not found

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(select(self.model).where(self.model.name == name))
            return result.scalar_one_or_none()

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to get {self.model.__name__} by name",
                extra={"name": name, "error": str(e)},
            )
            raise DatabaseError(f"Failed to get {self.model.__name__}") from e

    def get_all(self, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """
        Get all entities with pagination.

        Args:
            skip: Number of entities to skip
            limit: Maximum number of entities to return

        Returns:
            List of entities

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            stmt = select(self.model)

            # Order by name (if model has it) then id for consistent results
            if hasattr(self.model, "name"):
                stmt = stmt.order_by(self.model.name.asc(), self.model.id.asc())
            else:
                stmt = stmt.order_by(self.model.id.asc())

            result = self.db.execute(stmt.offset(skip).limit(limit))
            return list(result.scalars().all())

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to get all {self.model.__name__}", extra={"error": str(e)}
            )
            raise DatabaseError(f"Failed to get all {self.model.__name__}") from e

    def count(self) -> int:
        """
        Count total number of entities.

        Returns:
            Total count

        Raises:
            DatabaseError: If database operation fails
        """
        try:
            result = self.db.execute(select(func.count()).select_from(self.model))
            return result.scalar_one()

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to count {self.model.__name__}", extra={"error": str(e)}
            )
            raise DatabaseError(f"Failed to count {self.model.__name__}") from e

    def update(self, entity_id: int, **kwargs: Any) -> ModelType:
        """
        Update entity by ID.

        Args:
            entity_id: Entity identifier
            **kwargs: Attributes to update

        Returns:
            Updated entity

        Raises:
            EntityNotFoundError: If entity not found
            DatabaseError: If database operation fails
        """
        try:
            entity = self.get_by_id(entity_id)

            for key, value in kwargs.items():
                if value is not None and hasattr(entity, key):
                    setattr(entity, key, value)
            
            # Explicitly set updated_at if the model has this field
            if hasattr(entity, 'updated_at'):
                entity.updated_at = datetime.now(ZoneInfo('Asia/Jakarta'))

            self.db.flush()
            self.db.refresh(entity)

            logger.info(
                f"Updated {self.model.__name__}", extra={"entity_id": entity_id}
            )

            return entity

        except EntityNotFoundError:
            raise
        except IntegrityError as e:
            self.db.rollback()
            logger.warning(
                f"Duplicate {self.model.__name__} on update",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            raise DuplicateEntityError(
                entity_type=self.model.__name__,
                field="name",
                value=kwargs.get("name", "unknown"),
            ) from e
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                f"Failed to update {self.model.__name__}",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            raise DatabaseError(f"Failed to update {self.model.__name__}") from e

    def delete(self, entity_id: int) -> None:
        """
        Delete entity by ID.

        Args:
            entity_id: Entity identifier

        Raises:
            EntityNotFoundError: If entity not found
            DatabaseError: If database operation fails
        """
        try:
            entity = self.get_by_id(entity_id)
            self.db.delete(entity)
            self.db.flush()

            logger.info(
                f"Deleted {self.model.__name__}", extra={"entity_id": entity_id}
            )

        except EntityNotFoundError:
            raise
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(
                f"Failed to delete {self.model.__name__}",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            raise DatabaseError(f"Failed to delete {self.model.__name__}") from e

    def exists(self, entity_id: int) -> bool:
        """
        Check if entity exists.

        Args:
            entity_id: Entity identifier

        Returns:
            True if exists, False otherwise
        """
        try:
            result = self.db.execute(
                select(self.model.id).where(self.model.id == entity_id)
            )
            return result.scalar_one_or_none() is not None

        except SQLAlchemyError:
            return False
