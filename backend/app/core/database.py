"""
Database connection management with connection pooling.

Implements robust connection pool with safeguards against pool exhaustion.
"""

from contextlib import contextmanager
from typing import Generator
import logging as stdlib_logging
import threading

from fastapi import HTTPException

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool

from app.core.config import get_settings
from app.core.exceptions import DatabaseConnectionError, DatabaseError, RosettaException
from app.core.logging import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """
    Manages database engine and session factory.

    Implements singleton pattern to ensure single engine instance.
    Provides robust connection pooling with monitoring and safeguards.
    """

    _instance = None
    _engine: Engine | None = None
    _session_factory: sessionmaker[Session] | None = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(self) -> None:
        """
        Initialize database engine and session factory.

        Creates engine with optimized connection pool settings.
        Implements safeguards to prevent pool exhaustion.
        """
        with self._lock:
            if self._engine is not None:
                logger.warning("Database already initialized, skipping")
                return

            settings = get_settings()

            try:
                logger.info(
                    "Initializing database connection pool",
                    extra={
                        "pool_size": settings.db_pool_size,
                        "max_overflow": settings.db_max_overflow,
                        "pool_timeout": settings.db_pool_timeout,
                    },
                )

                # Convert async URL to sync URL for psycopg2
                db_url = settings.database_connection_string
                if db_url.startswith("postgresql+asyncpg://"):
                    db_url = db_url.replace(
                        "postgresql+asyncpg://", "postgresql+psycopg2://"
                    )
                elif db_url.startswith("postgresql://"):
                    db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

                # Create engine with connection pooling
                self._engine = create_engine(
                    db_url,
                    **settings.get_sqlalchemy_engine_config(),
                    poolclass=QueuePool,  # Use QueuePool for production
                )

                # Ensure pool logger stays at WARNING after engine creation.
                # create_engine() with echo_pool=True would override our
                # setup_logging() configuration, so we re-apply the level here
                # as a safety net.
                stdlib_logging.getLogger("sqlalchemy.pool").setLevel(
                    stdlib_logging.WARNING
                )

                # Create session factory
                self._session_factory = sessionmaker(
                    bind=self._engine,
                    class_=Session,
                    expire_on_commit=False,  # Prevent lazy loading issues
                    autocommit=False,
                    autoflush=False,
                )

                logger.info("Database connection pool initialized successfully")

            except SQLAlchemyError as e:
                logger.error(
                    "Failed to initialize database connection pool",
                    extra={"error": str(e)},
                )
                raise DatabaseConnectionError(
                    f"Failed to connect to database: {str(e)}"
                ) from e
            except Exception as e:
                logger.error(
                    "Unexpected error during database initialization",
                    extra={"error": str(e)},
                )
                raise DatabaseError(f"Unexpected database error: {str(e)}") from e

    def close(self) -> None:
        """
        Close database connections and dispose of engine.

        Should be called on application shutdown to ensure
        graceful cleanup of database connections.
        """
        with self._lock:
            if self._engine is not None:
                logger.info("Closing database connection pool")
                self._engine.dispose()
                self._engine = None
                self._session_factory = None
                logger.info("Database connection pool closed")

    @property
    def engine(self) -> Engine:
        """Get the database engine."""
        if self._engine is None:
            raise DatabaseConnectionError(
                "Database not initialized. Call initialize() first."
            )
        return self._engine

    @property
    def session_factory(self) -> sessionmaker[Session]:
        """Get the session factory."""
        if self._session_factory is None:
            raise DatabaseConnectionError(
                "Database not initialized. Call initialize() first."
            )
        return self._session_factory

    def get_pool_status(self) -> dict:
        """
        Get current connection pool status.

        Returns metrics for monitoring pool health and preventing exhaustion.
        """
        if self._engine is None:
            return {"status": "not_initialized"}

        pool = self._engine.pool
        return {
            "status": "healthy",
            "pool_size": pool.size(),
            "checked_in_connections": pool.checkedin(),
            "checked_out_connections": pool.checkedout(),
            "overflow": pool.overflow(),
            "total_connections": pool.size() + pool.overflow(),
        }

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Get database session as context manager.
        """
        session = self.session_factory()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# Global database manager instance
db_manager = DatabaseManager()


def get_db_session() -> Generator[Session, None, None]:
    """
    Dependency for getting database session.

    Provides context manager for database transactions.
    Automatically handles commit/rollback and session cleanup.

    Usage:
        @app.get("/example")
        def example(db: Session = Depends(get_db_session)):
            # Use db session
            pass
    """
    session = db_manager.session_factory()
    try:
        yield session
        session.commit()
    except HTTPException:
        session.rollback()
        raise
    except RosettaException:
        # Let business logic exceptions propagate
        session.rollback()
        raise
    except SQLAlchemyError as e:
        session.rollback()
        logger.error("Database error during session", extra={"error": str(e)})
        raise DatabaseError(f"Database operation failed: {str(e)}") from e
    except Exception as e:
        session.rollback()
        logger.error(
            "Unexpected error during database session", extra={"error": str(e)}
        )
        raise
    finally:
        session.close()


def get_db_session_readonly() -> Generator[Session, None, None]:
    """
    Dependency for getting a read-only database session.

    Unlike get_db_session(), this does NOT call session.commit() on success.
    Use for GET endpoints that only read data — avoids an unnecessary
    COMMIT round-trip to PostgreSQL (~0.1-0.5ms per request).

    Usage:
        @app.get("/example")
        def example(db: Session = Depends(get_db_session_readonly)):
            return db.query(Model).all()
    """
    session = db_manager.session_factory()
    try:
        yield session
    except HTTPException:
        session.rollback()
        raise
    except RosettaException:
        session.rollback()
        raise
    except SQLAlchemyError as e:
        session.rollback()
        logger.error("Database error during read-only session", extra={"error": str(e)})
        raise DatabaseError(f"Database operation failed: {str(e)}") from e
    except Exception as e:
        session.rollback()
        logger.error(
            "Unexpected error during read-only database session",
            extra={"error": str(e)},
        )
        raise
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """
    Alias for get_db_session for backward compatibility.

    Prefer using get_db_session() for clarity.
    """
    yield from get_db_session()


@contextmanager
def get_session_context() -> Generator[Session, None, None]:
    """
    Get database session as context manager.

    Useful for manual transaction management outside of FastAPI routes.

    Usage:
        with get_session_context() as session:
            # Use session
            session.execute(...)
            session.commit()
    """
    session = db_manager.session_factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_database_health() -> bool:
    """
    Check database connection health.

    Returns True if database is accessible, False otherwise.
    Used for health check endpoints.
    """
    try:
        with get_session_context() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error("Database health check failed", extra={"error": str(e)})
        return False
