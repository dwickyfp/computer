"""
Database connection management for worker service.

Provides synchronous SQLAlchemy sessions for Celery tasks.
"""

import threading
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from app.config.settings import get_settings

import structlog

logger = structlog.get_logger(__name__)


class WorkerDatabaseManager:
    """
    Manages database engine and session factory for worker.

    Singleton pattern ensures one engine per worker process.
    """

    _instance = None
    _engine: Engine | None = None
    _session_factory: sessionmaker[Session] | None = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(self) -> None:
        """Initialize database engine with connection pooling."""
        with self._lock:
            if self._engine is not None:
                return

            settings = get_settings()
            db_url = settings.database_connection_string

            logger.info(
                "Initializing worker database pool",
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
            )

            self._engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
                pool_timeout=settings.db_pool_timeout,
                pool_recycle=settings.db_pool_recycle,
                pool_pre_ping=True,
                echo=False,
                connect_args={
                    "connect_timeout": 5,
                    "options": "-c statement_timeout=30000",
                },
            )

            self._session_factory = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
            )

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self.initialize()
        return self._engine

    @property
    def session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is None:
            self.initialize()
        return self._session_factory

    def dispose(self) -> None:
        """Dispose engine and connections."""
        with self._lock:
            if self._engine:
                self._engine.dispose()
                self._engine = None
                self._session_factory = None
                logger.info("Worker database connections disposed")


_db_manager = WorkerDatabaseManager()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Get a database session context manager.

    Yields a session and handles commit/rollback/close.
    """
    _db_manager.initialize()
    session = _db_manager.session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_engine() -> Engine:
    """Get the database engine."""
    _db_manager.initialize()
    return _db_manager.engine
