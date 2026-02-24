"""
Redis client for worker service.

Provides connection to Redis for caching preview results.
Uses a connection pool for thread-safe concurrent access.
"""

import threading

import redis as redis_lib

from app.config.settings import get_settings

import structlog

logger = structlog.get_logger(__name__)


class RedisClient:
    """Thread-safe singleton Redis client with connection pooling."""

    _instance: redis_lib.Redis | None = None
    _pool: redis_lib.ConnectionPool | None = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> redis_lib.Redis | None:
        """Get or create pooled Redis connection (double-checked locking)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    try:
                        settings = get_settings()
                        cls._pool = redis_lib.ConnectionPool.from_url(
                            settings.redis_url,
                            decode_responses=True,
                            max_connections=settings.redis_max_connections,
                            socket_connect_timeout=5,
                            socket_timeout=10,
                            retry_on_timeout=True,
                        )
                        cls._instance = redis_lib.Redis(
                            connection_pool=cls._pool,
                        )
                        cls._instance.ping()
                        logger.info(
                            "Redis pool connected",
                            url=settings.redis_url,
                            max_connections=settings.redis_max_connections,
                        )
                    except Exception as e:
                        logger.warning("Redis connection failed", error=str(e))
                        cls._pool = None
                        cls._instance = None
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the Redis connection pool."""
        with cls._lock:
            if cls._instance:
                try:
                    cls._instance.close()
                except Exception:
                    pass
                cls._instance = None
            if cls._pool:
                try:
                    cls._pool.disconnect()
                except Exception:
                    pass
                cls._pool = None


def get_redis() -> redis_lib.Redis | None:
    """Get Redis client instance."""
    return RedisClient.get_instance()
