from typing import Optional
import redis
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

class RedisClient:
    _instance: Optional[redis.Redis] = None

    @classmethod
    def get_instance(cls) -> redis.Redis:
        if cls._instance is None:
            cls._instance = redis.from_url(
                settings.redis_url, 
                decode_responses=True
            )
            try:
                cls._instance.ping()
                logger.info("Connected to Redis")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                # Don't raise here, allow app to start even if redis is down? 
                # Or maybe we want to fail fast? 
                # For caching, we can degrade gracefully if redis is down.
        return cls._instance

def get_redis() -> redis.Redis:
    return RedisClient.get_instance()
