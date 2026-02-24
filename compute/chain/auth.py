"""
Chain authentication for validating incoming connections.

Validates the X-Chain-Key header against the stored chain key
in the rosetta_chain_config table.
"""

import logging
from typing import Optional

from fastapi import Header, HTTPException, status

from core.database import get_db_connection, return_db_connection
from core.security import decrypt_value

logger = logging.getLogger(__name__)

# Cache the decrypted key for performance (invalidated on restart)
_cached_chain_key: Optional[str] = None
_cache_valid: bool = False


def _load_chain_key() -> Optional[str]:
    """Load and decrypt the chain key from database."""
    global _cached_chain_key, _cache_valid

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT chain_key, is_active FROM rosetta_chain_config LIMIT 1"
            )
            row = cursor.fetchone()

            if not row:
                logger.debug("No chain config found")
                _cached_chain_key = None
                _cache_valid = True
                return None

            chain_key_encrypted, is_active = row
            if not is_active:
                logger.debug("Chain ingestion is disabled")
                _cached_chain_key = None
                _cache_valid = True
                return None

            try:
                _cached_chain_key = decrypt_value(chain_key_encrypted)
                _cache_valid = True
                return _cached_chain_key
            except Exception as e:
                logger.error(f"Failed to decrypt chain key: {e}")
                _cached_chain_key = None
                _cache_valid = True
                return None
    except Exception as e:
        logger.error(f"Failed to load chain key from database: {e}")
        return None
    finally:
        if conn:
            return_db_connection(conn)


def invalidate_key_cache() -> None:
    """Invalidate the cached chain key (call after key regeneration)."""
    global _cache_valid
    _cache_valid = False


def validate_chain_key(x_chain_key: str = Header(...)) -> str:
    """
    FastAPI dependency to validate the X-Chain-Key header.

    Raises 401 if the key is missing or invalid.
    Returns the validated key.
    """
    global _cache_valid

    if not _cache_valid:
        _load_chain_key()

    if _cached_chain_key is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chain ingestion is not configured or disabled on this instance",
        )

    if x_chain_key != _cached_chain_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid chain key",
        )

    return x_chain_key
