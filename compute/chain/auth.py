"""
Chain authentication for validating incoming connections.

Validates the X-Chain-Key header against the stored chain key
in the rosetta_chain_config table.

The decrypted key is cached with a TTL (default 60 s) so that a
regenerated key on the server side is picked up automatically
without restarting the compute process.
"""

import logging
import time
from typing import Optional

from fastapi import Header, HTTPException, status

from core.database import get_db_connection, return_db_connection
from core.security import decrypt_value

logger = logging.getLogger(__name__)

# Cache the decrypted key with a TTL for performance.
_cached_chain_key: Optional[str] = None
_cache_valid: bool = False
_cache_time: float = 0.0
_CACHE_TTL_SECONDS: float = 60.0  # Re-read from DB every 60 seconds


def _load_chain_key() -> Optional[str]:
    """Load and decrypt the chain key from database."""
    global _cached_chain_key, _cache_valid, _cache_time

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # ORDER BY id DESC ensures we always read the most-recently
            # generated key even if stale duplicate rows exist.
            cursor.execute(
                "SELECT chain_key, is_active FROM rosetta_chain_config ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()

            if not row:
                logger.debug("No chain config found")
                _cached_chain_key = None
                _cache_valid = True
                _cache_time = time.monotonic()
                return None

            chain_key_encrypted, is_active = row
            if not is_active:
                logger.debug("Chain ingestion is disabled")
                _cached_chain_key = None
                _cache_valid = True
                _cache_time = time.monotonic()
                return None

            try:
                decrypted = decrypt_value(chain_key_encrypted)

                # detect silent failure: decrypt_value returns the
                # encrypted blob unchanged when CREDENTIAL_ENCRYPTION_KEY
                # is missing or wrong.
                if decrypted == chain_key_encrypted:
                    logger.error(
                        "Chain key decryption returned the encrypted blob "
                        "unchanged — CREDENTIAL_ENCRYPTION_KEY is likely "
                        "missing or does not match the backend. Chain auth "
                        "will ALWAYS fail until this is fixed."
                    )
                    _cached_chain_key = None
                    _cache_valid = True
                    _cache_time = time.monotonic()
                    return None

                _cached_chain_key = decrypted
                _cache_valid = True
                _cache_time = time.monotonic()
                logger.debug("Chain key loaded and decrypted successfully")
                return _cached_chain_key
            except Exception as e:
                logger.error(f"Failed to decrypt chain key: {e}")
                _cached_chain_key = None
                _cache_valid = True
                _cache_time = time.monotonic()
                return None
    except Exception as e:
        logger.error(f"Failed to load chain key from database: {e}")
        return None
    finally:
        if conn:
            return_db_connection(conn)


def _is_cache_expired() -> bool:
    """Check whether the cached key has exceeded its TTL."""
    return (time.monotonic() - _cache_time) > _CACHE_TTL_SECONDS


def invalidate_key_cache() -> None:
    """Invalidate the cached chain key (call after key regeneration)."""
    global _cache_valid
    _cache_valid = False
    logger.info("Chain key cache invalidated")


def validate_chain_key(x_chain_key: str = Header(...)) -> str:
    """
    FastAPI dependency to validate the X-Chain-Key header.

    Raises 401 if the key is missing or invalid.
    Returns the validated key.
    """
    global _cache_valid

    if not _cache_valid or _is_cache_expired():
        _load_chain_key()

    if _cached_chain_key is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chain ingestion is not configured or disabled on this instance",
        )

    if x_chain_key != _cached_chain_key:
        # Key mismatch — maybe the key was just regenerated.  Try one
        # forced reload before rejecting.
        logger.info("Chain key mismatch — forcing DB reload to check for rotation")
        _load_chain_key()
        if x_chain_key != _cached_chain_key:
            logger.warning(
                "Chain key validation failed after forced reload. "
                "Sent key (first 8 chars): %s..., "
                "Expected key loaded: %s",
                x_chain_key[:8] if x_chain_key else "(empty)",
                "yes" if _cached_chain_key else "NO (decryption failed?)",
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid chain key",
            )

    return x_chain_key
