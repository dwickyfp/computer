"""
Security utilities for worker service.

Provides credential decryption compatible with backend's encryption.
"""

import base64
import threading

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.config.settings import get_settings

import structlog

logger = structlog.get_logger(__name__)

_cipher: AESGCM | None = None
_cipher_lock = threading.Lock()


def get_cipher() -> AESGCM:
    """
    Get AES-256-GCM cipher instance (thread-safe).

    Uses the same key format as backend for compatibility.
    Fails loudly if the key is invalid rather than silently padding.
    """
    global _cipher
    if _cipher is not None:
        return _cipher

    with _cipher_lock:
        # Double-checked locking
        if _cipher is not None:
            return _cipher

        settings = get_settings()
        key_str = settings.credential_encryption_key

        # Try base64 decode first, then raw bytes
        try:
            key_bytes = base64.b64decode(key_str)
            if len(key_bytes) == 32:
                _cipher = AESGCM(key_bytes)
                return _cipher
        except Exception:
            pass

        # Check if the string itself is 32 bytes
        if len(key_str.encode("utf-8")) == 32:
            _cipher = AESGCM(key_str.encode("utf-8"))
            return _cipher

        # Fail loudly — do not silently pad with null bytes
        raise ValueError(
            f"CREDENTIAL_ENCRYPTION_KEY must be exactly 32 bytes "
            f"(or base64-encoded 32 bytes). Got {len(key_str.encode('utf-8'))} bytes. "
            f"This key MUST match the backend's key exactly."
        )


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt a base64-encoded AES-256-GCM encrypted string.

    Compatible with backend's encrypt_value() output.
    """
    if not encrypted_value:
        return encrypted_value

    try:
        combined = base64.b64decode(encrypted_value)
        nonce = combined[:12]
        ciphertext = combined[12:]
        aesgcm = get_cipher()
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode("utf-8")
    except Exception as e:
        raise ValueError(f"Decryption failed: {str(e)}")
