"""
Security utilities for credential encryption.

Implements AES-256-GCM encryption for securing sensitive data at rest.
"""

import base64
import logging
import os
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from core.exceptions import ConfigurationException

logger = logging.getLogger(__name__)


def _get_encryption_key() -> str:
    """Get encryption key from environment variable."""
    key = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")
    if not key:
        raise ValueError("CREDENTIAL_ENCRYPTION_KEY environment variable is not set")
    return key


def get_cipher() -> AESGCM:
    """
    Get AESGCM cipher instance using the configured encryption key.

    The key must be 16, 24, or 32 bytes for AES-128/192/256.
    Raises ConfigurationException if the key length is invalid.
    """
    key = _get_encryption_key()
    valid_lengths = (16, 24, 32)

    # Try decoding if it looks like base64
    try:
        decoded = base64.b64decode(key)
        if len(decoded) in valid_lengths:
            return AESGCM(decoded)
    except Exception:
        pass

    # If not base64 or length mismatch, check if the string itself is valid
    key_bytes = key.encode() if isinstance(key, str) else key
    if len(key_bytes) in valid_lengths:
        return AESGCM(key_bytes)

    raise ConfigurationException(
        f"CREDENTIAL_ENCRYPTION_KEY has invalid length: {len(key_bytes)} bytes. "
        f"Expected 16 (AES-128), 24 (AES-192), or 32 (AES-256) bytes. "
        f"If base64-encoded, decoded length was checked too."
    )


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value using AES-256-GCM.

    Format: base64(nonce + ciphertext + tag)
    """
    if not value:
        return value

    aesgcm = get_cipher()
    nonce = os.urandom(12)  # 96-bit nonce

    ciphertext = aesgcm.encrypt(nonce, value.encode(), None)
    combined = nonce + ciphertext
    return base64.b64encode(combined).decode("utf-8")


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt a base64 encoded string using AES-256-GCM.

    Raises ConfigurationException if the encryption key is missing or
    if decryption fails (key mismatch). This prevents raw ciphertext
    from being silently used as credentials.
    """
    if not encrypted_value:
        return encrypted_value

    # Check if encryption key is configured
    try:
        _get_encryption_key()
    except ValueError:
        raise ConfigurationException(
            "CREDENTIAL_ENCRYPTION_KEY is not set — cannot decrypt value. "
            "Set this environment variable to match the backend's key."
        )

    try:
        combined = base64.b64decode(encrypted_value)

        # Extract nonce (first 12 bytes)
        nonce = combined[:12]
        ciphertext = combined[12:]

        aesgcm = get_cipher()
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode("utf-8")
    except ConfigurationException:
        raise  # Re-raise key validation errors from get_cipher()
    except base64.binascii.Error:
        # Value is not base64 encoded — likely a plaintext value
        # (e.g., pre-encryption migration). Return as-is with warning.
        logger.warning(
            f"Value is not base64 encoded — returning as plaintext. "
            f"If credentials should be encrypted, re-save them via the UI."
        )
        return encrypted_value
    except Exception as e:
        raise ConfigurationException(
            f"Decryption failed — CREDENTIAL_ENCRYPTION_KEY likely does not match "
            f"the backend's key. Error: {e}. "
            f"Verify that compute and backend share the same encryption key."
        )
