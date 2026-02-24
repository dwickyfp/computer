"""
Security utilities for credential encryption.

Implements AES-256-GCM encryption for securing sensitive data at rest.
"""

import base64
import os
import threading
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.core.config import get_settings

# ─── Thread-safe cached cipher singleton ──────────────────────────────────────
_cipher: AESGCM | None = None
_cipher_lock = threading.Lock()


def get_cipher() -> AESGCM:
    """
    Get AESGCM cipher instance (thread-safe, cached).

    The key must be 32 bytes (256 bits) for AES-256.
    Accepts a base64-encoded 32-byte key or a raw 32-byte string.
    The cipher is created once and reused for all subsequent calls.
    """
    global _cipher
    if _cipher is not None:
        return _cipher

    with _cipher_lock:
        # Double-checked locking
        if _cipher is not None:
            return _cipher

        settings = get_settings()
        key = settings.credential_encryption_key

        # Try decoding if it looks like base64, otherwise use bytes
        try:
            decoded = base64.b64decode(key)
            if len(decoded) == 32:
                _cipher = AESGCM(decoded)
                return _cipher
        except Exception:
            pass

        # If not base64 or length mismatch, check if the string itself is 32 bytes
        if len(key.encode()) == 32:
            _cipher = AESGCM(key.encode())
            return _cipher

        # Fail loudly — do not silently pad or truncate keys
        raise ValueError(
            f"CREDENTIAL_ENCRYPTION_KEY must be exactly 32 bytes "
            f"(or base64-encoded 32 bytes). Got {len(key.encode())} bytes."
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

    # Encrypt
    ciphertext = aesgcm.encrypt(nonce, value.encode(), None)

    # Combine nonce + ciphertext (tag is included in ciphertext by cryptography lib logic usually?
    # Wait, AESGCM.encrypt returns ciphertext + tag appended)
    # So we just need Prepended Nonce + (Ciphertext + Tag)

    combined = nonce + ciphertext
    return base64.b64encode(combined).decode("utf-8")


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt a base64 encoded string using AES-256-GCM.
    """
    if not encrypted_value:
        return encrypted_value

    try:
        combined = base64.b64decode(encrypted_value)

        # Extract nonce (first 12 bytes)
        nonce = combined[:12]
        ciphertext = combined[12:]

        aesgcm = get_cipher()
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode("utf-8")
    except Exception as e:
        # Log error? Return original if failed?
        # For security, failing explicitly is better than returning garbage.
        # But if it wasn't encrypted (legacy), maybe return as is?
        # For this implementation, we assume all values passed here ARE encrypted.
        raise ValueError(f"Decryption failed: {str(e)}")
