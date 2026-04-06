"""
Unit tests for compute/core/security.py.

AES-256-GCM encrypt/decrypt utilities.
CREDENTIAL_ENCRYPTION_KEY is injected via conftest.pytest_configure.
"""

import base64
import os
import sys
import pytest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ===========================================================================
# TestGetCipher
# ===========================================================================


class TestGetCipher:
    def test_returns_aesgcm_instance(self):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from core.security import get_cipher

        assert isinstance(get_cipher(), AESGCM)

    def test_missing_key_raises_value_error(self):
        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": ""}):
            from core import security as sec

            with pytest.raises((ValueError, Exception)):
                sec._get_encryption_key()

    def test_invalid_length_raises(self):
        """A key that isn't 16/24/32 bytes (raw or base64) raises."""
        from core.security import get_cipher
        from core.exceptions import ConfigurationException

        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": "short"}):
            with pytest.raises((ConfigurationException, ValueError)):
                get_cipher()

    def test_valid_16_byte_key_accepted(self):
        """AES-128 (16-byte key) is accepted."""
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from core.security import get_cipher

        key_16 = base64.b64encode(b"0123456789abcdef").decode()
        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": key_16}):
            cipher = get_cipher()
        assert isinstance(cipher, AESGCM)

    def test_valid_32_byte_key_accepted(self):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from core.security import get_cipher

        key_32 = base64.b64encode(b"0" * 32).decode()
        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": key_32}):
            cipher = get_cipher()
        assert isinstance(cipher, AESGCM)


# ===========================================================================
# TestEncryptValue
# ===========================================================================


class TestEncryptValue:
    def test_encrypt_returns_non_empty_string(self):
        from core.security import encrypt_value

        result = encrypt_value("hello-world")
        assert isinstance(result, str) and len(result) > 0

    def test_encrypt_is_valid_base64(self):
        from core.security import encrypt_value

        result = encrypt_value("test-value")
        decoded = base64.b64decode(result)
        # nonce(12) + ciphertext + GCM tag(16) → at least 28 bytes
        assert len(decoded) > 28

    def test_empty_value_returns_empty(self):
        from core.security import encrypt_value

        assert encrypt_value("") == ""

    def test_different_calls_produce_different_ciphertext(self):
        """Random nonce → different output for same input."""
        from core.security import encrypt_value

        enc1 = encrypt_value("same-plaintext")
        enc2 = encrypt_value("same-plaintext")
        assert enc1 != enc2


# ===========================================================================
# TestDecryptValue
# ===========================================================================


class TestDecryptValue:
    def test_round_trip(self):
        from core.security import encrypt_value, decrypt_value

        plaintext = "super-secret-pass!@#"
        assert decrypt_value(encrypt_value(plaintext)) == plaintext

    def test_empty_value_returns_empty(self):
        from core.security import decrypt_value

        assert decrypt_value("") == ""

    def test_garbage_raises(self):
        from core.security import decrypt_value
        from core.exceptions import ConfigurationException

        with pytest.raises((ConfigurationException, ValueError, Exception)):
            decrypt_value("not-valid-base64!!")

    def test_wrong_key_raises(self):
        """bytes encrypted with one key cannot be decrypted with another."""
        from core.security import encrypt_value, decrypt_value
        from core.exceptions import ConfigurationException

        # Encrypt with the current key (set in conftest)
        ciphertext = encrypt_value("sensitive-data")

        # Now change the key and try to decrypt
        wrong_key = base64.b64encode(b"W" * 32).decode()
        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": wrong_key}):
            # Reset any cached cipher if present
            import importlib
            import core.security as sec_mod

            original_cipher_val = sec_mod.__dict__.get("_cipher_cache", None)
            try:
                with pytest.raises((ConfigurationException, ValueError, Exception)):
                    decrypt_value(ciphertext)
            finally:
                pass  # cached cipher tests handled separately

    def test_unicode_round_trip(self):
        from core.security import encrypt_value, decrypt_value

        val = "パスワード123🔑"
        assert decrypt_value(encrypt_value(val)) == val

    def test_long_value_round_trip(self):
        from core.security import encrypt_value, decrypt_value

        val = "x" * 10_000
        assert decrypt_value(encrypt_value(val)) == val

    def test_missing_key_raises_configuration_exception(self):
        from core.security import decrypt_value
        from core.exceptions import ConfigurationException

        with patch.dict(os.environ, {"CREDENTIAL_ENCRYPTION_KEY": ""}):
            with pytest.raises((ConfigurationException, ValueError)):
                decrypt_value("dGVzdA==")
