"""
Unit tests for app.core.security — AES-256-GCM encrypt/decrypt utilities.

The CREDENTIAL_ENCRYPTION_KEY env var is injected by conftest.pytest_configure
before any app imports (base64-encoded 32-byte key).
Each test resets the module-level cipher singleton so tests are independent.
"""

import base64
import pytest
from unittest.mock import patch


# Reset the cached cipher singleton before every test so each test is isolated
@pytest.fixture(autouse=True)
def reset_cipher():
    """Clear the module-level _cipher singleton before each test."""
    import app.core.security as sec

    original = sec._cipher
    sec._cipher = None
    yield
    sec._cipher = original


# =============================================================================
# TestGetCipher
# =============================================================================


class TestGetCipher:
    def test_returns_aesgcm_instance(self):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from app.core.security import get_cipher

        cipher = get_cipher()
        assert isinstance(cipher, AESGCM)

    def test_returns_same_instance_on_repeated_calls(self):
        from app.core.security import get_cipher

        c1 = get_cipher()
        c2 = get_cipher()
        assert c1 is c2

    def test_invalid_key_length_raises_value_error(self):
        """Key that is neither 32 bytes raw nor base64-encoded 32 bytes raises."""
        from app.core.security import get_cipher
        from app.core.config import Settings

        bad_settings = Settings(  # type: ignore[call-arg]
            database_url="postgresql://x:x@localhost/x",
            secret_key="x" * 32,
            credential_encryption_key="tooshort",
            redis_url="redis://localhost:6379/0",
        )

        with patch("app.core.security.get_settings", return_value=bad_settings):
            with pytest.raises(ValueError, match="32 bytes"):
                get_cipher()


# =============================================================================
# TestEncryptValue
# =============================================================================


class TestEncryptValue:
    def test_encrypt_returns_non_empty_string(self):
        from app.core.security import encrypt_value

        result = encrypt_value("hello")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_encrypt_produces_valid_base64(self):
        from app.core.security import encrypt_value

        result = encrypt_value("test-value-123")
        # Should not raise
        decoded = base64.b64decode(result)
        # nonce (12 bytes) + ciphertext + GCM tag (16 bytes) → at least 28 bytes
        assert len(decoded) > 28

    def test_empty_string_returns_empty_string(self):
        from app.core.security import encrypt_value

        assert encrypt_value("") == ""
        assert encrypt_value(None) is None  # type: ignore[arg-type]

    def test_different_calls_produce_different_ciphertext(self):
        """Each call uses a fresh random nonce → different output even for same input."""
        from app.core.security import encrypt_value

        enc1 = encrypt_value("same-plaintext")
        enc2 = encrypt_value("same-plaintext")
        assert enc1 != enc2


# =============================================================================
# TestDecryptValue
# =============================================================================


class TestDecryptValue:
    def test_decrypt_round_trip(self):
        """encrypt → decrypt produces original plaintext."""
        from app.core.security import encrypt_value, decrypt_value

        plaintext = "super-secret-password!"
        assert decrypt_value(encrypt_value(plaintext)) == plaintext

    def test_decrypt_empty_returns_empty(self):
        from app.core.security import decrypt_value

        assert decrypt_value("") == ""

    def test_decrypt_garbage_raises_value_error(self):
        from app.core.security import decrypt_value

        with pytest.raises(ValueError, match="Decryption failed"):
            decrypt_value("not-valid-base64-ciphertext!!!")

    def test_decrypt_valid_base64_wrong_key_raises(self):
        """Base64-encoded bytes that were not encrypted with this key should fail."""
        from app.core.security import decrypt_value
        import base64

        # A plausible-looking but random ciphertext: 12 nonce + 32 fake bytes
        garbage = base64.b64encode(b"\xde\xad" * 22).decode()
        with pytest.raises(ValueError, match="Decryption failed"):
            decrypt_value(garbage)

    def test_unicode_plaintext_round_trip(self):
        from app.core.security import encrypt_value, decrypt_value

        unicode_val = "密码pass🔑"
        assert decrypt_value(encrypt_value(unicode_val)) == unicode_val

    def test_long_value_round_trip(self):
        from app.core.security import encrypt_value, decrypt_value

        long_val = "x" * 10_000
        assert decrypt_value(encrypt_value(long_val)) == long_val
