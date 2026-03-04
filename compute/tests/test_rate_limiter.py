"""
Unit tests for the sliding-window rate limiter in compute/server.py.

Tests _check_rate_limit() and _rate_limit_windows directly
without starting the FastAPI server.
"""

import os
import sys
import time
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server as srv


def _clear_window(chain_id: str) -> None:
    """Helper: remove chain_id from the rate-limit window dict."""
    with srv._rate_limit_lock:
        srv._rate_limit_windows.pop(chain_id, None)


# ===========================================================================
# TestCheckRateLimitBasic
# ===========================================================================


class TestCheckRateLimitBasic:
    def setup_method(self):
        _clear_window("basic-chain")

    def teardown_method(self):
        _clear_window("basic-chain")

    def test_first_request_allowed(self):
        assert srv._check_rate_limit("basic-chain") is True

    def test_second_request_allowed(self):
        srv._check_rate_limit("basic-chain")
        assert srv._check_rate_limit("basic-chain") is True

    def test_within_limit_all_allowed(self):
        # Fill up to limit - 1
        limit = srv._RATE_LIMIT_PER_MINUTE
        for _ in range(limit - 1):
            result = srv._check_rate_limit("basic-chain")
        assert result is True

    def test_at_limit_returns_false(self):
        limit = srv._RATE_LIMIT_PER_MINUTE
        # Fill the window to the limit
        for _ in range(limit):
            srv._check_rate_limit("basic-chain")
        # Next request should be rejected
        assert srv._check_rate_limit("basic-chain") is False


# ===========================================================================
# TestCheckRateLimitIsolation
# ===========================================================================


class TestCheckRateLimitIsolation:
    def setup_method(self):
        _clear_window("chain-A")
        _clear_window("chain-B")

    def teardown_method(self):
        _clear_window("chain-A")
        _clear_window("chain-B")

    def test_different_chain_ids_independent(self):
        limit = srv._RATE_LIMIT_PER_MINUTE
        # Exhaust chain-A
        for _ in range(limit):
            srv._check_rate_limit("chain-A")
        # chain-B should still be allowed
        assert srv._check_rate_limit("chain-B") is True

    def test_chain_a_limit_does_not_affect_chain_b(self):
        limit = srv._RATE_LIMIT_PER_MINUTE
        for _ in range(limit + 5):
            srv._check_rate_limit("chain-A")
        for _ in range(limit - 1):
            srv._check_rate_limit("chain-B")
        assert srv._check_rate_limit("chain-B") is True


# ===========================================================================
# TestCheckRateLimitSlidingWindow
# ===========================================================================


class TestCheckRateLimitSlidingWindow:
    CHAIN_ID = "sliding-chain"

    def setup_method(self):
        _clear_window(self.CHAIN_ID)

    def teardown_method(self):
        _clear_window(self.CHAIN_ID)

    def test_expired_timestamps_not_counted(self):
        """Timestamps older than 60s are dropped — window slides forward."""
        limit = srv._RATE_LIMIT_PER_MINUTE
        old_time = time.time() - 65  # 65 seconds ago = outside the 60s window

        # Manually inject old timestamps that should be pruned
        with srv._rate_limit_lock:
            srv._rate_limit_windows[self.CHAIN_ID] = [old_time] * limit

        # Since all injected timestamps are older than 60s, the next request
        # should be allowed (they are evicted from the sliding window)
        assert srv._check_rate_limit(self.CHAIN_ID) is True

    def test_fresh_timestamps_are_counted(self):
        """Recent timestamps within the 60s window count toward the limit."""
        limit = srv._RATE_LIMIT_PER_MINUTE
        recent = time.time() - 5  # 5 seconds ago = inside the 60s window

        with srv._rate_limit_lock:
            srv._rate_limit_windows[self.CHAIN_ID] = [recent] * limit

        # Window is still full → next request rejected
        assert srv._check_rate_limit(self.CHAIN_ID) is False

    def test_empty_window_cleaned_up(self):
        """When all timestamps expire, the dict entry is removed (no memory leak)."""
        limit = srv._RATE_LIMIT_PER_MINUTE
        old_time = time.time() - 65

        # Fill with expired timestamps
        with srv._rate_limit_lock:
            srv._rate_limit_windows[self.CHAIN_ID] = [old_time] * limit

        # Call once — it prunes expired timestamps and may remove if still >= limit
        srv._check_rate_limit(self.CHAIN_ID)

        # After allowing the request, the key should exist with 1 fresh timestamp
        # or may have been cleared — key should not hold expired timestamps
        with srv._rate_limit_lock:
            timestamps = srv._rate_limit_windows.get(self.CHAIN_ID, [])
        now = time.time()
        for ts in timestamps:
            # All stored timestamps should be within the 60s window
            assert ts > now - 61, f"Stale timestamp found: {ts}"


# ===========================================================================
# TestRateLimitWithCustomLimit
# ===========================================================================


class TestRateLimitWithCustomLimit:
    CHAIN_ID = "custom-limit-chain"

    def setup_method(self):
        _clear_window(self.CHAIN_ID)

    def teardown_method(self):
        _clear_window(self.CHAIN_ID)
        # Restore original limit
        srv._RATE_LIMIT_PER_MINUTE = int(os.getenv("CHAIN_RATE_LIMIT_PER_MINUTE", "60"))

    def test_custom_low_limit_enforced(self):
        original = srv._RATE_LIMIT_PER_MINUTE
        srv._RATE_LIMIT_PER_MINUTE = 3
        try:
            assert srv._check_rate_limit(self.CHAIN_ID) is True  # 1
            assert srv._check_rate_limit(self.CHAIN_ID) is True  # 2
            assert srv._check_rate_limit(self.CHAIN_ID) is True  # 3
            assert srv._check_rate_limit(self.CHAIN_ID) is False  # 4 → over limit
        finally:
            srv._RATE_LIMIT_PER_MINUTE = original
