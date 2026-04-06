"""
Unit tests for compute/core/timezone.py.

Pure unit tests — no I/O required.
"""

import os
import sys
from datetime import datetime, timezone, timedelta, time as dt_time
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ===========================================================================
# TestGetTargetTimezone
# ===========================================================================


class TestGetTargetTimezone:
    def test_returns_tzinfo(self):
        from core.timezone import get_target_timezone
        import datetime as _dt

        tz = get_target_timezone()
        assert isinstance(tz, _dt.tzinfo)

    def test_default_is_jakarta(self):
        """Default env (Asia/Jakarta) offset is UTC+7."""
        from core.timezone import get_target_timezone

        tz = get_target_timezone()
        # Create a non-DST datetime to measure offset
        dt = datetime(2024, 6, 15, 0, 0, 0, tzinfo=tz)
        offset = dt.utcoffset()
        assert offset == timedelta(hours=7)

    def test_custom_tz_respected(self):
        """ROSETTA_TIMEZONE env var selects a different timezone."""
        from core import timezone as tz_mod
        import importlib

        with patch.dict(os.environ, {"ROSETTA_TIMEZONE": "UTC"}):
            # Force re-read via direct call (function always re-reads env)
            tz = tz_mod.get_target_timezone()
        dt = datetime(2024, 1, 1, tzinfo=tz)
        assert dt.utcoffset() == timedelta(0)

    def test_unknown_tz_falls_back_to_utc7(self):
        """Unknown timezone name falls back to UTC+7 fixed offset."""
        from core import timezone as tz_mod

        with patch.dict(os.environ, {"ROSETTA_TIMEZONE": "Invalid/Timezone_XYZ"}):
            tz = tz_mod.get_target_timezone()
        dt = datetime(2024, 6, 1, tzinfo=tz)
        assert dt.utcoffset() == timedelta(hours=7)


# ===========================================================================
# TestConvertTimestampToTargetTz
# ===========================================================================


class TestConvertTimestampToTargetTz:
    def test_naive_datetime_returned_unchanged(self):
        from core.timezone import convert_timestamp_to_target_tz

        naive = datetime(2024, 6, 15, 12, 0, 0)
        result = convert_timestamp_to_target_tz(naive)
        assert result == naive
        assert result.tzinfo is None

    def test_utc_aware_converted_to_jakarta(self):
        from core.timezone import convert_timestamp_to_target_tz, get_target_timezone

        utc_dt = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
        result = convert_timestamp_to_target_tz(utc_dt)
        assert result.tzinfo is not None
        # UTC+7 → 07:00
        assert result.hour == 7
        assert result.day == 15

    def test_aware_preserves_instant(self):
        """Conversion changes representation but not the absolute instant."""
        from core.timezone import convert_timestamp_to_target_tz

        utc_dt = datetime(2024, 3, 10, 18, 30, 0, tzinfo=timezone.utc)
        result = convert_timestamp_to_target_tz(utc_dt)
        # Both should represent the same UTC moment
        assert result.utctimetuple()[:6] == utc_dt.utctimetuple()[:6]

    def test_already_jakarta_unchanged_value(self):
        """A datetime already in Jakarta TZ should keep same UTC value."""
        from core.timezone import convert_timestamp_to_target_tz, get_target_timezone

        jakarta_tz = get_target_timezone()
        src = datetime(2024, 1, 1, 7, 0, 0, tzinfo=jakarta_tz)
        result = convert_timestamp_to_target_tz(src)
        assert (
            result.hour == src.hour
            or result.utctimetuple()[:6] == src.utctimetuple()[:6]
        )


# ===========================================================================
# TestConvertIsoTimestampToTargetTz
# ===========================================================================


class TestConvertIsoTimestampToTargetTz:
    def test_utc_z_converted(self):
        from core.timezone import convert_iso_timestamp_to_target_tz

        result = convert_iso_timestamp_to_target_tz("2024-06-15T00:00:00Z")
        # Should contain a timezone offset, not 'Z'
        assert "+07:00" in result or "+0700" in result

    def test_no_tz_returned_as_is(self):
        from core.timezone import convert_iso_timestamp_to_target_tz

        naive_str = "2024-06-15T10:30:00"
        assert convert_iso_timestamp_to_target_tz(naive_str) == naive_str

    def test_invalid_string_returned_as_is(self):
        from core.timezone import convert_iso_timestamp_to_target_tz

        bad = "not-a-timestamp"
        assert convert_iso_timestamp_to_target_tz(bad) == bad

    def test_none_returned_as_is(self):
        from core.timezone import convert_iso_timestamp_to_target_tz

        assert convert_iso_timestamp_to_target_tz(None) is None

    def test_empty_string_returned_as_is(self):
        from core.timezone import convert_iso_timestamp_to_target_tz

        assert convert_iso_timestamp_to_target_tz("") == ""


# ===========================================================================
# TestConvertTimeToTargetTz
# ===========================================================================


class TestConvertTimeToTargetTz:
    def test_naive_time_returned_unchanged(self):
        from core.timezone import convert_time_to_target_tz

        t = dt_time(10, 30, 0)
        assert convert_time_to_target_tz(t) == t

    def test_utc_aware_time_converted(self):
        from core.timezone import convert_time_to_target_tz

        utc_time = dt_time(0, 0, 0, tzinfo=timezone.utc)
        result = convert_time_to_target_tz(utc_time)
        assert result.tzinfo is not None
        # UTC+7 → 07:00
        assert result.hour == 7


# ===========================================================================
# TestNowInTargetTz
# ===========================================================================


class TestNowInTargetTz:
    def test_returns_aware_datetime(self):
        from core.timezone import now_in_target_tz

        result = now_in_target_tz()
        assert result.tzinfo is not None

    def test_returns_current_time_roughly(self):
        from core.timezone import now_in_target_tz
        import time

        before = time.time()
        result = now_in_target_tz()
        after = time.time()
        result_ts = result.timestamp()
        assert before <= result_ts <= after


# ===========================================================================
# TestFormatSyncTimestamp
# ===========================================================================


class TestFormatSyncTimestamp:
    def test_returns_iso_string(self):
        from core.timezone import format_sync_timestamp

        result = format_sync_timestamp()
        assert isinstance(result, str)
        # Basic ISO-8601 check: contains 'T'
        assert "T" in result

    def test_includes_timezone_offset(self):
        from core.timezone import format_sync_timestamp

        result = format_sync_timestamp()
        # Should have timezone offset (+07:00, +00:00, etc.)
        assert "+" in result or result.endswith("Z")
