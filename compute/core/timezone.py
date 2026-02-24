"""
Timezone utilities for Rosetta Compute Engine.

Provides centralized timezone conversion for CDC and backfill pipelines.
All timezone-aware timestamps are converted to the configured target timezone
(default: Asia/Jakarta, UTC+7) before writing to Snowflake.

Design rules:
- TIMESTAMP WITH TIME ZONE / TIMESTAMPTZ → convert to target TZ (Asia/Jakarta)
- TIMESTAMP WITHOUT TIME ZONE → keep as-is (naive, no conversion)
- TIME WITH TIME ZONE → convert to target TZ offset
- TIME WITHOUT TIME ZONE → keep as-is (no conversion)
- SYNC_TIMESTAMP_ROSETTA → always in target TZ
"""

import os
import logging
from datetime import datetime, time as dt_time, timezone, timedelta, tzinfo
from typing import Optional, Union

logger = logging.getLogger(__name__)

# Default target timezone: Asia/Jakarta (UTC+7)
_DEFAULT_TIMEZONE = "Asia/Jakarta"


def _get_target_timezone_name() -> str:
    """Get target timezone name from environment or default."""
    return os.getenv("ROSETTA_TIMEZONE", _DEFAULT_TIMEZONE)


def get_target_timezone() -> tzinfo:
    """
    Get the target timezone for timestamp conversion.

    Reads from ROSETTA_TIMEZONE env var, defaults to Asia/Jakarta (UTC+7).
    Uses zoneinfo (Python 3.9+) for full IANA timezone support.

    Returns:
        tzinfo object for the configured timezone
    """
    tz_name = _get_target_timezone_name()

    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(tz_name)
    except ImportError:
        # Fallback for environments without zoneinfo
        logger.warning(
            f"zoneinfo not available, using fixed UTC+7 offset for {tz_name}"
        )
        return timezone(timedelta(hours=7))
    except KeyError:
        logger.warning(
            f"Unknown timezone '{tz_name}', falling back to UTC+7 (Asia/Jakarta)"
        )
        return timezone(timedelta(hours=7))


def convert_timestamp_to_target_tz(dt: datetime) -> datetime:
    """
    Convert a timezone-aware datetime to the target timezone.

    If the datetime is naive (no tzinfo), it is returned as-is — we assume
    it was stored as TIMESTAMP WITHOUT TIME ZONE and should not be converted.

    Args:
        dt: datetime object (naive or aware)

    Returns:
        datetime converted to target timezone (if aware), or unchanged (if naive)
    """
    if dt.tzinfo is None:
        # Naive datetime → TIMESTAMP WITHOUT TIME ZONE → no conversion
        return dt

    target_tz = get_target_timezone()
    return dt.astimezone(target_tz)


def convert_time_to_target_tz(t: dt_time) -> dt_time:
    """
    Convert a timezone-aware time to the target timezone offset.

    TIME WITH TIME ZONE values need their offset adjusted to the target timezone.
    TIME WITHOUT TIME ZONE values are returned as-is.

    Note: TIME WITH TIME ZONE conversion is an offset shift only — there is no
    date context, so DST transitions cannot be applied. The fixed UTC offset
    of the target timezone is used.

    Args:
        t: time object (naive or aware)

    Returns:
        time converted to target timezone offset (if aware), or unchanged (if naive)
    """
    if t.tzinfo is None:
        # Naive time → TIME WITHOUT TIME ZONE → no conversion
        return t

    target_tz = get_target_timezone()

    # For TIME WITH TIME ZONE, we convert via a dummy date to handle offset math
    # Use a fixed date to avoid DST ambiguity; the date itself is discarded
    dummy_date = datetime(2000, 1, 1)
    dummy_dt = datetime.combine(dummy_date, t)

    # If the time already has tzinfo, the combine keeps it
    converted = dummy_dt.astimezone(target_tz)
    return converted.timetz()


def convert_iso_timestamp_to_target_tz(iso_str: str) -> str:
    """
    Convert an ISO-8601 timestamp string with timezone to the target timezone.

    Handles formats like:
    - "2024-01-15T10:30:00Z"               → UTC
    - "2024-01-15T10:30:00+00:00"           → UTC
    - "2024-01-15T10:30:00.123456+05:30"    → IST
    - "2024-01-15T10:30:00+07:00"           → Already Jakarta

    If the string has no timezone info, it is returned as-is.

    Args:
        iso_str: ISO-8601 timestamp string

    Returns:
        ISO-8601 string converted to target timezone, or original if no TZ
    """
    if not iso_str or not isinstance(iso_str, str):
        return iso_str

    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        # Not a valid ISO format, return as-is
        return iso_str

    if dt.tzinfo is None:
        # No timezone info → return as-is
        return iso_str

    converted = convert_timestamp_to_target_tz(dt)
    return converted.isoformat()


def convert_iso_time_to_target_tz(iso_time_str: str) -> str:
    """
    Convert an ISO-8601 time string with timezone offset to the target timezone.

    Handles formats like:
    - "14:30:00+00:00"      → UTC
    - "14:30:00+05:30"      → IST
    - "14:30:00.123456+07"  → Jakarta

    If the string has no timezone info, it is returned as-is.

    Args:
        iso_time_str: ISO time string (e.g., "14:30:00+07:00")

    Returns:
        Time string converted to target timezone offset
    """
    if not iso_time_str or not isinstance(iso_time_str, str):
        return iso_time_str

    try:
        t = dt_time.fromisoformat(iso_time_str)
    except (ValueError, AttributeError):
        return iso_time_str

    if t.tzinfo is None:
        return iso_time_str

    converted = convert_time_to_target_tz(t)
    return converted.isoformat()


def now_in_target_tz() -> datetime:
    """
    Get current datetime in the target timezone.

    Returns:
        Current datetime with target timezone info
    """
    target_tz = get_target_timezone()
    return datetime.now(target_tz)


def format_sync_timestamp() -> str:
    """
    Get current timestamp formatted for SYNC_TIMESTAMP_ROSETTA column.

    Always returns the current time in the configured target timezone
    (default: Asia/Jakarta) in ISO-8601 format.

    Returns:
        ISO-8601 formatted timestamp string with target timezone offset
    """
    return now_in_target_tz().isoformat()
