"""Utilities to ensure consistent handling of dates.

All datetimes should be stored as ISO 8601 in seconds in UTC.
"""

from datetime import datetime, timezone


def get_datetime_now() -> str:
    """Get datetime.now in UTC removing microseconds."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def convert_epoch_time_in_sec_to_iso(epochtime_in_sec: int) -> str:
    """Convert an epoch time in seconds to iso format.

    e.g. 1705524187 -> "2024-01-17T20:43:07+00:00"
    """
    return datetime.fromtimestamp(epochtime_in_sec, tz=timezone.utc).isoformat(timespec="seconds")


def convert_epoch_time_in_ms_to_iso(epochtime_in_ms: int) -> str:
    """Convert an epoch time in milliseconds to iso format.

    e.g. 1705524187192 -> "2024-01-17T20:43:07+00:00"
    """
    return convert_epoch_time_in_sec_to_iso(int(epochtime_in_ms / 1000))


def convert_zulu_to_iso(time_str: str) -> str:
    """Convert from Zulu time to ISO 8601.

    e.g. "2023-05-06T14:00:00Z" -> "2023-05-06T14:00:00+00:00"
    """
    time_str = time_str.replace("Z", "+00:00")
    return datetime.fromisoformat(time_str).isoformat(timespec="seconds")
