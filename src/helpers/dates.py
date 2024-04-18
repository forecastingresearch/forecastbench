"""Utilities to ensure consistent handling of dates.

All datetimes should be stored as ISO 8601 in seconds in UTC.
"""

from datetime import datetime, timezone

import pytz


def get_datetime_today():
    """Get datetime.now in UTC."""
    return datetime.now(timezone.utc)


def get_datetime_today_midnight():
    """Get datetime.now in UTC at midnight."""
    return get_datetime_today().replace(hour=0, minute=0, second=0, microsecond=0)


def get_datetime_now() -> str:
    """Get datetime.now in UTC removing microseconds."""
    return get_datetime_today().isoformat(timespec="seconds")


def convert_epoch_time_in_sec_to_iso(epochtime_in_sec: int) -> str:
    """Convert an epoch time in seconds to iso format.

    e.g. 1705524187 -> "2024-01-17T20:43:07+00:00"
    """
    return datetime.fromtimestamp(epochtime_in_sec, tz=timezone.utc).isoformat(timespec="seconds")


def convert_epoch_time_in_sec_to_datetime(epoch):
    """Convert an epoch time in seconds to datetime object.

    e.g. 1705524187 -> datetime.datetime(2024, 1, 17, 20, 43, 7, tzinfo=<UTC>)
    """
    return datetime.fromtimestamp(epoch, pytz.utc)


def convert_epoch_time_in_ms_to_iso(epochtime_in_ms: int) -> str:
    """Convert an epoch time in milliseconds to iso format.

    e.g. 1705524187192 -> "2024-01-17T20:43:07+00:00"
    """
    return convert_epoch_time_in_sec_to_iso(int(epochtime_in_ms / 1000))


def convert_epoch_in_ms_to_datetime(epoch):
    """Convert an epoch time in seconds to datetime object.

    e.g. 1705524187192 -> datetime.datetime(2024, 1, 17, 20, 43, 7, tzinfo=<UTC>)
    """
    return convert_epoch_time_in_sec_to_datetime(int(epoch / 1000))


def convert_zulu_to_datetime(time_str: str) -> datetime:
    """Convert from Zulu time to datetime object.

    e.g. "2023-05-06T14:00:00Z" -> datetime.datetime(2023, 5, 6, 14, 0, tzinfo=datetime.timezone.utc)
    """
    return datetime.fromisoformat(time_str)


def convert_zulu_to_iso(time_str: str) -> str:
    """Convert from Zulu time to ISO 8601.

    e.g. "2023-05-06T14:00:00Z" -> "2023-05-06T14:00:00+00:00"
    """
    return convert_zulu_to_datetime(time_str).isoformat(timespec="seconds")
