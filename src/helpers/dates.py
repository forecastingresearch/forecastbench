"""Utilities to ensure consistent handling of dates."""

from datetime import datetime, timezone


def get_datetime_now() -> str:
    """Get datetime.now in UTC removing microseconds."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
