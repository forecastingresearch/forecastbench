"""
Shared utilities: date/time helpers with mock support.

Set BUILD_ENV=dev and MOCK_DATE=YYYY-MM-DD to simulate a different current date.
In production (BUILD_ENV=prod or unset), always uses the real system time.
"""

import os
from datetime import datetime, timezone

BUILD_ENV = os.environ.get("BUILD_ENV", "prod")
MOCK_DATE = os.environ.get("MOCK_DATE", "")


def now_utc() -> datetime:
    """Returns current UTC time, or mocked time if BUILD_ENV=dev and MOCK_DATE is set."""
    if BUILD_ENV == "dev" and MOCK_DATE:
        try:
            d = datetime.strptime(MOCK_DATE, "%Y-%m-%d")
            return d.replace(hour=12, minute=0, second=0, tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def is_past_deadline(round_date_str: str) -> bool:
    try:
        due = datetime.strptime(round_date_str, "%Y-%m-%d")
        deadline = due.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return now_utc() > deadline
    except ValueError:
        return False
