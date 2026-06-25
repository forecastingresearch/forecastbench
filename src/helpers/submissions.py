"""Submission pipeline date utilities."""

import os
from datetime import datetime, timezone

from constants import RunMode

RUN_MODE = RunMode(os.environ.get("RUN_MODE", "PROD"))
MOCK_DATE = os.environ.get("MOCK_DATE", "")


def get_datetime_now_utc() -> datetime:
    """Get current UTC datetime, with mock-date support in TEST mode."""
    if RUN_MODE == RunMode.TEST and MOCK_DATE:
        try:
            return datetime.strptime(MOCK_DATE, "%Y-%m-%d").replace(
                hour=12, minute=0, second=0, tzinfo=timezone.utc
            )
        except ValueError:
            pass
    from dates import get_datetime_today

    return get_datetime_today()


def is_past_deadline(round_date_str: str) -> bool:
    """Return True if the current UTC time is past 23:59:59 on round_date_str."""
    try:
        due = datetime.strptime(round_date_str, "%Y-%m-%d")
        deadline = due.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return get_datetime_now_utc() > deadline
    except ValueError:
        return False
