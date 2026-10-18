"""Info relevant to selecting questions."""

import math
import os
from datetime import timedelta

from . import constants, dates

# The sources we sample questions from, with each source's share of its question type's target.
# Dropping a source here stops sampling it without affecting resolution: `helpers/resolution.py`
# takes its source lists from `sources/_metadata.py`, which holds every source we've ever published
# questions for. A source's intro and resolution criteria come from `sources/_metadata.py` too,
# through the source registry.
FREEZE_QUESTION_MARKET_SOURCES = {
    "metaculus": {"sampling_share": 1 / 6},
    "kalshi": {"sampling_share": 5 / 12},
    "polymarket": {"sampling_share": 5 / 12},
}

FREEZE_QUESTION_DATA_SOURCES = {
    "acled": {"sampling_share": 0.2},
    "dbnomics": {"sampling_share": 0.1},
    "fred": {"sampling_share": 0.2},
    "serpapi": {"sampling_share": 0.3},
    "yfinance": {"sampling_share": 0.2},
}

FREEZE_QUESTION_SOURCES = {**FREEZE_QUESTION_MARKET_SOURCES, **FREEZE_QUESTION_DATA_SOURCES}

for _sources in (FREEZE_QUESTION_MARKET_SOURCES, FREEZE_QUESTION_DATA_SOURCES):
    assert math.isclose(sum(source["sampling_share"] for source in _sources.values()), 1), (
        "sampling shares within a source type must sum to 1"
    )

DATA_SOURCES = list(FREEZE_QUESTION_DATA_SOURCES.keys())
MARKET_SOURCES = list(FREEZE_QUESTION_MARKET_SOURCES.keys())

FREEZE_WINDOW_IN_DAYS = 10

FREEZE_DATETIME = os.environ.get("FREEZE_DATETIME", dates.get_datetime_today()).replace(
    hour=0, minute=0, second=0, microsecond=0
)

FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=FREEZE_WINDOW_IN_DAYS)

FORECAST_DATE = FORECAST_DATETIME.date()


def get_num_days_since_original_forecast_due_date():
    """Return the number of days since the original forecast due date.

    The original forecast due date is the day the original question set was published.
    """
    return (dates.get_date_today() - constants.BENCHMARK_TOURNAMENT_START_DATE_DATETIME_DATE).days


def is_today_question_set_publication_date():
    """Return true if today is the day to publish the question set.

    This is done every 2 weeks since the original benchamrk question set was published.
    """
    return get_num_days_since_original_forecast_due_date() % 14 == 0


def is_today_question_curation_date():
    """Return true if today is the day to curate questions.

    This is done every 2 weeks - FREEZE_WINDOW_IN_DAYS since the original benchamrk question set was
    created.
    """
    return get_num_days_since_original_forecast_due_date() % 14 == 14 - FREEZE_WINDOW_IN_DAYS
