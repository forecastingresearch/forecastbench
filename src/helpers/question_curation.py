"""Info relevant to selecting questions."""

import os
from datetime import timedelta

from . import (
    acled,
    constants,
    dates,
    dbnomics,
    fred,
    kalshi,
    metaculus,
    polymarket,
    serpapi,
)

FREEZE_NUM_LLM_QUESTIONS = 500
FREEZE_NUM_HUMAN_QUESTIONS = 200

# How each question set is split between market and dataset questions. Dataset questions resolve
# at several horizons, so the human set leans toward market questions to gain power (issue #310).
FREEZE_NUM_LLM_QUESTIONS_BY_TYPE = {"market": 250, "dataset": 250}
FREEZE_NUM_HUMAN_QUESTIONS_BY_TYPE = {"market": 130, "dataset": 70}

assert sum(FREEZE_NUM_LLM_QUESTIONS_BY_TYPE.values()) == FREEZE_NUM_LLM_QUESTIONS
assert sum(FREEZE_NUM_HUMAN_QUESTIONS_BY_TYPE.values()) == FREEZE_NUM_HUMAN_QUESTIONS

# Assumed in the code: human questions are sampled from the LLM set.
assert all(
    FREEZE_NUM_HUMAN_QUESTIONS_BY_TYPE[t] < FREEZE_NUM_LLM_QUESTIONS_BY_TYPE[t]
    for t in FREEZE_NUM_LLM_QUESTIONS_BY_TYPE
)


FREEZE_QUESTION_MARKET_SOURCES = {
    # The market sources we sample questions from. Dropping a source here stops sampling it
    # without affecting resolution: `helpers/resolution.py` takes its source lists from
    # `sources/_metadata.py`, which holds every market source we've ever published questions for.
    "metaculus": {
        "name": "Metaculus",
        "source_intro": metaculus.SOURCE_INTRO,
        "resolution_criteria": metaculus.RESOLUTION_CRITERIA,
    },
    "kalshi": {
        "name": "Kalshi",
        "source_intro": kalshi.SOURCE_INTRO,
        "resolution_criteria": kalshi.RESOLUTION_CRITERIA,
    },
    "polymarket": {
        "name": "Polymarket",
        "source_intro": polymarket.SOURCE_INTRO,
        "resolution_criteria": polymarket.RESOLUTION_CRITERIA,
    },
}

FREEZE_QUESTION_DATA_SOURCES = {
    "serpapi": {
        "name": "SerpAPI",
        "source_intro": serpapi.SOURCE_INTRO,
        "resolution_criteria": serpapi.RESOLUTION_CRITERIA,
    },
    "acled": {
        "name": "ACLED",
        "source_intro": acled.SOURCE_INTRO,
        "resolution_criteria": acled.RESOLUTION_CRITERIA,
    },
    "dbnomics": {
        "name": "DBnomics",
        "source_intro": dbnomics.SOURCE_INTRO,
        "resolution_criteria": dbnomics.RESOLUTION_CRITERIA,
    },
    "fred": {
        "name": "FRED",
        "source_intro": fred.SOURCE_INTRO,
        "resolution_criteria": fred.RESOLUTION_CRITERIA,
    },
}

FREEZE_QUESTION_SOURCES = {**FREEZE_QUESTION_MARKET_SOURCES, **FREEZE_QUESTION_DATA_SOURCES}

DATA_SOURCES = list(FREEZE_QUESTION_DATA_SOURCES.keys())
MARKET_SOURCES = list(FREEZE_QUESTION_MARKET_SOURCES.keys())

QUESTION_TYPE_SOURCES = {"market": MARKET_SOURCES, "dataset": DATA_SOURCES}
assert QUESTION_TYPE_SOURCES.keys() == FREEZE_NUM_LLM_QUESTIONS_BY_TYPE.keys()
assert QUESTION_TYPE_SOURCES.keys() == FREEZE_NUM_HUMAN_QUESTIONS_BY_TYPE.keys()

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
