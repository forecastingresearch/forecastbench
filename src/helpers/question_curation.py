"""Info relevant to selecting questions."""

import os
from datetime import timedelta

from . import (
    acled,
    dates,
    dbnomics,
    fred,
    infer,
    manifold,
    metaculus,
    polymarket,
    wikipedia,
    yfinance,
)

FREEZE_NUM_LLM_QUESTIONS = 1000
FREEZE_NUM_HUMAN_QUESTIONS = 200

# Assumed in the code
assert FREEZE_NUM_LLM_QUESTIONS > FREEZE_NUM_HUMAN_QUESTIONS


FREEZE_QUESTION_MARKET_SOURCES = {
    # If market sources are ever removed, the key must be added to MARKET_SOURCES in
    # `helpers/resolution.py` as the resolution code needs all old market sources.
    "manifold": {
        "name": "Manifold",
        "source_intro": manifold.SOURCE_INTRO,
        "resolution_criteria": manifold.RESOLUTION_CRITERIA,
    },
    "metaculus": {
        "name": "Metaculus",
        "source_intro": metaculus.SOURCE_INTRO,
        "resolution_criteria": metaculus.RESOLUTION_CRITERIA,
    },
    "infer": {
        "name": "INFER",
        "source_intro": infer.SOURCE_INTRO,
        "resolution_criteria": infer.RESOLUTION_CRITERIA,
    },
    "polymarket": {
        "name": "Polymarket",
        "source_intro": polymarket.SOURCE_INTRO,
        "resolution_criteria": polymarket.RESOLUTION_CRITERIA,
    },
}

FREEZE_QUESTION_DATA_SOURCES = {
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
    "wikipedia": {
        "name": "Wikipedia",
        "source_intro": wikipedia.SOURCE_INTRO,
        "resolution_criteria": wikipedia.RESOLUTION_CRITERIA,
    },
    "yfinance": {
        "name": "Yahoo Finance",
        "source_intro": yfinance.SOURCE_INTRO,
        "resolution_criteria": yfinance.RESOLUTION_CRITERIA,
    },
}

FREEZE_QUESTION_SOURCES = {**FREEZE_QUESTION_MARKET_SOURCES, **FREEZE_QUESTION_DATA_SOURCES}

DATA_SOURCES = list(FREEZE_QUESTION_DATA_SOURCES.keys())
MARKET_SOURCES = list(FREEZE_QUESTION_MARKET_SOURCES.keys())
ALL_SOURCES = DATA_SOURCES + MARKET_SOURCES

FREEZE_WINDOW_IN_DAYS = 10

FREEZE_DATETIME = os.environ.get("FREEZE_DATETIME", dates.get_datetime_today()).replace(
    hour=0, minute=0, second=0, microsecond=0
)

FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=FREEZE_WINDOW_IN_DAYS)

FORECAST_DATE = FORECAST_DATETIME.date()

COMBINATION_PROMPT = (
    "We are presenting you with two probability questions. Please predict the probability that both "
    "will happen, that one will happen but not the other, and that neither will happen. In other "
    "words, for each resolution date please provide 4 predictions."
)
