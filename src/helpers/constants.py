"""Constants."""

import os
from datetime import timedelta

from . import dates, prompts, resolutions

BENCHMARK_START_YEAR = 2024

BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")
PUBLIC_BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET_QUESTIONS")
PROJECT_ID = os.environ.get("CLOUD_PROJECT")

FREEZE_NUM_LLM_QUESTIONS = 1000

FREEZE_NUM_HUMAN_QUESTIONS = 200

# Assumed in the code
assert FREEZE_NUM_LLM_QUESTIONS > FREEZE_NUM_HUMAN_QUESTIONS

FREEZE_QUESTION_SOURCES = {
    "manifold": {
        "name": "Manifold",  # Name to use in the human prompt
        "human_prompt": prompts.market,  # The human prompt to use
        "resolution_criteria": resolutions.market,
    },
    "metaculus": {
        "name": "Metaculus",
        "human_prompt": prompts.market,
        "resolution_criteria": resolutions.metaculus,
    },
    "acled": {
        "name": "ACLED",
        "human_prompt": prompts.acled,
        "resolution_criteria": resolutions.acled,
    },
    "infer": {
        "name": "INFER",
        "human_prompt": prompts.market,
        "resolution_criteria": resolutions.infer,
    },
}

FREEZE_WINDOW_IN_DAYS = 7

FREEZE_DATETIME = os.environ.get("FREEZE_DATETIME", dates.get_datetime_today()).replace(
    hour=0, minute=0, second=0, microsecond=0
)

FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=FREEZE_WINDOW_IN_DAYS)

FORECAST_DATE = FORECAST_DATETIME.date()

FORECAST_HORIZONS_IN_DAYS = [
    7,  # 1 week
    30,  # 1 month
    90,  # 3 months
    180,  # 6 months
    365,  # 1 year
    1095,  # 3 years
    1825,  # 5 years
    3650,  # 10 years
]

QUESTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "question": str,
    "background": str,
    "source_resolution_criteria": str,
    "source_begin_datetime": str,
    "source_close_datetime": str,
    "url": str,
    "source_resolution_datetime": str,
    "resolved": bool,
    "continual_resolution": bool,
    "forecast_horizons": object,  # list<int>
    "value_at_freeze_datetime": str,
    "value_at_freeze_datetime_explanation": str,
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "date": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]

MANIFOLD_TOPIC_SLUGS = ["entertainment", "sports-default", "technology-default"]

METACULUS_CATEGORIES = [
    "geopolitics",
    "natural-sciences",
    "sports-entertainment",
    "health-pandemics",
    "law",
    "computing-and-math",
]

ACLED_FETCH_COLUMN_DTYPE = {
    "event_id_cnty": str,
    "event_date": str,
    "iso": int,
    "region": str,
    "country": str,
    "admin1": str,
    "event_type": str,
    "fatalities": int,
    "timestamp": str,
}
ACLED_FETCH_COLUMNS = list(ACLED_FETCH_COLUMN_DTYPE.keys())

ACLED_QUESTION_FILE_COLUMN_DTYPE = {
    **QUESTION_FILE_COLUMN_DTYPE,
    "lhs_func": str,
    "lhs_args": object,  # <dict>
    "comparison_operator": str,
    "rhs_func": str,
    "rhs_args": object,  # <dict>
}
ACLED_QUESTION_FILE_COLUMNS = list(ACLED_QUESTION_FILE_COLUMN_DTYPE.keys())
