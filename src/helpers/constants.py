"""Constants."""

import os

from . import dates

BENCHMARK_START_YEAR = 2024

BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")
PROJECT_ID = os.environ.get("CLOUD_PROJECT")

FREEZE_WINDOW_IN_DAYS = 7

FREEZE_DATETIME = os.environ.get("FREEZE_DATETIME", dates.get_datetime_today()).replace(
    hour=0, minute=0, second=0, microsecond=0
)

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
    "begin_datetime": str,
    "close_datetime": str,
    "url": str,
    "resolution_datetime": str,
    "resolved": bool,
    "continual_resolution": bool,
    "forecast_horizons": object,  # list<int>
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
    "lhs_args": dict,
    "comparison_operator": str,
    "rhs_func": str,
    "rhs_args": dict,
}
ACLED_QUESTION_FILE_COLUMNS = list(ACLED_QUESTION_FILE_COLUMN_DTYPE.keys())
