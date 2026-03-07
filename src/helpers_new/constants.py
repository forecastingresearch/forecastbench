"""Constants used by the resolve pipeline."""

from datetime import datetime
from enum import Enum

BENCHMARK_NAME = "ForecastBench"

BENCHMARK_START_YEAR = 2024
BENCHMARK_START_MONTH = 5
BENCHMARK_START_DAY = 1
BENCHMARK_START_DATE = f"{BENCHMARK_START_YEAR}-{BENCHMARK_START_MONTH}-{BENCHMARK_START_DAY}"
BENCHMARK_START_DATE_DATETIME = datetime.strptime(BENCHMARK_START_DATE, "%Y-%m-%d")
BENCHMARK_START_DATE_DATETIME_DATE = BENCHMARK_START_DATE_DATETIME.date()

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
    "url": str,
    "resolved": bool,
    "forecast_horizons": object,
    "freeze_datetime_value": str,
    "freeze_datetime_value_explanation": str,
    "market_info_resolution_criteria": str,
    "market_info_open_datetime": str,
    "market_info_close_datetime": str,
    "market_info_resolution_datetime": str,
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "date": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]


class RunMode(str, Enum):
    """Run modes for code execution.

    - TEST: Test/dev runs; use to reduce costs when running models.
    - PROD: Full production runs; execute all models with full question set.

    Construction is case-insensitive (e.g., RunMode("teST") --> RunMode.TEST).
    """

    TEST = "TEST"
    PROD = "PROD"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            return cls.__members__.get(value.upper())
        return None


TEST_FORECAST_FILE_PREFIX = RunMode.TEST.value
