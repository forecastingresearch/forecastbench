"""ACLED-specific variables."""

from . import constants

FETCH_COLUMN_DTYPE = {
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
FETCH_COLUMNS = list(FETCH_COLUMN_DTYPE.keys())

QUESTION_FILE_COLUMN_DTYPE = {
    **constants.QUESTION_FILE_COLUMN_DTYPE,
    "lhs_func": str,
    "lhs_args": object,  # <dict>
    "comparison_operator": str,
    "rhs_func": str,
    "rhs_args": object,  # <dict>
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())
