"""Helper functions for resolution code."""

import logging

import pandas as pd

from sources import ALL_SOURCE_NAMES as ALL_SOURCES  # noqa: F401
from sources import DATASET_SOURCE_NAMES as DATA_SOURCES  # noqa: F401
from sources import MARKET_SOURCE_NAMES as MARKET_SOURCES  # noqa: F401
from sources._base import BaseSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def split_dataframe_on_source(df, source):
    """Return tuple of this data source from dataframe and everything else."""
    mask = df["source"] == source
    return df[mask].copy(), df[~mask].copy()


def is_combo(row):
    """Check whether a row represents a combo question. Delegates to BaseSource."""
    return BaseSource._is_combo(row)


def make_columns_hashable(df):
    """Make columns that have array type into tuples. Delegates to BaseSource."""
    return BaseSource._make_columns_hashable(df)


def get_resolution_values() -> dict[str, dict[str, pd.DataFrame]]:
    """Get resolution values from GCP. Delegates to orchestration._io.load_question_bank().

    Returns:
        {source: {"dfq": pd.DataFrame, "dfr": pd.DataFrame}} by source.
    """
    from orchestration._io import load_question_bank

    question_bank = load_question_bank(sources_to_get=ALL_SOURCES)
    return {source: {"dfq": sqb.dfq, "dfr": sqb.dfr} for source, sqb in question_bank.items()}


def download_and_read_question_set_file(filename, run_locally=False):
    """Download question set file. Delegates to orchestration._io."""
    from orchestration._io import download_and_read_question_set_file as _download

    return _download(filename, run_locally=run_locally)


def get_valid_forecast_files_and_dates(bucket, only_keep_date=""):
    """Return valid forecast filenames from bucket. Delegates to orchestration._io."""
    from orchestration._io import get_valid_forecast_files_and_dates as _get

    return _get(bucket, only_keep_date=only_keep_date)


def read_forecast_file(filename, f=None):
    """Read a forecast JSON file and validate its content. Delegates to orchestration._io."""
    from orchestration._io import read_forecast_file as _read

    return _read(filename, f=f)
