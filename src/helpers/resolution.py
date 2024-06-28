"""Helper functions for resolution code."""

import json
import logging
import os
import sys
from typing import Union

import pandas as pd
from tqdm import tqdm

from . import (  # noqa:F401
    acled,
    constants,
    data_utils,
    dates,
    env,
    question_curation,
    wikipedia,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Store here a growing list of all market sources ever used to allow for future resolution even when
# a source is dropped from question curation.
MARKET_SOURCES = sorted(
    {"manifold", "metaculus", "infer", "polymarket"}.union(set(question_curation.MARKET_SOURCES))
)

# Store here a growing list of all data sources ever used to allow for future resolution even when
# a source is dropped from question curation.
DATA_SOURCES = sorted(
    {"acled", "dbnomics", "fred", "wikipedia", "yfinance"}.union(
        set(question_curation.DATA_SOURCES)
    )
)


def split_dataframe_on_source(df, source):
    """Return tuple of this data source from dataframe and everything else."""
    mask = df["source"] == source
    return df[mask].copy(), df[~mask].copy()


def is_combo(row):
    """Tell whether or not id is a combo question."""
    if isinstance(row, pd.Series) and "id" in row.index:
        return isinstance(row["id"], tuple)
    elif isinstance(row, str) or isinstance(row, tuple):
        return isinstance(row, tuple)
    raise ValueError(f"Problem in `is_combo` with {row}. This type is not handled: {type(row)}")


def combo_change_sign(value: Union[bool, int, float], sign: int):
    """Change direction of bool value given sign (-1 or 1)."""
    if sign not in (1, -1):
        raise ValueError(f"Wrong value for sign: {sign}")
    return value if sign == 1 else 1 - value


def get_question(dfq, mid):
    """Get question from dfq."""
    dftmp = dfq[dfq["id"] == mid]
    return None if dftmp.empty else dftmp.iloc[0]


def make_list_hashable(df, col):
    """Turn list into tuple to make it hashable."""
    df[col] = df[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df[col] = df[col].apply(lambda x: tuple() if pd.isna(x) else x)
    return df


def make_columns_hashable(df):
    """Make columns that have array type into tuples."""
    for col in ["id", "direction"]:
        if col in df.columns:
            df = make_list_hashable(df, col)
    return df


def standardize_direction(val):
    """Try to standardize the value in direction which is given by the user."""
    return tuple() if val is None or val == "N/A" else val


def make_resolution_df(source):
    """Prepare data for resolution."""
    files = gcp.storage.list_with_prefix(bucket_name=env.QUESTION_BANK_BUCKET, prefix=source)
    df = pd.concat(
        [
            pd.read_json(
                f"gs://{env.QUESTION_BANK_BUCKET}/{f}",
                lines=True,
                dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                convert_dates=False,
            )
            for f in tqdm(files, f"downloading `{source}` resoultion files")
            if f.startswith(f"{source}/")
        ],
        ignore_index=True,
    )
    df = make_columns_hashable(df)
    df["date"] = pd.to_datetime(df["date"])
    df["id"] = df["id"].astype(str)
    return df


def get_resolution_values(sources_to_get=question_curation.ALL_SOURCES):
    """Get resolution values from GCP.

    For each source, create dfr (resolutions) and dfq (questions).

    Parameters:
    sources_to_get (list): list of sources to get. If empty, get all sources.
    """

    def get_dfr(source):
        if source in ["acled", "wikipedia"]:
            return globals()[source].make_resolution_df()
        return make_resolution_df(source)

    def get_sources(sources):
        """Download dfq and dfr for all sources.

        1. Check last updated timestamp for market-based dfq files. If any is < today, stop
           processing to save time.
        2. Download dfq and dfr for every source.
        """
        today = dates.get_date_today()
        any_out_of_date = False
        for source in set(sources).intersection(MARKET_SOURCES):
            last_updated = data_utils.get_last_modified_time_of_dfq_from_cloud_storage(
                source=source
            )
            if last_updated.date() < today:
                logger.error(
                    f"ERROR: dfq for `{source}` is out of date. "
                    f"dfq was last updated {last_updated.date()} but today is {today}. "
                    "Run fetch/update."
                )
                any_out_of_date = True

        if any_out_of_date:
            raise ValueError("Market-based dfq files need updating.")

        return {
            source: {
                "dfr": get_dfr(source),
                "dfq": data_utils.get_data_from_cloud_storage(
                    source=source, return_question_data=True
                ),
            }
            for source in sources
        }

    return get_sources(sources=sources_to_get)


def download_and_read_question_set_file(filename, run_locally=False):
    """Download question set file."""
    local_filename = filename
    if not run_locally:
        local_filename = "/tmp/tmp.json"
        gcp.storage.download(
            bucket_name=env.QUESTION_SETS_BUCKET, filename=filename, local_filename=local_filename
        )

    questions = None
    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
        questions = data.get("questions")

    if questions is None:
        raise ValueError(
            "In `resolution.download_and_read_question_set_file()`: Could not download/load "
            f"question set {filename}"
        )

    df = pd.DataFrame(questions)
    df = make_columns_hashable(df)
    return df
