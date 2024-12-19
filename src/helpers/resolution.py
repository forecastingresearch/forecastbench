"""Helper functions for resolution code."""

import json
import logging
import os
import pickle
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Union

import numpy as np
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


def _is_combo_question_resolved_helper(is_resolved, direction, resolved_to):
    """Determine whether or not a combo question has resolved for a single direction."""
    return is_resolved and (
        (direction == 1 and not resolved_to) or (direction == -1 and resolved_to)
    )


def is_combo_question_resolved(is_resolved0, is_resolved1, dir0, dir1, resolved_to0, resolved_to1):
    """Determine whether or not a combo question has resolved.

    Combo questions are asked in 4 directions: (1,1), (1,-1), (-1,1), (-1,-1).

    If neither question has resolved, the combo question has not resolved. If both have resolved,
    the combo question has resolved.

    However, if only one question has resolved, then 2 of the 4 directions of the combo question
    have resolved. e.g. if q2 resolves No, then questions with directions (1,1) and (-1,1) have
    resolved to 0; no matter the outcome of q1, the score for these two questions will not change.
    """
    if (is_resolved0 and is_resolved1) or np.isnan(resolved_to0) or np.isnan(resolved_to1):
        return True
    return bool(
        _is_combo_question_resolved_helper(
            is_resolved=is_resolved0,
            direction=dir0,
            resolved_to=resolved_to0,
        )
        or _is_combo_question_resolved_helper(
            is_resolved=is_resolved1,
            direction=dir1,
            resolved_to=resolved_to1,
        )
    )


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
    files = [
        f
        for f in gcp.storage.list_with_prefix(bucket_name=env.QUESTION_BANK_BUCKET, prefix=source)
        if f.startswith(f"{source}/")
    ]
    with ThreadPoolExecutor() as executor:
        dfs = list(
            tqdm(
                executor.map(
                    lambda f: pd.read_json(
                        f"gs://{env.QUESTION_BANK_BUCKET}/{f}",
                        lines=True,
                        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                        convert_dates=False,
                    ),
                    files,
                ),
                total=len(files),
                desc=f"downloading `{source}` resolution files",
            )
        )
        executor.shutdown(wait=True)

    df = pd.concat(dfs, ignore_index=True)
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
            if last_updated is None or last_updated.date() < today:
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


def get_and_pickle_resolution_values(filename, save_pickle_file=False, sources_to_get=None):
    """Get and pickle dfr and dfq from GCP so that we can avoid doing this on every run.

    If `sources_to_get` is passed, only get dfr and dfq for these sources. Update the existing .pkl
    file. NB: this is only used if a resolution file already exists.

    save_pickle_file should only be set to True when working locally; never save pickle file on
    Cloud as we always want the latest data.
    """
    resolution_values = None
    if os.path.exists(filename):
        with open(filename, "rb") as handle:
            resolution_values = pickle.load(handle)

        if sources_to_get:
            resolution_values_tmp = get_resolution_values(sources_to_get=sources_to_get)
            if resolution_values is not None and isinstance(resolution_values, dict):
                resolution_values.update(resolution_values_tmp)
            else:
                resolution_values = resolution_values_tmp

            if save_pickle_file:
                with open(filename, "wb") as handle:
                    pickle.dump(resolution_values, handle)
    else:
        resolution_values = get_resolution_values()
        if save_pickle_file:
            with open(filename, "wb") as handle:
                pickle.dump(resolution_values, handle)
    return resolution_values


def download_and_read_question_set_file(filename, run_locally=False):
    """Download question set file."""
    local_filename = filename
    if not run_locally:
        with tempfile.NamedTemporaryFile(dir="/tmp/", delete=False) as tmp:
            local_filename = tmp.name
        gcp.storage.download(
            bucket_name=env.QUESTION_SETS_BUCKET,
            filename=filename,
            local_filename=local_filename,
        )

    questions = None
    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
        questions = data.get("questions")

    if not run_locally:
        os.remove(local_filename)

    if questions is None:
        raise ValueError(
            "In `resolution.download_and_read_question_set_file()`: Could not download/load "
            f"question set {filename}"
        )

    df = pd.DataFrame(questions)
    df = make_columns_hashable(df)
    return df


def get_field_from_question_set_file(filename, field):
    """Download value in `field` from question set `filename`."""
    with tempfile.NamedTemporaryFile(mode="r+", suffix=".json", dir="/tmp") as tmp:
        gcp.storage.download(
            bucket_name=env.QUESTION_SETS_BUCKET,
            filename=filename,
            local_filename=tmp.name,
        )

        retval = json.load(tmp).get(field)
        if not retval:
            raise ValueError(f"`{field}` not found in {filename}.")
        return retval
