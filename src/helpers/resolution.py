"""Helper functions for resolution code."""

import json
import logging
import os
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, List, Optional, TextIO, Tuple, Union

import numpy as np
import pandas as pd
from termcolor import colored
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

ALL_SOURCES = MARKET_SOURCES + DATA_SOURCES


def split_dataframe_on_source(df, source):
    """Return tuple of this data source from dataframe and everything else."""
    mask = df["source"] == source
    return df[mask].copy(), df[~mask].copy()


def get_market_resolution_date(row):
    """Return the minimum of the market close date and the resolution date.

    This is used to create the resolution file. What we care about is when the market closed or, if
    resolution happened before the close date, then the resolution date.
    """

    def to_date_or_max(s):
        """Convert a string representation of a date to a date.

        If not able to convert, e.g. "N/A" is passed, return the max date.
        """
        try:
            return dates.convert_zulu_to_datetime(s).date()
        except (ValueError, TypeError):
            return date.max

    close_date = to_date_or_max(row["market_info_close_datetime"].iloc[0])
    resolution_date = to_date_or_max(row["market_info_resolution_datetime"].iloc[0])
    return min(close_date, resolution_date)


def is_combo(row):
    """Tell whether or not id is a combo question."""
    if isinstance(row, pd.Series) and "id" in row.index:
        return isinstance(row["id"], tuple)
    elif isinstance(row, str) or isinstance(row, tuple):
        return isinstance(row, tuple)
    raise ValueError(f"Problem in `is_combo` with {row}. This type is not handled: {type(row)}")


def get_combo_question_resolution_date(
    is_resolved0,
    is_resolved1,
    dir0,
    dir1,
    resolved_to0,
    resolved_to1,
    resolution_date0,
    resolution_date1,
):
    """Return the resolution date if a combo question has resolved. Return None otherwise."""
    try:
        return _get_combo_question_resolution_date_helper(
            is_resolved0,
            is_resolved1,
            dir0,
            dir1,
            resolved_to0,
            resolved_to1,
            resolution_date0,
            resolution_date1,
        )
    except ValueError:
        pass
    return None


def _get_combo_question_resolution_date_helper(
    is_resolved0,
    is_resolved1,
    dir0,
    dir1,
    resolved_to0,
    resolved_to1,
    resolution_date0,
    resolution_date1,
):
    """Determine when a combo forecast question is resolved based on two sub-questions.

    Combo questions are asked in 4 directions: (1,1), (1,-1), (-1,1), (-1,-1).

    If neither question has resolved, the combo question has not resolved. If both have resolved,
    the combo question has resolved.

    However, if only one question has resolved, then 2 of the 4 directions of the combo question may
    have resolved, depending on the direction of the forecast and the direction of resolution.
    e.g. if q2 resolves No, then questions with directions (1,1) and (-1,1) have resolved to 0; no
    matter the outcome of q1, the score for these two questions will not change.
    """
    if not is_resolved0 and not is_resolved1:
        return None

    def same_dir(is_resolved, direction, resolved_to):
        return bool(
            is_resolved
            and ((direction == 1 and resolved_to == 1) or (direction == -1 and resolved_to == 0))
        )

    def diff_dir(is_resolved, direction, resolved_to):
        return bool(
            is_resolved
            and ((direction == 1 and resolved_to == 0) or (direction == -1 and resolved_to == 1))
        )

    zero_same_dir = same_dir(is_resolved0, dir0, resolved_to0)
    zero_diff_dir = diff_dir(is_resolved0, dir0, resolved_to0)
    one_same_dir = same_dir(is_resolved1, dir1, resolved_to1)
    one_diff_dir = diff_dir(is_resolved1, dir1, resolved_to1)

    # When one or more questions resolve NaN
    if np.isnan(resolved_to0) and np.isnan(resolved_to1):
        return min(resolution_date0, resolution_date1)
    elif np.isnan(resolved_to0):
        if one_diff_dir:
            return min(resolution_date0, resolution_date1)
        else:
            return resolution_date0
    elif np.isnan(resolved_to1):
        if zero_diff_dir:
            return min(resolution_date0, resolution_date1)
        else:
            return resolution_date1

    # When no questions resolve NaN
    # When both questions have resolved
    if zero_same_dir and one_same_dir:
        return max(resolution_date0, resolution_date1)

    if zero_diff_dir and one_diff_dir:
        return min(resolution_date0, resolution_date1)

    if zero_same_dir and one_diff_dir:
        return resolution_date1

    if one_same_dir and zero_diff_dir:
        return resolution_date0

    # When only one question has resolved
    if zero_diff_dir:
        return resolution_date0

    if one_diff_dir:
        return resolution_date1

    raise ValueError(
        "\n\nCombo question should have a resolution date:\n"
        f"{(zero_same_dir, zero_diff_dir, is_resolved0, dir0, resolved_to0)}\n"
        f"{(one_same_dir, one_diff_dir, is_resolved1, dir1, resolved_to1)}\n\n"
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


def get_resolution_values() -> Dict[str, Dict[str, pd.DataFrame]]:
    """Get resolution values from GCP.

    For each source, create dfr (resolutions) and dfq (questions).

    Args:
       None

    Returns:
        retval (Dict[str, Dict[str, pd.DataFrame]]): dfq and dfr by source.
    """
    logger.info("Getting resolution values...")
    today = dates.get_date_today()
    any_out_of_date_dfq = False
    for source in MARKET_SOURCES:
        last_updated_dfq = data_utils.get_last_modified_time_of_dfq_from_cloud_storage(
            source=source
        )
        any_out_of_date_dfq |= last_updated_dfq is None or last_updated_dfq.date() < today
        if last_updated_dfq is None or last_updated_dfq.date() < today:
            last_updated = last_updated_dfq.date() if last_updated_dfq else "(does not exist)"

            logger.error(
                colored(
                    f"ERROR: dfq for `{source}` is out of date. "
                    f"dfq was last updated {last_updated} but today is {today}. "
                    "Run fetch/update.",
                    "red",
                )
            )

    last_updated_tarball = gcp.storage.get_last_modified_time(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=f"{env.QUESTION_BANK_BUCKET}.tar.gz",
    )
    if last_updated_tarball is None or last_updated_tarball.date() < today:
        last_updated = last_updated_tarball.date() if last_updated_tarball else "(does not exist)"
        logger.warning(
            colored(
                f"WARNING: Question Bank tarball is out of date."
                f"dfq was last updated {last_updated} but today is {today}. "
                "Run fetch/update.",
                "yellow",
            )
        )

    if any_out_of_date_dfq:
        raise ValueError("Market-based dfq files need updating.")

    dfs = get_and_unpack_question_bank(sources_to_get=ALL_SOURCES)
    logger.info("Done!")
    return dfs


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


def get_valid_forecast_files_and_dates(
    bucket: str,
    only_keep_date: str = "",
) -> Tuple[List[str], List[str]]:
    """Return valid processed forecast filenames based on inclusion criteria from bucket.

    Args:
        bucket (str): The GCP bucket to pull forecast files from
        only_keep_date (str): If provided, only include files starting with this date.

    Returns:
        tuple(List[str], List[str]): Filenames that meet the age or date-prefix requirements, and
                                     the folders they're in.
    """
    files = gcp.storage.list(bucket_name=bucket, mnt=env.BUCKET_MOUNT_POINT)
    files = [
        f
        for f in files
        if f.endswith(".json") and not f.startswith(constants.TEST_FORECAST_FILE_PREFIX)
    ]
    if only_keep_date:
        return [f for f in files if f.startswith(only_keep_date)]

    # Get unique, valid, date-named folders
    date_folders = set()
    for f in files:
        if "/" not in f:
            continue
        try:
            folder = f.split("/", 1)[0]
            datetime.strptime(folder, "%Y-%m-%d")
            date_folders.add(folder)
        except ValueError:
            raise ValueError(f"Problem with file organizaiton on {bucket}")

    files = [f for f in files if f.split("/")[0] in date_folders]
    return files, sorted(date_folders)


def read_forecast_file(filename: str, f: Optional[TextIO] = None) -> Dict[str, Any]:
    """
    Read a forecast JSON file and validate its content.

    Args:
        filename (str): Path to the forecast JSON file.
        f (Optional[TextIO]): Open file handle. If None, filename will be opened.

    Returns:
        Dict[str, Any]: Mapping from filename to loaded JSON data with a DataFrame
            under key 'df', or None if invalid.
    """
    logger.info(f"Reading forecast file {filename}")
    retval_null = None  # {filename: None}

    if f:
        data = json.load(f)
    else:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

    if not data or not isinstance(data, dict):
        logger.warning(colored(f"Problem processing {filename}. Can't load JSON.", "yellow"))
        return retval_null

    organization = data.get("organization")
    model = data.get("model")
    model_organization = data.get("model_organization")
    question_set = data.get("question_set")
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", question_set)
    forecast_due_date = date_match.group(0) if date_match else None
    forecasts = data.get("forecasts")
    if not organization or not model or not model_organization or not question_set or not forecasts:
        logger.error(colored(f"Problem processing {filename}. Missing required fields.", "yellow"))
        return retval_null

    if not forecast_due_date:
        logger.error(
            colored(
                f"Problem processing {filename}. Issue with question set filename: {question_set}",
                "yellow",
            )
        )
        return retval_null

    df = pd.DataFrame(forecasts)
    if df.empty:
        logger.error(
            colored(f"Problem processing {filename}. Couldn't load forecasts as df.", "yellow")
        )
        return retval_null

    df = df.drop(labels="reasoning", axis=1, errors="ignore")
    data["df"] = df

    return data


def get_and_unpack_question_bank(
    sources_to_get: List[str],
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Download and unpack the question bank (either tar.gz file or mounted volume).

    Args:
        sources_to_get (List[str]): list of sources to get from the tarball.

    Returns:
        retval (Dict[str, Dict[str, pd.DataFrame]]): dfq and dfr by source.
    """
    local_question_bank_dir = data_utils.get_local_file_dir(bucket=env.QUESTION_BANK_BUCKET)

    retval = {source: {"dfq": {}, "dfr": {}} for source in sources_to_get}
    filenames = {}
    for source in sources_to_get:
        filenames[source] = data_utils.generate_filenames(source)

    # Question data
    dtype = constants.QUESTION_FILE_COLUMN_DTYPE
    for source in sources_to_get:
        dfq = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)
        source_question_file = filenames.get(source).get("jsonl_question")
        local_filename = f"{local_question_bank_dir}/{source_question_file}"
        df = pd.read_json(
            local_filename,
            lines=True,
            dtype=dtype,
            convert_dates=False,
        )
        assert not df.empty, f"Could not read {local_filename}"
        # Allows us to use a dtype that may contain column names that are not in the df
        dtype_modified = {k: v for k, v in dtype.items() if k in df.columns}
        dfq = df.astype(dtype=dtype_modified) if dtype_modified else df
        retval[source]["dfq"] = dfq

    # Resolution files
    for source in sources_to_get:
        if source == "acled":
            retval[source]["dfr"], _, _ = acled.download_dff_and_prepare_dfr(
                local_question_bank_dir=local_question_bank_dir
            )
        else:
            files = [
                os.path.join(root, filename)
                for root, _, filenames in os.walk(f"{local_question_bank_dir}/{source}")
                for filename in filenames
                if "hash_mapping.json" not in filename
            ]
            df_list = [
                pd.read_json(
                    f,
                    lines=True,
                    dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                    convert_dates=False,
                )
                for f in files
            ]
            df_list = [
                df for df in df_list if set(df.columns) == set(constants.RESOLUTION_FILE_COLUMNS)
            ]
            assert len(df_list) > 0, f"Could not find a resolution file for {source}."
            dfr = pd.concat(df_list, ignore_index=True)
            dfr["date"] = pd.to_datetime(dfr["date"])
            dfr["id"] = dfr["id"].astype(str)
            retval[source]["dfr"] = dfr

    return retval
