"""IO layer for the resolve orchestration.

All GCS, filesystem, git, and Slack interactions live here.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandera.pandas as pa
from termcolor import colored

from _schemas import AcledResolutionFrame, QuestionFrame, ResolutionFrame
from _types import QuestionBank, SourceQuestionBank
from helpers import constants, dates, env, git, keys
from sources import ALL_SOURCE_NAMES, MARKET_SOURCE_NAMES
from sources._base import BaseSource
from utils import archiving, gcp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filename generation
# ---------------------------------------------------------------------------


def _generate_filenames(source: str) -> dict[str, str]:
    """Generate filenames for a source."""
    return {
        "jsonl_fetch": f"{source}_fetch.jsonl",
        "local_fetch": f"/tmp/{source}_fetch.jsonl",
        "jsonl_question": f"{source}_questions.jsonl",
        "local_question": f"/tmp/{source}_questions.jsonl",
        "jsonl_resolution": f"{source}_resolutions.jsonl",
        "local_resolution": f"/tmp/{source}_resolutions.jsonl",
    }


# ---------------------------------------------------------------------------
# Local directory / GCS-FUSE mount
# ---------------------------------------------------------------------------


def _get_local_file_dir(bucket: str) -> str:
    """Return the local directory containing files from `bucket`."""
    mount_dir = f"{env.BUCKET_MOUNT_POINT}/{bucket}"
    if os.path.exists(mount_dir):
        logger.info(f"Mount dir found: {mount_dir}.")
        return mount_dir

    logger.info("Mount dir not found. Downloading tarball.")
    local_dir = "/tmp"
    if env.RUNNING_LOCALLY:
        return f"{local_dir}/{bucket}"

    filename = f"{bucket}.tar.gz"
    local_filename = f"{local_dir}/{filename}"
    gcp.storage.download(
        bucket_name=bucket,
        filename=filename,
        local_filename=local_filename,
    )
    dir_to_rm_before_extract = f"{local_dir}/{bucket}"
    archiving.tar_gz.extract(
        archive_name=local_filename,
        rm_dir_before_extract=dir_to_rm_before_extract,
        extract_dir=local_dir,
    )
    return f"{local_dir}/{bucket}"


# ---------------------------------------------------------------------------
# Question bank loading
# ---------------------------------------------------------------------------


def _get_last_modified_time_of_dfq(source: str):
    """Return the last modified datetime of the dfq file for a source."""
    filenames = _generate_filenames(source)
    return gcp.storage.get_last_modified_time(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=filenames["jsonl_question"],
    )


def _read_acled_dfr(local_question_bank_dir: str) -> pd.DataFrame:
    """Read ACLED fetch file and prepare the resolution DataFrame."""
    acled_fetch_column_dtype = {
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
    filenames = _generate_filenames("acled")
    source_fetch_file = filenames.get("jsonl_fetch")
    local_filename = f"{local_question_bank_dir}/{source_fetch_file}"

    df = pd.read_json(
        local_filename,
        lines=True,
        dtype=acled_fetch_column_dtype,
        convert_dates=False,
    )

    # Fix year prefix bug in ACLED data
    def fix_year_prefix(date_str):
        if isinstance(date_str, str):
            if date_str.startswith("0025-"):
                return "2025-" + date_str[5:]
            if date_str.startswith("0024-"):
                return "2024-" + date_str[5:]
        return date_str

    df["event_date"] = df["event_date"].apply(fix_year_prefix)
    df = AcledResolutionFrame.validate(df)
    df["event_date"] = pd.to_datetime(df["event_date"])

    df = df[["country", "event_date", "event_type", "fatalities"]].copy()

    dfr = (
        pd.get_dummies(df, columns=["event_type"], prefix="", prefix_sep="")
        .groupby(["country", "event_date"])
        .sum()
        .reset_index()
    )

    return dfr


def load_question_bank(sources_to_get: list[str] | None = None) -> QuestionBank:
    """Load the question bank from GCS/local.

    Args:
        sources_to_get: List of source names. Defaults to ALL_SOURCE_NAMES.

    Returns:
        QuestionBank: {source_name: SourceQuestionBank}.
    """
    if sources_to_get is None:
        sources_to_get = ALL_SOURCE_NAMES

    logger.info("Getting resolution values...")
    today = dates.get_date_today()

    # Check market dfq files are up-to-date
    any_out_of_date_dfq = False
    for source in MARKET_SOURCE_NAMES:
        last_updated_dfq = _get_last_modified_time_of_dfq(source)
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

    question_bank = _build_question_bank(sources_to_get)
    return question_bank


def _build_question_bank(sources_to_get: list[str]) -> QuestionBank:
    """Read question and resolution DataFrames from disk."""
    local_question_bank_dir = _get_local_file_dir(bucket=env.QUESTION_BANK_BUCKET)
    question_bank: QuestionBank = {}

    # Load question DataFrames
    for source in sources_to_get:
        filenames = _generate_filenames(source)
        source_question_file = filenames.get("jsonl_question")
        local_filename = f"{local_question_bank_dir}/{source_question_file}"
        dfq = pd.read_json(local_filename, lines=True, convert_dates=False)
        assert not dfq.empty, f"Could not read {local_filename}"
        dfq = QuestionFrame.validate(dfq)
        question_bank[source] = SourceQuestionBank(dfq=dfq, dfr=pd.DataFrame())

    # Load resolution DataFrames
    for source in sources_to_get:
        if source == "acled":
            dfr = _read_acled_dfr(local_question_bank_dir)
            question_bank[source].dfr = dfr
        else:
            files = [
                os.path.join(root, filename)
                for root, _, filenames_list in os.walk(f"{local_question_bank_dir}/{source}")
                for filename in filenames_list
                if "hash_mapping.json" not in filename
            ]
            validated = []
            for f in files:
                raw = pd.read_json(f, lines=True, convert_dates=False)
                try:
                    validated.append(ResolutionFrame.validate(raw))
                except pa.errors.SchemaError as e:
                    logger.warning(
                        f"Skipped {source} resolution file as it does not match the "
                        f"ResolutionFrame schema: {os.path.basename(f)}: {e}"
                    )
                    continue
            assert len(validated) > 0, f"Could not find a resolution file for {source}."
            dfr = pd.concat(validated, ignore_index=True)
            dfr["date"] = pd.to_datetime(dfr["date"])
            question_bank[source].dfr = dfr

    logger.info("Done!")
    return question_bank


# ---------------------------------------------------------------------------
# Hash mapping IO
# ---------------------------------------------------------------------------


def load_hash_mapping(source: BaseSource, source_name: str) -> None:
    """Download and load hash mapping for a source."""
    remote_filename = f"{source_name}/hash_mapping.json"
    local_filename = f"/tmp/hash_mapping_{source_name}.json"
    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=remote_filename,
        local_filename=local_filename,
    )
    raw_json = ""
    if os.path.exists(local_filename) and os.path.getsize(local_filename) > 0:
        with open(local_filename, "r") as f:
            raw_json = f.read()
    source._load_hash_mapping(raw_json)


# ---------------------------------------------------------------------------
# Question set file download
# ---------------------------------------------------------------------------


def download_question_set_file(filename: str) -> pd.DataFrame:
    """Download and read a question set file from GCS."""
    with tempfile.NamedTemporaryFile(dir="/tmp/", delete=False) as tmp:
        local_filename = tmp.name
    gcp.storage.download(
        bucket_name=env.QUESTION_SETS_BUCKET,
        filename=filename,
        local_filename=local_filename,
    )

    with open(local_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
        questions = data.get("questions")

    os.remove(local_filename)

    if questions is None:
        raise ValueError(
            f"In `download_question_set_file()`: Could not download/load "
            f"question set {filename}"
        )

    df = pd.DataFrame(questions)
    df = BaseSource._make_columns_hashable(df)
    return df


# ---------------------------------------------------------------------------
# Forecast file IO
# ---------------------------------------------------------------------------


def get_valid_forecast_files_and_dates(
    bucket: str,
    only_keep_date: str = "",
) -> Tuple[List[str], List[str]]:
    """Return valid forecast filenames and date folders from GCS."""
    files = gcp.storage.list(bucket_name=bucket, mnt=env.BUCKET_MOUNT_POINT)
    files = [
        f
        for f in files
        if f.endswith(".json") and not f.startswith(constants.TEST_FORECAST_FILE_PREFIX)
    ]
    if only_keep_date:
        filtered = [f for f in files if f.startswith(only_keep_date)]
        return filtered, [only_keep_date]

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


def read_forecast_file(filename: str) -> Optional[Dict[str, Any]]:
    """Read a forecast JSON file and validate its content."""
    logger.info(f"Reading forecast file {filename}")

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data or not isinstance(data, dict):
        logger.warning(colored(f"Problem processing {filename}. Can't load JSON.", "yellow"))
        return None

    organization = data.get("organization")
    model = data.get("model")
    model_organization = data.get("model_organization")
    question_set = data.get("question_set")
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", question_set) if question_set else None
    forecast_due_date = date_match.group(0) if date_match else None
    forecasts = data.get("forecasts")
    if not organization or not model or not model_organization or not question_set or not forecasts:
        logger.error(colored(f"Problem processing {filename}. Missing required fields.", "yellow"))
        return None

    if not forecast_due_date:
        logger.error(
            colored(
                f"Problem processing {filename}. Issue with question set filename: {question_set}",
                "yellow",
            )
        )
        return None

    df = pd.DataFrame(forecasts)
    if df.empty:
        logger.error(
            colored(f"Problem processing {filename}. Couldn't load forecasts as df.", "yellow")
        )
        return None

    df = df.drop(labels="reasoning", axis=1, errors="ignore")
    data["df"] = df

    return data


# ---------------------------------------------------------------------------
# Upload functions
# ---------------------------------------------------------------------------


def upload_resolution_set(df: pd.DataFrame, forecast_due_date: str, question_set_filename: str):
    """Upload resolution set to GCS and push to git."""
    basename = f"{forecast_due_date}_resolution_set.json"
    local_filename = f"/tmp/{basename}"
    df = df[["id", "source", "direction", "resolution_date", "resolved_to", "resolved"]]
    df["direction"] = df["direction"].apply(lambda x: None if len(x) == 0 else x)
    df["resolution_date"] = df["resolution_date"].dt.strftime("%Y-%m-%d").astype(str)
    json_data = {
        "forecast_due_date": forecast_due_date,
        "question_set": question_set_filename,
        "resolutions": df.to_dict(orient="records"),
    }
    with open(local_filename, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
    logger.info(f"Wrote Resolution File {local_filename}.")

    upload_folder = "datasets/resolution_sets"
    gcp.storage.upload(
        bucket_name=env.PUBLIC_RELEASE_BUCKET,
        local_filename=local_filename,
        destination_folder=upload_folder,
    )
    logger.info(f"Uploaded Resolution File {local_filename} to {upload_folder}.")

    mirrors = keys.get_secret_that_may_not_exist("HUGGING_FACE_REPO_URL")
    mirrors = [mirrors] if mirrors else []
    git.clone_and_push_files(
        repo_url=keys.API_GITHUB_DATASET_REPO_URL,
        files={local_filename: f"{upload_folder}/{basename}"},
        commit_message=f"resolution set: automatic update for {question_set_filename}.",
        mirrors=mirrors,
    )


def upload_processed_forecast_file(data: dict, forecast_due_date: str, filename: str):
    """Upload processed forecast file to GCS."""
    local_filename = "/tmp/tmp.json"
    with open(local_filename, "w") as f:
        f.write(json.dumps(data, indent=4))

    gcp.storage.upload(
        bucket_name=env.PROCESSED_FORECAST_SETS_BUCKET,
        local_filename=local_filename,
        filename=filename,
    )
