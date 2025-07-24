"""utils for data-related tasks in llm-benchmark."""

import json
import logging
import os
import sys

import pandas as pd

from . import constants, env

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))  # noqa: E402
from utils import archiving, gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def print_error_info_handler(details):
    """Print warning on backoff."""
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def generate_filenames(source):
    """
    Generate and return filenames based on the given source.

    Parameters:
    - source (str): The source name used to construct filenames.

    Returns:
    - A dictionary containing the keys 'jsonl_fetch', 'local_fetch', 'jsonl_question',
      'local_question', 'jsonl_resolution', and 'local_resolution' with their respective filenames.
    """
    filenames = {
        "jsonl_fetch": f"{source}_fetch.jsonl",
        "local_fetch": f"/tmp/{source}_fetch.jsonl",
        "jsonl_question": f"{source}_questions.jsonl",
        "local_question": f"/tmp/{source}_questions.jsonl",
        "jsonl_resolution": f"{source}_resolutions.jsonl",
        "local_resolution": f"/tmp/{source}_resolutions.jsonl",
    }
    return filenames


def download_and_read(filename, local_filename, df_tmp, dtype):
    """Download data from cloud storage."""
    logger.info(f"Get from {env.QUESTION_BANK_BUCKET}/{filename}")
    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=filename,
        local_filename=local_filename,
    )
    df = pd.read_json(local_filename, lines=True, dtype=dtype, convert_dates=False)
    if df.empty:
        return df_tmp
    # Allows us to pass a dtype that may contain column names that are not in the df
    dtype_modified = {k: v for k, v in dtype.items() if k in df.columns}
    return df.astype(dtype=dtype_modified) if dtype_modified else df


def get_last_modified_time_of_dfq_from_cloud_storage(source):
    """Return the last modified date of the dfq file for `source`.

    To be removed once we update dfq to contain the last modified date.
    """
    filenames = generate_filenames(source)
    return gcp.storage.get_last_modified_time(
        bucket_name=env.QUESTION_BANK_BUCKET,
        filename=filenames["jsonl_question"],
    )


def get_data_from_cloud_storage(
    source, return_question_data=False, return_resolution_data=False, return_fetch_data=False
):
    """
    Download data from cloud storage based on source and selectively return data frames.

    Parameters:
    - bucket_name (str): The name of the cloud storage bucket.
    - source (str): The source name used to construct and identify filenames.
    - return_question_data (bool): Whether to return the question data frame.
    - return_resolution_data (bool): Whether to return the resolution data frame.
    - return_fetch_data (bool): Whether to return the fetch data frame.

    Returns:
    - A tuple of pandas DataFrame objects as per the boolean flags.
    """
    filenames = generate_filenames(source)

    results = []
    if return_question_data:
        dfq = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)
        dfq = download_and_read(
            filenames["jsonl_question"],
            filenames["local_question"],
            dfq,
            constants.QUESTION_FILE_COLUMN_DTYPE,
        )
        results.append(dfq)

    if return_resolution_data:
        dfr = pd.DataFrame(columns=constants.RESOLUTION_FILE_COLUMNS)
        dfr = download_and_read(
            filenames["jsonl_resolution"],
            filenames["local_resolution"],
            dfr,
            constants.RESOLUTION_FILE_COLUMN_DTYPE,
        )
        results.append(dfr)

    if return_fetch_data:
        dff = pd.DataFrame(
            columns=constants.QUESTION_FILE_COLUMNS + ["fetch_datetime", "probability"]
        )
        dff = download_and_read(
            filenames["jsonl_fetch"],
            filenames["local_fetch"],
            dff,
            {"id": str},
        )
        results.append(dff)

    if len(results) == 1:
        return results[0]

    return tuple(results)


def get_local_file_dir(bucket: str) -> str:
    """
    Return the local file directory that contains the files in `bucket`.

    This function will either
    * download a tarball that contains all files in the bucket, or
    * return the mount directory of the bucket if it has been mounted with GCS-FUSE.

    Args:
        bucket(str): the name of the bucket.

    Returns:
        (str): the directory that contais the files.
    """
    mount_dir = f"{env.BUCKET_MOUNT_POINT}/{bucket}"
    if os.path.exists(mount_dir):
        logger.info(f"Mount dir found: {mount_dir}.")
        return mount_dir

    logger.info("Mount dir not found. Downloading tarball.")
    local_dir = "/tmp"
    filename = f"{bucket}.tar.gz"
    local_filename = f"/{local_dir}/{filename}"
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
    return f"/{local_dir}/{bucket}"


def upload_questions(dfq, source):
    """
    Write question data frame to disk and upload to cloud storage.

    This function handles file naming through the `generate_filenames` utility and ensures
    that data is sorted before upload. It leverages GCP storage utilities for the upload process.

    Parameters:
    - dfq (pandas.DataFrame): DataFrame containing question data.
    - source (str): The source name.
    """
    filenames = generate_filenames(source)
    local_question_filename = filenames["local_question"]

    dfq = dfq.sort_values(by=["id"], ignore_index=True)

    with open(local_question_filename, "w", encoding="utf-8") as f:
        for record in dfq.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_question_filename,
    )


def upload_resolutions(dfr, source):
    """
    Write resolution data frame to disk and upload to cloud storage.

    This function handles file naming through the `generate_filenames` utility and ensures
    that data is sorted before upload. It leverages GCP storage utilities for the upload process.

    Parameters:
    - dfr (pandas.DataFrame): DataFrame containing resolutiondata.
    - source (str): The source name.
    """
    filenames = generate_filenames(source)
    local_resolution_filename = filenames["local_resolution"]

    dfr = dfr.sort_values(by=["id", "date"], ignore_index=True)

    dfr.to_json(local_resolution_filename, orient="records", lines=True, date_format="iso")

    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_resolution_filename,
    )


def upload_questions_and_resolution(dfq, dfr, source):
    """
    Upload both questions and resolutions.

    Wrapper for `upload_questions` and `upload_resolutions`.

    Parameters:
    - dfq (pandas.DataFrame): DataFrame containing question data.
    - dfr (pandas.DataFrame): DataFrame containing resolutiondata.
    - source (str): The source name.
    """
    upload_questions(dfq, source)
    upload_resolutions(dfq, source)


def read_jsonl(file_path):
    """
    Read a JSONL file and return its content as a list of dictionaries.

    Args:
        file_path (str): The path to the JSONL file.

    Returns:
        list: A list of dictionaries, each representing a JSON object from the file.
    """
    data = []
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                json_object = json.loads(line)
                data.append(json_object)
    return data


def list_files(directory):
    """List all filenames under a directory."""
    filenames = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            filenames.append(filename)
    return filenames
