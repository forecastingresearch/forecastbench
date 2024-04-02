"""utils for data-related tasks in llm-benchmark."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
}
QUESTION_FILE_COLUMNS = list(QUESTION_FILE_COLUMN_DTYPE.keys())

RESOLUTION_FILE_COLUMN_DTYPE = {
    "id": str,
    "datetime": str,
}

# value is not included in dytpe because it's of type ANY
RESOLUTION_FILE_COLUMNS = list(RESOLUTION_FILE_COLUMN_DTYPE.keys()) + ["value"]


def print_error_info_handler(details):
    """Print warning on backoff."""
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def get_data_from_cloud_storage(
    BUCKET_NAME,
    JSONL_QUESTION_FILENAME,
    LOCAL_QUESTION_FILENAME,
    JSONL_RESOLUTION_FILENAME,
    LOCAL_RESOLUTION_FILENAME,
    JSONL_FETCH_FILENAME,
    LOCAL_FETCH_FILENAME,
):
    """Download question data from cloud storage."""
    dfq = pd.DataFrame(columns=QUESTION_FILE_COLUMNS)
    dfr = pd.DataFrame(columns=RESOLUTION_FILE_COLUMNS)
    dff = pd.DataFrame(
        columns=QUESTION_FILE_COLUMNS
        + [
            "fetch_datetime",
            "probability",
        ]
    )

    def _download_and_read(filename, local_filename, df_tmp, dtype):
        logger.info(f"Get from {BUCKET_NAME}/{filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=BUCKET_NAME,
            filename=filename,
            local_filename=local_filename,
        )
        df = pd.read_json(local_filename, lines=True, dtype=dtype, convert_dates=False)
        return df if not df.empty else df_tmp

    try:
        dfq = _download_and_read(
            JSONL_QUESTION_FILENAME,
            LOCAL_QUESTION_FILENAME,
            dfq,
            QUESTION_FILE_COLUMN_DTYPE,
        )
        dfr = _download_and_read(
            JSONL_RESOLUTION_FILENAME,
            LOCAL_RESOLUTION_FILENAME,
            dfr,
            dtype=RESOLUTION_FILE_COLUMN_DTYPE,
        )
        dff = _download_and_read(
            JSONL_FETCH_FILENAME,
            LOCAL_FETCH_FILENAME,
            dff,
            {**QUESTION_FILE_COLUMN_DTYPE, "fetch_datetime": str},
        )
    except Exception:
        pass

    return dfq, dfr, dff


def upload_questions_and_resolution(
    dfq,
    dfr,
    BUCKET_NAME,
    LOCAL_QUESTION_FILENAME,
    LOCAL_RESOLUTION_FILENAME,
):
    """Write files to disk and upload to storage."""
    dfq = dfq.sort_values(by=["id"], ignore_index=True)
    dfr = dfr.sort_values(by=["id", "datetime"], ignore_index=True)

    with open(LOCAL_QUESTION_FILENAME, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfr.to_json(LOCAL_RESOLUTION_FILENAME, orient="records", lines=True, date_format="iso")

    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=LOCAL_QUESTION_FILENAME,
    )
    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=LOCAL_RESOLUTION_FILENAME,
    )


def get_stored_question_data(
    BUCKET_NAME,
    JSON_MARKET_FILENAME,
    LOCAL_MARKET_FILENAME,
    JSON_MARKET_VALUE_FILENAME,
    LOCAL_MARKET_VALUES_FILENAME,
):
    """Download question data from cloud storage."""
    # Initialize dataframes with predefined columns
    dfq = pd.DataFrame(columns=QUESTION_FILE_COLUMNS)
    dfmv = pd.DataFrame(columns=RESOLUTION_FILE_COLUMNS)

    try:
        # Attempt to download and read the market questions file
        logger.info(f"Get questions from {BUCKET_NAME}/{JSON_MARKET_FILENAME}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=BUCKET_NAME,
            filename=JSON_MARKET_FILENAME,
            local_filename=LOCAL_MARKET_FILENAME,
        )
        # Check if the file is not empty before reading
        if os.path.getsize(LOCAL_MARKET_FILENAME) > 0:
            dfq_tmp = pd.read_json(LOCAL_MARKET_FILENAME, lines=True)
            if not dfq_tmp.empty:
                dfq = dfq_tmp

        # Attempt to download and read the market values file
        logger.info(f"Get market values from {BUCKET_NAME}/{JSON_MARKET_VALUE_FILENAME}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=BUCKET_NAME,
            filename=JSON_MARKET_VALUE_FILENAME,
            local_filename=LOCAL_MARKET_VALUES_FILENAME,
        )
        # Check if the file is not empty before reading
        if os.path.getsize(LOCAL_MARKET_VALUES_FILENAME) > 0:
            dfmv_tmp = pd.read_json(LOCAL_MARKET_VALUES_FILENAME, lines=True)
            if not dfmv_tmp.empty:
                dfmv = dfmv_tmp
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    return dfq, dfmv
