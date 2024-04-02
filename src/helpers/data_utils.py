"""utils for data-related tasks in llm-benchmark."""

import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_stored_question_data(
    BUCKET_NAME,
    JSON_MARKET_FILENAME,
    LOCAL_MARKET_FILENAME,
    JSON_MARKET_VALUE_FILENAME,
    LOCAL_MARKET_VALUES_FILENAME,
):
    """Download question data from cloud storage."""
    # Initialize dataframes with predefined columns
    dfq_columns = [
        "id",
        "question",
        "background",
        "source_resolution_criteria",
        "begin_datetime",
        "close_datetime",
        "url",
        "resolved",
        "resolution_datetime",
    ]
    dfmv_columns = [
        "id",
        "datetime",
        "value",
    ]
    dfq = pd.DataFrame(columns=dfq_columns)
    dfmv = pd.DataFrame(columns=dfmv_columns)

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
