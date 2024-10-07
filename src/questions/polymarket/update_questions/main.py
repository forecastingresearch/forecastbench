"""Polymarket update question script."""

import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import constants, data_utils, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "polymarket"


def create_resolution_file(question):
    """
    Create/update a resolution file based on the question provided.

    Upload all fetched price data, making the function idempotent.

    Parameters:
    - question (dict): A dictionary containing the question.

    Returns:
    - None.
    """
    df = pd.DataFrame(question["historical_prices"])
    df["id"] = question["id"]
    df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    basename = f"{question['id']}.jsonl"
    remote_filename = f"{SOURCE}/{basename}"
    local_filename = "/tmp/tmp.jsonl"
    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_filename,
        filename=remote_filename,
    )


def update_questions(dfq, dff):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dff (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    for question in dff.to_dict("records"):

        create_resolution_file(question)

        del question["fetch_datetime"]
        del question["probability"]
        del question["historical_prices"]

        # Check if the question exists in dfq
        if question["id"] in dfq["id"].values:
            # Case 1: Update existing question
            dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
            for key, value in question.items():
                dfq.at[dfq_index, key] = value
        else:
            # Case 2: Append new question
            new_q_row = pd.DataFrame([question])
            new_q_row = new_q_row.astype(constants.QUESTION_FILE_COLUMN_DTYPE)
            dfq = pd.concat([dfq, new_q_row], ignore_index=True)

    return dfq


@decorator.log_runtime
def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    dfq = update_questions(dfq, dff)

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(dfq, SOURCE)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
