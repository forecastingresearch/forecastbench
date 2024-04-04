"""Yfinance update question script."""

import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import data_utils  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"
BUCKET_NAME = "fri-benchmark-question-bank"  # os.environ.get("CLOUD_STORAGE_BUCKET")


def update_questions(dfq, dfr, dff):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dfr (pd.DataFrame): DataFrame containing all historical community predictions.
    - dff  (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    dff_list = dff.to_dict("records")
    for question in dff_list:
        # Prepare a new row as a dictionary for dfr
        new_mv_row = pd.DataFrame(
            [
                {
                    "id": question["id"],
                    "datetime": question["fetch_datetime"],
                    "value": question["probability"],
                }
            ]
        )
        # Append new row to dfr using pd.concat
        dfr = pd.concat([dfr, new_mv_row], ignore_index=True)

        del question["fetch_datetime"]
        del question["probability"]

        # Check if the question exists in dfq
        if question["id"] in dfq["id"].values:
            # Case 1: Update existing question
            dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
            for key, value in question.items():
                dfq.at[dfq_index, key] = value
        else:
            # Case 2: Append new question
            new_q_row = pd.DataFrame([question])
            dfq = pd.concat([dfq, new_q_row], ignore_index=True)

    return dfq, dfr


def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq, dfr, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_resolution_data=True, return_fetch_data=True
    )

    # Update the existing questions
    dfq, dfr = update_questions(dfq, dfr, dff)

    logger.info("Uploading to GCP...")
    # Save and upload
    data_utils.upload_questions_and_resolution(dfq, dfr, SOURCE)
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
