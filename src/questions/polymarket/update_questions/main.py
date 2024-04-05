"""Polymarket update question script."""

import logging
import os
import sys
from datetime import timedelta

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import constants, data_utils, dates, decorator  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "polymarket"


def create_resolution_file(question, source=SOURCE):
    """
    Create or update a resolution file based on the question ID provided. Download the existing file, if any.

    Check the last entry date, and update with new data if there's no entry for today. Upload the updated file
    back to the specified Google Cloud Platform bucket.

    Args:
    - question (dict): A dictionary containing at least the 'id' of the question.
    - source (str): The source directory path within the bucket where files are stored.

    Returns:
    - DataFrame: Return the current state of the resolution file as a DataFrame if no update is needed.
      If an update occurs, the function returns None after uploading the updated file.
    """
    basename = f"{question['id']}.jsonl"
    remote_filename = f"{source}/{basename}"
    local_filename = "/tmp/tmp.jsonl"

    gcp.storage.download_no_error_message_on_404(
        bucket_name=constants.BUCKET_NAME,
        filename=remote_filename,
        local_filename=local_filename,
    )
    current_df = pd.read_json(
        local_filename,
        lines=True,
        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
        convert_dates=False,
    )

    yesterday = dates.get_date_today() - timedelta(days=1)

    if not current_df.empty and pd.to_datetime(current_df["date"].iloc[-1]).date() >= yesterday:
        logger.info(f"{question['id']} is skipped because it's already up-to-date!")
        # Check last date to see if we've already gotten the resolution value for today
        # If we have it alreeady, return to avoid unnecessary API calls
        return False

    fetch_df = pd.DataFrame(question["historical_prices"])

    if not current_df.empty:
        # combine the newly fetched prices with the previously fetched prices.
        final_df = pd.concat([current_df, fetch_df]).drop_duplicates().reset_index(drop=True)
    else:
        final_df = fetch_df.copy()

    final_df = final_df[["id", "date", "value"]].astype(
        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE
    )
    final_df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME,
        local_filename=local_filename,
        filename=remote_filename,
    )

    return True


def update_questions(dfq, dff):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dff  (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    dff_list = dff.to_dict("records")
    for question in dff_list:

        result_TF = create_resolution_file(question)

        # only update the question info if we modify(update/create) the resolution file
        # If the resolution come out today, the resolution file won't be updated
        # and we should not change the question status to resolved, otherwise once we
        # change its status to resolved, it won't append the actual resolution to the resolution file
        if result_TF:

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
    # Download existing questions from cloud storage
    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    # Update the existing questions
    dfq = update_questions(dfq, dff)

    logger.info("Uploading to GCP...")
    # Save and upload
    data_utils.upload_questions(dfq, SOURCE)
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
