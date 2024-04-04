"""Generate questions from Wikidata."""

import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "wikidata"
jsonl_question_filename = f"{source}_questions.jsonl"
local_question_filename = f"/tmp/{jsonl_question_filename}"
jsonl_resolution_filename = f"{source}_resolutions.jsonl"
local_resolution_filename = f"/tmp/{jsonl_resolution_filename}"
jsonl_fetch_filename = f"{source}_fetch.jsonl"
local_fetch_filename = f"/tmp/{jsonl_fetch_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


def _update_questions_and_resolved_values(dfq, dfr, dff):
    """Update the dataframes that hold the questions and the resolution values.

    dfq: Wikidata questions in the question bank
    dfr: Wikidata resolution values
    dff: Today's fetched markets
    """

    def _get_resolution_entry(market_id, utc_datetime_str, value):
        return {
            "id": market_id,
            "datetime": utc_datetime_str,
            "value": value,
        }

    def _entry_exists_for_today(resolution_values, utc_date_str):
        return resolution_values["datetime"].str.startswith(utc_date_str).any()

    # For Wikidata, we're getting all countries every time. If a country exists in dfq but no
    # longer exists in dff, this means the country no longer exists. It may have been renamed or
    # the borders may have changed. Mark countries that satisfy this criteria as `resolved`
    resolved_countries = ~dfq["id"].isin(dff["id"])
    dfq.loc[resolved_countries, "resolved"] = True
    dfq.loc[resolved_countries, "resolution_datetime"] = dff["fetch_datetime"].unique()[0]

    # Find rows in dff not in dfq: These are the new countrties to add to dfq
    rows_to_append = dff[~dff["id"].isin(dfq["id"])]
    rows_to_append = rows_to_append.drop(columns=["fetch_datetime", "probability"])
    dfq = pd.concat([dfq, rows_to_append], ignore_index=True)

    # Update dfr for everything in dff as these markets are all open/non-resolved and we already
    # have the market values.
    for _, row in dff.iterrows():
        utc_date_str = row["fetch_datetime"][:10]
        if dfr.empty or not _entry_exists_for_today(dfr[dfr["id"] == row["id"]], utc_date_str):
            dfr.loc[len(dfr)] = _get_resolution_entry(
                row["id"],
                row["fetch_datetime"],
                row["probability"],
            )

    return dfq, dfr


def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    dfq, dfr, dff = data_utils.get_data_from_cloud_storage(
        bucket_name,
        jsonl_question_filename,
        local_question_filename,
        jsonl_resolution_filename,
        local_resolution_filename,
        jsonl_fetch_filename,
        local_fetch_filename,
    )

    # Update the existing questions and resolution values
    dfq, dfr = _update_questions_and_resolved_values(dfq, dfr, dff)

    # Save and upload
    data_utils.upload_questions_and_resolution(
        dfq,
        dfr,
        bucket_name,
        local_question_filename,
        local_resolution_filename,
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
