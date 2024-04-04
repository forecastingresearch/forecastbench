"""Generate questions from Manifold API."""

import logging
import os
import sys

import backoff
import certifi
import numpy as np
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, dates  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "manifold"
jsonl_question_filename = f"{source}_questions.jsonl"
local_question_filename = f"/tmp/{jsonl_question_filename}"
jsonl_resolution_filename = f"{source}_resolutions.jsonl"
local_resolution_filename = f"/tmp/{jsonl_resolution_filename}"
jsonl_fetch_filename = f"{source}_fetch.jsonl"
local_fetch_filename = f"/tmp/{jsonl_fetch_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _get_market(market_id):
    """Get the market description and close time for the specified market."""
    logger.info(f"Calling market endpoint for {market_id}")
    endpoint = f"https://api.manifold.markets/v0/market/{market_id}"
    response = requests.get(endpoint, verify=certifi.where())
    utc_datetime_str = dates.get_datetime_now()
    if not response.ok:
        logger.error(f"Request to market endpoint failed for {market_id}.")
        response.raise_for_status()
    return utc_datetime_str, response.json()


def _update_questions_and_resolved_values(dfq, dfr, dff):
    """Update the dataframes that hold the questions and the resolution values.

    dfq: Manifold questions in the question bank
    dfr: Manifold resolution values
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

    def _extract_description_helper(d):
        """Extract 'text' values from a nested dictionary/list structure."""
        if isinstance(d, dict):
            return " ".join(
                _extract_description_helper(v)
                for k, v in d.items()
                if k == "text" or k == "label" or isinstance(v, (dict, list))
            )
        elif isinstance(d, list):
            return " ".join(_extract_description_helper(item) for item in d)
        else:
            return d

    def _extract_description(description):
        description = _extract_description_helper(description)
        return description if description else "N/A"

    def _get_potentially_resolved_market_value(market):
        """Get the market value based on the resolution.

        A market that has resolved should return the resolved value. The possible values for
        market["resolution"] and the associated return values are:
        * YES -> 1
        * NO -> 0
        * MKT -> market probability
        * CANCEL (i.e. N/A) -> NaN

        A market that hasn't resolved returns the current market probability. This includes closed markets.
        """
        if not market["isResolved"]:
            return market["probability"]

        return {"YES": 1, "NO": 0, "CANCEL": np.nan}.get(
            market["resolution"], market["probability"]
        )

    # Find rows in dff not in dfq: These are the new markets to add to dfq
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

    # Update all questions in dfq. Update resolved, resolution_datetime, and background. For those
    # questions that were not updated in the loop above, update dfr.
    for index, row in dfq.iterrows():
        utc_datetime_str, market = _get_market(row["id"])
        utc_date_str = utc_datetime_str[:10]
        if market["isResolved"]:
            dfq.at[index, "resolved"] = True
            dfq.at[index, "resolution_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
                market["resolutionTime"]
            )
        dfq.at[index, "background"] = _extract_description(market["description"])
        if dfr.empty or not _entry_exists_for_today(dfr[dfr["id"] == market["id"]], utc_date_str):
            dfr.loc[len(dfr)] = _get_resolution_entry(
                row["id"],
                utc_datetime_str,
                _get_potentially_resolved_market_value(market),
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
