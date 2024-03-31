"""Generate questions from Manifold API."""

import json
import os
import sys
from datetime import datetime, timezone

import backoff
import certifi
import numpy as np
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

source = "manifold"
jsonl_question_filename = f"{source}_questions.jsonl"
local_question_filename = f"/tmp/{jsonl_question_filename}"
jsonl_resolution_filename = f"{source}_resolutions.jsonl"
local_resolution_filename = f"/tmp/{jsonl_resolution_filename}"
jsonl_fetch_filename = f"{source}_fetch.jsonl"
local_fetch_filename = f"/tmp/{jsonl_fetch_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


def _get_data_from_cloud_storage():
    """Download Manifold question data from cloud storage."""
    dfq = pd.DataFrame(
        columns=[
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
    )
    dfr = pd.DataFrame(
        columns=[
            "id",
            "datetime",
            "value",
        ]
    )
    dff = pd.DataFrame(
        columns=[
            "fetch_datetime",
            "id",
            "question",
            "background",
            "source_resolution_criteria",
            "begin_datetime",
            "close_datetime",
            "url",
            "resolved",
            "resolution_datetime",
            "probability",
        ]
    )

    def _download_and_read(filename, local_filename, df_tmp):
        print(f"Get from {bucket_name}/{filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=filename,
            local_filename=local_filename,
        )
        df = pd.read_json(local_filename, lines=True)
        return df if not df.empty else df_tmp

    try:
        dfq = _download_and_read(jsonl_question_filename, local_question_filename, dfq)
        dfr = _download_and_read(jsonl_resolution_filename, local_resolution_filename, dfr)
        dff = _download_and_read(jsonl_fetch_filename, local_fetch_filename, dff)
    except Exception:
        pass

    dfr["datetime"] = pd.to_datetime(dfr["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    dff["fetch_datetime"] = pd.to_datetime(dff["fetch_datetime"])

    return dfq, dfr, dff


def _print_error_info_handler(details):
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=_print_error_info_handler,
)
def _get_market(market_id):
    """Get the market description and close time for the specified market."""
    print(f"Calling market endpoint for {market_id}")
    endpoint = f"https://api.manifold.markets/v0/market/{market_id}"
    response = requests.get(endpoint, verify=certifi.where())
    if not response.ok:
        print(f"ERROR: Request to market endpoint failed for {market_id}.")
        response.raise_for_status()
    return datetime.now(timezone.utc), response.json()


def _write_and_upload(dfq, dfr):
    dfq = dfq.sort_values(by=["id"], ignore_index=True)
    dfr = dfr.sort_values(by=["id", "datetime"], ignore_index=True)

    with open(local_question_filename, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfr.to_json(local_resolution_filename, orient="records", lines=True, date_format="iso")

    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_question_filename,
    )
    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_resolution_filename,
    )


def _update_questions_and_resolved_values(dfq, dfr, dff):
    """Update the dataframes that hold the questions and the resolution values.

    dfq: Manifold questions in the question bank
    dfr: Manifold resolution values
    dff: Today's fetched markets
    """

    def _get_resolution_entry(market_id, utc_datetime_obj, value):
        return {
            "id": market_id,
            "datetime": utc_datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f%z"),
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
        utc_date_str = row["fetch_datetime"].strftime("%Y-%m-%d")
        if dfr.empty or not _entry_exists_for_today(dfr[dfr["id"] == row["id"]], utc_date_str):
            dfr.loc[len(dfr)] = _get_resolution_entry(
                row["id"],
                row["fetch_datetime"],
                row["probability"],
            )

    # Update all questions in dfq. Update resolved, resolution_datetime, and background. For those
    # questions that were not updated in the loop above, update dfr.
    for index, row in dfq.iterrows():
        utc_datetime_obj, market = _get_market(row["id"])
        if market["isResolved"]:
            dfq.at[index, "resolved"] = True
            dfq.at[index, "resolution_datetime"] = pd.to_datetime(
                market["resolutionTime"], unit="ms", utc=True
            ).strftime("%Y-%m-%d %H:%M:%S.%f%z")
        dfq.at[index, "background"] = _extract_description(market["description"])
        if dfr.empty or not _entry_exists_for_today(dfr[dfr["id"] == market["id"]], utc_date_str):
            dfr.loc[len(dfr)] = _get_resolution_entry(
                row["id"],
                utc_datetime_obj,
                _get_potentially_resolved_market_value(market),
            )

    return dfq, dfr


def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    dfq, dfr, dff = _get_data_from_cloud_storage()

    # Update the existing questions and resolution values
    dfq, dfr = _update_questions_and_resolved_values(dfq, dfr, dff)

    # Save and upload
    _write_and_upload(dfq, dfr)

    print("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
