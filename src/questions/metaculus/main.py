"""Generate questions from Metaculus API."""

import json
import os
import sys
from datetime import datetime, timezone

import backoff
import numpy as np
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

json_market_filename = "metaculus.json"
local_market_filename = f"/tmp/{json_market_filename}"
json_market_values_filename = "metaculus_values.json"
local_market_values_filename = f"/tmp/{json_market_values_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
metaculus_categories = [
    "geopolitics",
    "natural-sciences",
    "sports-entertainment",
    "health-pandemics",
    "law",
    "computing-and-math",
]


def _get_stored_question_data():
    """Download Metaculus question data from cloud storage."""
    dfq = pd.DataFrame(
        columns=[
            "id",
            "question",
            "resolution_criteria",
            "resolved",
        ]
    )
    dfmv = pd.DataFrame(
        columns=[
            "id",
            "datetime",
            "value",
        ]
    )
    try:
        print(f"Get questions from {bucket_name}/{json_market_filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_market_filename,
            local_filename=local_market_filename,
        )
        dfq_tmp = pd.read_json(local_market_filename, lines=True)
        if not dfq_tmp.empty:
            dfq = dfq_tmp

        print(f"Get market values from {bucket_name}/{json_market_values_filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_market_values_filename,
            local_filename=local_market_values_filename,
        )
        dfmv_tmp = pd.read_json(local_market_values_filename, lines=True)
        if not dfmv_tmp.empty:
            dfmv = dfmv_tmp
    except Exception:
        pass
    return dfq, dfmv


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
def _get_data(topics):
    """Get the top 100 markets from Metaculus."""
    print("Calling Metaculus search-markets endpoint")
    endpoint = "https://www.metaculus.com/api2/questions/"
    params = {
        "order_by": "-activity",
        "forecast_type": "binary",
        "status": "active",
        "has_group": "false",
        "limit": 100,
        "main-feed": True,
    }
    response = requests.get(endpoint, params=params)
    utc_datetime_obj = datetime.now(timezone.utc)
    if not response.ok:
        print(f"ERROR: Request to Metaculus API endpoint {endpoint} failed.")
        response.raise_for_status()
    tmp = [(utc_datetime_obj, response.json()["results"])]

    for topic in topics:
        response = requests.get(endpoint, params={**params, "search": f"include:{topic}"})
        utc_datetime_obj = datetime.now(timezone.utc)
        if not response.ok:
            print(f"ERROR: Request to Metaculus API endpoint {endpoint} failed.")
            response.raise_for_status()
        tmp.append((utc_datetime_obj, response.json()["results"]))

    # Remove duplicate markets
    seen_market_ids = set()
    unique_markets = []
    for datetime_obj, markets in tmp:
        unique_markets_batch = []
        for market in markets:
            if market["id"] not in seen_market_ids:
                unique_markets_batch.append(market)
                seen_market_ids.add(market["id"])
        unique_markets.append((datetime_obj, unique_markets_batch))

    return unique_markets


def _get_potentially_resolved_market_value(market, market_value):
    """Get the market value based on the resolution.

    A market that has resolved should return the resolved value. The possible values for
    market["resolution"] and the associated return values are:
    * 0.0 (i.e. No) -> 0
    * 1.0 (i.e. Yes) -> 1
    * -1.0 (i.e. Ambiguous) -> NaN
    * -2.0 (i.e. Annulled) -> NaN

    A market that hasn't resolved returns the current market probability. This includes closed markets.
    """
    if market["active_state"] != "RESOLVED":
        return market_value

    return int(market["resolution"]) if market["resolution"] > 0 else np.nan


def _update_questions(dfq, dfmv, datetime_and_markets):
    """Update the dataframes given the latest Metaculus info."""

    def _get_market_value_entry(market_id, utc_datetime_obj, value):
        return {
            "id": market_id,
            "datetime": utc_datetime_obj,
            "value": value,
        }

    def _entry_exists_for_today(market_values, utc_date_str):
        return market_values["datetime"].dt.strftime("%Y-%m-%d").eq(utc_date_str).any()

    new_markets = []
    new_market_values = []
    for utc_datetime_obj, markets in datetime_and_markets:
        utc_date_str = utc_datetime_obj.strftime("%Y-%m-%d")
        for market in markets:
            market_for_id = dfq[dfq["id"] == market["id"]]
            market_value = market["community_prediction"]["full"]
            market_value = market_value.get("q2") if isinstance(market_value, dict) else np.nan
            if market_for_id.empty:
                print(f"Adding new market `{market['id']}`")
                new_markets.append(
                    {
                        "id": market["id"],
                        "question": market["title"],
                        "resolution_criteria": (
                            "Resolves to the resolved value according to Metaculus. "
                            "If the question is unresolved, resolves to the market value on "
                            f"https://www.metaculus.com{market['page_url']} at 12AM UTC."
                        ),
                        "resolved": False,
                    }
                )
                new_market_values.append(
                    _get_market_value_entry(market["id"], utc_datetime_obj, market_value)
                )
            else:
                index = market_for_id.index[0]
                if not market_for_id.at[index, "resolved"]:
                    print(f"Updating market `{market['id']}`")
                    dfq.at[index, "resolved"] = market["active_state"] == "RESOLVED"
                    if not _entry_exists_for_today(dfmv[dfmv["id"] == market["id"]], utc_date_str):
                        dfmv.loc[len(dfmv)] = _get_market_value_entry(
                            market["id"],
                            utc_datetime_obj,
                            _get_potentially_resolved_market_value(market, market_value),
                        )

    if new_markets:
        dfq = (
            pd.DataFrame(new_markets)
            if dfq.empty
            else pd.concat([dfq, pd.DataFrame(new_markets)], ignore_index=True)
        )
        dfmv = (
            pd.DataFrame(new_market_values)
            if dfmv.empty
            else pd.concat([dfmv, pd.DataFrame(new_market_values)], ignore_index=True)
        )

    dfmv["id"] = dfmv["id"].astype(str)
    dfmv["datetime"] = pd.to_datetime(dfmv["datetime"], utc=True, errors="coerce")
    dfmv = dfmv.sort_values(by=["id", "datetime"])
    return dfq, dfmv


def driver(event, context):
    """Generate questions from Metaculus API and update question file in GCP Cloud Storage."""
    # Download existing questions from cloud storage
    dfq, dfmv = _get_stored_question_data()

    # Get the latest Metaculus data
    response = _get_data(metaculus_categories)

    # Update the existing questions
    dfq, dfmv = _update_questions(dfq, dfmv, response)

    # Save and upload
    with open(local_market_filename, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfmv.to_json(local_market_values_filename, orient="records", lines=True, date_format="iso")

    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_filename,
    )
    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_values_filename,
    )
    print("Done.")


if __name__ == "__main__":
    driver(None, None)
