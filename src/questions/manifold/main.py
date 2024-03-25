"""Generate questions from Manifold API."""

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
import gcp.storage.run as storage  # noqa: E402

json_market_filename = "manifold.json"
local_market_filename = f"/tmp/{json_market_filename}"
json_market_values_filename = "manifold_values.json"
local_market_values_filename = f"/tmp/{json_market_values_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
manifold_topic_slugs = ["entertainment", "sports-default", "technology-default"]


def _get_stored_question_data():
    """Download Manifold question data from cloud storage."""
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
        storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_market_filename,
            local_filename=local_market_filename,
        )
        dfq_tmp = pd.read_json(local_market_filename, lines=True)
        if not dfq_tmp.empty:
            dfq = dfq_tmp

        print(f"Get market values from {bucket_name}/{json_market_values_filename}")
        storage.download_no_error_message_on_404(
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


def _get_manifold_data(topics):
    """Get the top 100 markets from Manifold Markets."""
    print("Calling Manifold search-markets endpoint")
    endpoint = "https://api.manifold.markets/v0/search-markets"
    params = {
        "sort": "most-popular",
        "contractType": "BINARY",
        "filter": "open",
        "limit": 100,
    }
    response = requests.get(endpoint, params=params)
    utc_datetime_obj = datetime.now(timezone.utc)
    if not response.ok:
        print(f"ERROR: Request to Manifold Markets API endpoint {endpoint} failed.")
        raise
    tmp = [(utc_datetime_obj, response.json())]

    for topic in topics:
        response = requests.get(endpoint, params={**params, "topicSlug": topic})
        utc_datetime_obj = datetime.now(timezone.utc)
        if not response.ok:
            print(f"ERROR: Request to Manifold Markets API endpoint {endpoint} failed.")
            raise
        tmp.append((utc_datetime_obj, response.json()))

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


def _update_questions(dfq, dfmv, datetime_and_markets):
    """Update the dataframes given the latest Manifold market info."""

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
            if market_for_id.empty:
                print(f"Adding new market `{market['id']}`")
                new_markets.append(
                    {
                        "id": market["id"],
                        "question": market["question"],
                        "resolution_criteria": (
                            f"Resolves to the market value on {market['url']} at 12AM UTC."
                        ),
                        "resolved": False,
                    }
                )
                new_market_values.append(
                    _get_market_value_entry(market["id"], utc_datetime_obj, market["probability"])
                )
            else:
                index = market_for_id.index[0]
                if not market_for_id.at[index, "resolved"]:
                    print(f"Updating market `{market['id']}`")
                    dfq.at[index, "resolved"] = market["isResolved"]
                    if not _entry_exists_for_today(dfmv[dfmv["id"] == market["id"]], utc_date_str):
                        dfmv.loc[len(dfmv)] = _get_market_value_entry(
                            market["id"], utc_datetime_obj, market["probability"]
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
    """Generate questions from Manifold API and update question file in GCP Cloud Storage."""
    # Download existing questions from cloud storage
    dfq, dfmv = _get_stored_question_data()

    # Get the latest Manifold data
    response = _get_manifold_data(manifold_topic_slugs)

    # Update the existing questions
    dfq, dfmv = _update_questions(dfq, dfmv, response)

    # Save and upload
    with open(local_market_filename, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfmv.to_json(local_market_values_filename, orient="records", lines=True, date_format="iso")

    storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_filename,
    )
    storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_values_filename,
    )
    print("Done.")


if __name__ == "__main__":
    driver(None, None)
