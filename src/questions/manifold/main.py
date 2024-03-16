"""Generate questions from Manifold API."""

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
import gcp.storage.run as storage  # noqa: E402

json_filename = "manifold.json"
local_filename = f"/tmp/{json_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


def _get_stored_question_data():
    """Download Manifold question data from cloud storage."""
    print(f"Get questions from {bucket_name}/{json_filename}")
    df = pd.DataFrame(
        columns=[
            "id",
            "question",
            "resolution_criteria",
            "resolved",
            "market_values",
        ]
    )
    try:
        storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_filename,
            local_filename=local_filename,
        )
        df = pd.read_json(local_filename)
    except Exception:
        pass
    return df


def _get_manifold_data():
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
    if not response.ok:
        print("ERROR: Request to Manifold Markets API failed.")
        raise Exception("Problem downloading Manifold data.")
    return response


def _update_questions(df, response):
    """Update the data in `df` given the latest Manifold market info contained in `response`."""
    utc_datetime_obj = datetime.now(timezone.utc)
    utc_datetime_str = utc_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    utc_date_str = utc_datetime_obj.strftime("%Y-%m-%d")

    def _get_market_values(value):
        return {"datetime": utc_datetime_str, "value": value}

    def _entry_exists_for_today(market_values):
        for entry in market_values:
            if entry["datetime"].startswith(utc_date_str):
                return True
        return False

    new_markets = []
    for market in response.json():
        market_w_id = df[df["id"] == market["id"]]
        if market_w_id.empty:
            print(f"Adding new market `{market['id']}`")
            new_markets.append(
                {
                    "id": market["id"],
                    "question": market["question"],
                    "resolution_criteria": (
                        "Resolves to the market value on https://manifold.markets at 12AM UTC."
                    ),
                    "resolved": False,
                    "market_values": [_get_market_values(market["probability"])],
                }
            )
        else:
            index = market_w_id.index[0]
            if not market_w_id.at[index, "resolved"]:
                # save the latest market value and resolve if market resolved
                print(f"Updating market `{market['id']}`")
                df.at[index, "resolved"] = market["isResolved"]
                market_values = market_w_id.at[index, "market_values"].copy()
                if not _entry_exists_for_today(market_values):
                    market_values.append(_get_market_values(market["probability"]))
                    df.at[index, "market_values"] = market_values

    return pd.concat([df, pd.DataFrame(new_markets)], ignore_index=True)


def driver(event, context):
    """Generate questions from Manifold API and update question file in GCP Cloud Storage."""
    # Download existing questions from cloud storage
    df = _get_stored_question_data()

    # Get the latest Manifold data
    response = _get_manifold_data()

    # Update the existing questions
    df = _update_questions(df, response)

    # Save and upload
    records = df.to_dict(orient="records")
    with open(local_filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(records, ensure_ascii=False))

    storage.upload(
        bucket_name=bucket_name,
        local_filename=local_filename,
    )
    print("Done.")


if __name__ == "__main__":
    driver(None, None)
