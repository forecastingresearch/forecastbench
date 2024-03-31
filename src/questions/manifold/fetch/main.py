"""Fetch data from Manifold API."""

import json
import os
import sys
from datetime import datetime, timezone

import backoff
import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

source = "manifold"
local_filename = f"/tmp/{source}_fetch.jsonl"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
manifold_topic_slugs = ["entertainment", "sports-default", "technology-default"]


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
    """Get the top 100 markets from Manifold Markets."""
    print("Calling Manifold search-markets endpoint")
    endpoint = "https://api.manifold.markets/v0/search-markets"
    params = {
        "sort": "most-popular",
        "contractType": "BINARY",
        "filter": "open",
        "limit": 100,
    }

    response = requests.get(endpoint, params=params, verify=certifi.where())
    utc_datetime_obj = datetime.now(timezone.utc)
    if not response.ok:
        print(f"ERROR: Request to Manifold Markets API endpoint {endpoint} failed.")
        response.raise_for_status()
    df = pd.DataFrame(response.json())
    df["fetch_datetime"] = utc_datetime_obj

    for topic in topics:
        response = requests.get(
            endpoint, params={**params, "topicSlug": topic}, verify=certifi.where()
        )
        utc_datetime_obj = datetime.now(timezone.utc)
        if not response.ok:
            print(f"ERROR: Request to Manifold Markets API endpoint {endpoint} failed.")
            response.raise_for_status()
        df_tmp = pd.DataFrame(response.json())
        df_tmp["fetch_datetime"] = utc_datetime_obj
        df = pd.concat([df, df_tmp], ignore_index=True)

    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)
    df["fetch_datetime"] = df["fetch_datetime"].astype(str)
    df["background"] = "N/A"
    df["resolution_datetime"] = "N/A"
    df["source_resolution_criteria"] = "N/A"
    df["begin_datetime"] = pd.to_datetime(df["createdTime"], unit="ms", utc=True).astype(str)
    df["close_datetime"] = pd.to_datetime(df["closeTime"], unit="ms", utc=True).astype(str)
    df["resolved"] = False
    df["resolution_datetime"] = "N/A"
    return df[
        [
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
    ].sort_values(by="id")


def driver(_):
    """Fetch Manifold data and update question file in GCP Cloud Storage."""
    # Get the latest Manifold data
    df = _get_data(manifold_topic_slugs)

    # Save
    with open(local_filename, "w", encoding="utf-8") as f:
        # can't use `df.to_json` because we don't want escape chars
        for record in df.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_filename,
    )
    print("Done.")
    return "OK", 200


if __name__ == "__main__":
    driver(None)
