"""Fetch data from Manifold API."""

import json
import logging
import os
import sys

import backoff
import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, dates  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


source = "manifold"
local_filename = f"/tmp/{source}_fetch.jsonl"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
manifold_topic_slugs = ["entertainment", "sports-default", "technology-default"]


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint(df, additional_params=None):
    """Get the top 100 markets from Manifold Markets."""
    endpoint = "https://api.manifold.markets/v0/search-markets"
    params = {
        "sort": "most-popular",
        "contractType": "BINARY",
        "filter": "open",
        "limit": 100,
    }
    if additional_params:
        params.update(additional_params)
    logger.info(f"Calling {endpoint} with additional params {additional_params}")

    response = requests.get(endpoint, params=params, verify=certifi.where())
    utc_datetime_str = dates.get_datetime_now()
    if not response.ok:
        logger.error(
            f"Request to endpoint failed for {endpoint}: {response.status_code} Error. "
            f"{response.text}"
        )
        response.raise_for_status()
    df_tmp = pd.DataFrame(response.json())
    if df.empty and df_tmp.empty:
        return df

    if not df_tmp.empty:
        # removing potentially null columns to avoid `pd.concat` FutureWarning
        df_tmp = df_tmp[
            [
                "id",
                "question",
                "createdTime",
                "closeTime",
                "url",
                "probability",
            ]
        ]
        df_tmp["fetch_datetime"] = utc_datetime_str
        df = df_tmp if df.empty else pd.concat([df, df_tmp], ignore_index=True)

    return df


def _get_data(topics):
    """Get pertinent Manifold questions and data."""
    logger.info("Calling Manifold search-markets endpoint")
    df = _call_endpoint(pd.DataFrame())
    for topic in topics:
        df = _call_endpoint(df, {"topicSlug": topic})

    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)
    df["fetch_datetime"] = df["fetch_datetime"]
    df["background"] = "N/A"
    df["source_resolution_criteria"] = "N/A"
    df["begin_datetime"] = df["createdTime"].apply(dates.convert_epoch_time_in_ms_to_iso)
    df["close_datetime"] = df["closeTime"].apply(dates.convert_epoch_time_in_ms_to_iso)
    df["resolved"] = False
    df["resolution_datetime"] = "N/A"
    df = df.dropna(subset=["probability"])
    df = df.astype(data_utils.QUESTION_FILE_COLUMN_DTYPE)
    return df[
        data_utils.QUESTION_FILE_COLUMNS
        + [
            "fetch_datetime",
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

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
