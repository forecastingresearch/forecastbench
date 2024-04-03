"""Fetch data from Acled API."""

import json
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

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "acled"
local_filename = f"/tmp/{source}_fetch.jsonl"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
API_KEY = os.environ.get("API_KEY_ACLED")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint(df, additional_params=None):
    """Get the top 100 markets from Acled."""
    endpoint = "https://api.acleddata.com/acled/read"
    utc_datetime_obj = dates.get_datetime_now()
    start_date = (utc_datetime_obj - timedelta(days=30)).strftime("%Y-%m-%d")
    regions = [
        "1|2|3|4|5", # Africa
        "7|8|9|13|17", # Asia
        "11", # Middle East
        "12", # Europe
        "14|15|16|18", # Americas
        "19|20", # Oceana, Antarctica
    ]
    params = {
        "key": API_KEY,
        "email": API_EMAIL,
        "event_date": start_date,
        "event_date_where":"<",
    }

    for region in regions:
        params["region"] = region
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            print(f"ERROR: Request to Acled Markets API endpoint {endpoint} failed.")
            response.raise_for_status()



            
    params = {
        "order_by": "-activity",
        "forecast_type": "binary",
        "status": "open",
        "has_group": "false",
        "limit": 100,
        "main-feed": True,
    }
    if additional_params:
        params.update(additional_params)
    logger.info(f"Calling {endpoint} with additional params {additional_params}")
    headers = {"Authorization": f"Token {API_KEY}"}
    response = requests.get(endpoint, params=params, headers=headers, verify=certifi.where())
    utc_datetime_obj = dates.get_datetime_now()
    if not response.ok:
        logger.error("Request to Acled API endpoint failed.")
        response.raise_for_status()
    df_tmp = pd.DataFrame(response.json()["results"])

    if df.empty and df_tmp.empty:
        return df

    if not df_tmp.empty:
        # removing potentially null columns to avoid `pd.concat` FutureWarning
        df_tmp = df_tmp[
            ["id", "title", "publish_time", "close_time", "page_url", "community_prediction"]
        ]
        df_tmp["fetch_datetime"] = utc_datetime_obj
        df = df_tmp if df.empty else pd.concat([df, df_tmp], ignore_index=True)

    return df


def _get_data(topics):
    """Get pertinent Acled questions and data."""
    logger.info("Calling Acled search-markets endpoint")
    df = _call_endpoint(pd.DataFrame())
    for topic in topics:
        df = _call_endpoint(df, {"search": f"include:{topic}"})

    def _extract_probability(market):
        """Parse the forecasts for the community prediction presented on Acled.

        Modifying the API data here because it's too much to keep in git and we can always backout
        the Acled forecasts using the API if there's an error here.
        """
        market_value = market["full"]
        return market_value.get("q2") if isinstance(market_value, dict) else np.nan

    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)
    df["fetch_datetime"] = df["fetch_datetime"]
    df["question"] = df["title"]
    df["background"] = "N/A"
    df["source_resolution_criteria"] = "N/A"
    df["begin_datetime"] = df["publish_time"]
    df["close_datetime"] = df["close_time"]
    df["url"] = "https://www.acled.com" + df["page_url"]
    df["resolved"] = False
    df["resolution_datetime"] = "N/A"
    df["probability"] = df["community_prediction"].apply(_extract_probability)
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
    """Fetch Acled data and update fetch file in GCP Cloud Storage."""
    # Get the latest Manifold data
    df = _get_data(acled_categories)

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
