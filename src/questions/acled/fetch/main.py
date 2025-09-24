"""Fetch data from Acled API."""

import json
import logging
import os
import sys
from typing import Any

import backoff
import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import acled, constants, data_utils, decorator, env, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "acled"
filenames = data_utils.generate_filenames(source=source)
# Need 2 years of data to get monthly average over the year
# As ACLED only uses > filter so >2022 gets 2023 or more recent, providing yearly average for
# questions in 2024
ACLED_START_YEAR = constants.BENCHMARK_START_YEAR - 2


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
    max_time=60,
    on_backoff=data_utils.print_error_info_handler,
)
def get_access_token() -> str:
    """
    Authenticate with the ACLED API and retrieves an access token.

    Returns:
        str: The access token if the request is successful.
    """
    logger.info("Get ACLED access token.")
    endpoint = "https://acleddata.com/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    params = {
        "username": keys.API_EMAIL_ACLED,
        "password": keys.API_PASSWORD_ACLED,
        "grant_type": "password",
        "client_id": "acled",
    }

    try:
        response = requests.post(endpoint, headers=headers, data=params)
        logger.debug(f"Response status code: {response.status_code}")
        logger.debug(f"Response headers: {response.headers}")
        logger.debug(f"Response content: {response.text}")
        response.raise_for_status()

        data = response.json()
        if "access_token" not in data:
            raise ValueError("Access token not found in response")
        return data["access_token"]

    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"Failed to authenticate with ACLED API: {str(e)}"
        )
    except ValueError as e:
        raise ValueError(f"Error processing API response: {str(e)}")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def get_acled_events(access_token: str) -> pd.DataFrame:
    """
    Fetch data from the ACLED API and return it as a pandas DataFrame.

    Args:
        access_token (str): OAuth2 bearer token for authenticating with the ACLED API.

    Returns:
        pd.DataFrame: A DataFrame containing all retrieved ACLED events with standardized columns.
    """
    endpoint = "https://acleddata.com/api/acled/read?_format=json"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    params = {
        "fields": "|".join(acled.FETCH_COLUMNS),
        "year": ACLED_START_YEAR,
        "year_where": ">",
        "page": 0,
    }

    seen_ids = set()
    dfs = []
    df = pd.DataFrame(columns=acled.FETCH_COLUMNS)
    while True:
        params["page"] += 1
        logger.info(f"Downloading page {params['page']}")
        response = requests.get(endpoint, headers=headers, params=params, verify=certifi.where())

        if not response.ok:
            logger.error(f"Request to ACLED API endpoint {endpoint} failed with params {params}")
        response.raise_for_status()
        data = response.json()

        if data["count"] == 0:
            break

        df_tmp = pd.DataFrame(data["data"]).astype(acled.FETCH_COLUMN_DTYPE)
        df_new_rows = df_tmp[~df_tmp["event_id_cnty"].isin(seen_ids)]
        seen_ids.update(df_new_rows["event_id_cnty"])
        dfs.append(df_new_rows)

    df = pd.concat(dfs, ignore_index=True).sort_values(by="event_id_cnty", ignore_index=True)
    logger.info(f"Downloaded {len(df)} rows.")
    return df


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Acled data and store in GCP Cloud Storage."""
    # Get the latest ACLED data
    logger.info("Downloading ACLED data.")
    access_token = get_access_token()
    df = get_acled_events(access_token=access_token)

    if df.empty:
        logger.error("No ACLED data was downloaded.")
        return

    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
