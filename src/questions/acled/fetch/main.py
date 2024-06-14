"""Fetch data from Acled API."""

import json
import logging
import os
import sys

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
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint():
    """Fetch data from Acled."""
    endpoint = "https://api.acleddata.com/acled/read"
    params = {
        "key": keys.API_KEY_ACLED,
        "email": keys.API_EMAIL_ACLED,
        "fields": "|".join(acled.FETCH_COLUMNS),
        "year": ACLED_START_YEAR,
        "year_where": ">",
    }

    page = 0
    df = pd.DataFrame(columns=acled.FETCH_COLUMNS)
    while True:
        page += 1
        logger.info(f"Downloading page {page}")
        params["page"] = page
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(f"Request to ACLED API endpoint {endpoint} failed with params {params}")
            response.raise_for_status()
        if response.json()["count"] == 0:
            break
        df_tmp = pd.DataFrame(response.json()["data"])
        df_tmp = df_tmp.astype(acled.FETCH_COLUMN_DTYPE)
        rows_to_append = df_tmp[~df_tmp["event_id_cnty"].isin(df["event_id_cnty"])]
        df = df_tmp if df.empty else pd.concat([df, rows_to_append], ignore_index=True)

    logger.info(f"Downloaded {len(df)} rows.")
    return df.sort_values(by="event_id_cnty", ignore_index=True)


@decorator.log_runtime
def driver(_):
    """Fetch Acled data and store in GCP Cloud Storage."""
    # Get the latest ACLED data
    logger.info("Downloading ACLED data.")
    df = _call_endpoint()

    # Save
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
