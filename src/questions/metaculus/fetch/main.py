"""Fetch data from Metaculus API."""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

import backoff
import certifi
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import (  # noqa: E402
    data_utils,
    dates,
    decorator,
    env,
    keys,
    metaculus,
    question_curation,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

filenames = data_utils.generate_filenames(source="metaculus")

MIN_NUM_FORECASTERS_ON_MARKET = 50


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def call_endpoint(additional_params=None):
    """Get the top 100 markets from Metaculus."""
    ids = set()
    endpoint = "https://www.metaculus.com/api/posts/"
    params = {
        "statuses": "open",
        "with_cp": "false",
        "scheduled_resolve_time__gt": (
            dates.get_date_today() + timedelta(days=question_curation.FREEZE_WINDOW_IN_DAYS)
        ).strftime("%Y-%m-%d"),
        "forecast_type": "binary",
        "order_by": "-hotness",
        "limit": 150,  # not listed as a parameter but valid
        "for_main_feed": "true",  # not listed as a parameter but valid
    }
    if additional_params:
        params.update(additional_params)
    logger.info(f"Calling {endpoint} with additional params {additional_params}")

    headers = {"Authorization": f"Token {keys.API_KEY_METACULUS}"}
    response = requests.get(endpoint, params=params, headers=headers, verify=certifi.where())
    if not response.ok:
        logger.error("Request to Metaculus API endpoint failed.")
        response.raise_for_status()

    for market in response.json()["results"]:
        if market["nr_forecasters"] > MIN_NUM_FORECASTERS_ON_MARKET:
            if "cp_reveal_time" in market["question"]:
                cp_reveal_date = market["question"]["cp_reveal_time"]
                cp_reveal_date = datetime.strptime(cp_reveal_date[:10], "%Y-%m-%d").date()
                if cp_reveal_date < dates.get_date_today():
                    ids.add(str(market["id"]))
    return ids


def get_data():
    """Get pertinent Metaculus questions and data."""
    logger.info("Calling Metaculus search-markets endpoint")
    ids = call_endpoint()
    for topic in metaculus.CATEGORIES:
        ids = ids.union(call_endpoint(additional_params={"categories": topic}))
    return sorted(ids)


@decorator.log_runtime
def driver(_):
    """Fetch Metaculus data and update fetch file in GCP Cloud Storage."""
    ids = get_data()

    with open(filenames["local_fetch"], "w") as f:
        for id_str in ids:
            f.write(json.dumps({"id": id_str}) + "\n")

    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
