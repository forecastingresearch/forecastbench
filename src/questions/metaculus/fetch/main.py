"""Fetch data from Metaculus API."""

import json
import logging
import os
import sys

import backoff
import certifi
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, decorator, env, keys, metaculus  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

filenames = data_utils.generate_filenames(source="metaculus")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint(ids, additional_params=None):
    """Get the top 100 markets from Metaculus."""
    endpoint = "https://www.metaculus.com/api2/questions/"
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

    headers = {"Authorization": f"Token {keys.API_KEY_METACULUS}"}
    response = requests.get(endpoint, params=params, headers=headers, verify=certifi.where())
    if not response.ok:
        logger.error("Request to Metaculus API endpoint failed.")
        response.raise_for_status()

    ids.update(str(market["id"]) for market in response.json()["results"])
    return ids


def _get_data(topics):
    """Get pertinent Metaculus questions and data."""
    logger.info("Calling Metaculus search-markets endpoint")
    ids = _call_endpoint(set())
    for topic in topics:
        ids = _call_endpoint(ids, {"search": f"include:{topic}"})
    return sorted(ids)


@decorator.log_runtime
def driver(_):
    """Fetch Metaculus data and update fetch file in GCP Cloud Storage."""
    # Don't fetch new questions until API docs are out.
    return

    # Get the latest Manifold data
    ids = _get_data(metaculus.CATEGORIES)

    # Save
    with open(filenames["local_fetch"], "w") as f:
        for id_str in ids:
            f.write(json.dumps({"id": id_str}) + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
