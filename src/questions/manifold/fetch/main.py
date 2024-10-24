"""Fetch data from Manifold API."""

import json
import logging
import os
import sys

import backoff
import certifi
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

filenames = data_utils.generate_filenames(source="manifold")

MANIFOLD_TOPIC_SLUGS = ["entertainment", "sports-default", "technology-default"]


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=500,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint(ids, additional_params=None):
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
    if not response.ok:
        logger.error(
            f"Request to endpoint failed for {endpoint}: {response.status_code} Error. "
            f"{response.text}"
        )
        response.raise_for_status()

    ids.update(market["id"] for market in response.json())
    return ids


def _get_data():
    """Get pertinent Manifold questions and data."""
    logger.info("Calling Manifold search-markets endpoint")
    ids = _call_endpoint(set())
    for topic in MANIFOLD_TOPIC_SLUGS:
        ids = _call_endpoint(ids, {"topicSlug": topic})
    return sorted(ids)


@decorator.log_runtime
def driver(_):
    """Fetch Manifold data and update question file in GCP Cloud Storage."""
    # Get the latest Manifold data
    ids = _get_data()

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
