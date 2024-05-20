"""Fetch data from Acled API."""

import json
import logging
import os
import sys

import backoff
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "dbnomics"
filenames = data_utils.generate_filenames(source=source)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint(id, record_start_date):
    """Fetch data from DBnomics."""
    logger.info(f"Calling DBnomics for series {id}")
    endpoint = "https://api.db.nomics.world/v22/series/" + id
    params = {"observations": "true"}
    response = requests.get(url=endpoint, params=params)
    if not response.ok:
        logger.error("Request to DBnomics API endpoint failed.")
        response.raise_for_status()
    data = response.json()
    docs = data["series"]["docs"][0]
    df = pd.DataFrame(
        {
            "id": id,
            "period": docs["period"],
            "value": docs["value"],
            "provider_name": data["provider"]["name"],
            "dataset_name": docs["dataset_name"],
            "series_name": docs["series_name"],
        }
    )
    df["period"] = pd.to_datetime(df["period"])
    # Filter to record_start_date and beyond
    df = df[df["period"] >= record_start_date].reset_index(drop=True)
    return df


@decorator.log_runtime
def driver(_):
    """Fetch DBnomics data and store in GCP Cloud Storage."""
    # Get the latest DBnomics data
    logger.info("Downloading DBnomics data.")
    seriesIds = constants.DBNOMICS_DATA

    df = None

    for row in seriesIds.itertuples():
        id = row.id
        record_start_date = row.record_start_date
        new_rows = _call_endpoint(id=id, record_start_date=record_start_date)
        if df is not None:
            df = pd.concat([df, new_rows])
        else:
            df = new_rows

    df["period"] = df["period"].astype(str)

    # Save
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME, local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
