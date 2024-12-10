"""Fetch data from DBnomics API."""

import json
import logging
import os
import sys

import backoff
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, dbnomics, decorator, env  # noqa: E402

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
def _call_endpoint(id):
    """Fetch data from DBnomics."""
    logger.info(f"Calling DBnomics for series {id}")
    endpoint = "https://api.db.nomics.world/v22/series/" + id
    params = {"observations": "true"}
    response = requests.get(url=endpoint, params=params)
    if not response.ok:
        logger.error("Request to DBnomics API endpoint failed.")
        response.raise_for_status()
    data = response.json()
    docs = data.get("series", {}).get("docs", [{}])[0]
    id_safe = id.replace("/", "_")
    df = pd.DataFrame(
        {
            "id": id_safe,
            "period": docs.get("period"),
            "value": docs.get("value"),
            "provider_name": data.get("provider", {}).get("name"),
            "dataset_name": docs.get("dataset_name"),
            "series_name": docs.get("series_name"),
        }
    )
    df["period"] = pd.to_datetime(df["period"]).dt.date
    # Filter to record start date and beyond
    df = df[
        (df["period"] >= constants.QUESTION_BANK_DATA_STORAGE_START_DATE)
        & (df["period"] < dates.get_date_today())
    ].reset_index(drop=True)
    return df if not df.empty else None


@decorator.log_runtime
def driver(_):
    """Fetch DBnomics data and store in GCP Cloud Storage."""
    # Get the latest DBnomics data
    logger.info("Downloading DBnomics data.")

    df = None

    for row in pd.DataFrame(dbnomics.CONSTANTS).itertuples():
        id = row.id
        new_rows = _call_endpoint(id=id)
        df = new_rows if df is None else pd.concat([df, new_rows])

    df["period"] = df["period"].astype(str)

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


if __name__ == "__main__":
    driver(None)
