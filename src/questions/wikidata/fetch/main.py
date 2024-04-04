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

source = "wikidata"
local_filename = f"/tmp/{source}_fetch.jsonl"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def _call_endpoint():
    """
    Download data used to create the Wikidata Heads of state and government table.

    https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads
    """
    query = """
    SELECT ?countryLabel
           ?headOfGovernmentLabel
           ?headOfStateLabel
    WHERE {
           ?country p:P31/ps:P31 wd:Q6256 . ?country p:P1813 [ pq:P31 wd:Q28840786 ]
           OPTIONAL { ?country wdt:P35 ?headOfState. }
           OPTIONAL { ?country wdt:P6 ?headOfGovernment. }
           SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?countryLabel
    """
    endpoint = "https://query.wikidata.org/sparql"
    params = {"format": "json", "query": query}
    logger.info(f"Calling {endpoint}")
    response = requests.get(endpoint, params=params, verify=certifi.where())
    utc_datetime_str = dates.get_datetime_now()
    if not response.ok:
        logger.error(f"Request to endpoint failed for {endpoint}")
        response.raise_for_status()
    df = pd.DataFrame(response.json()["results"]["bindings"])
    df["fetch_datetime"] = utc_datetime_str
    return df


def _get_data():
    """Get pertinent Wikidata questions and data."""
    logger.info("Calling Wikidata search-markets endpoint")
    df = _call_endpoint()

    df["country"] = df["countryLabel"].apply(lambda x: x["value"] if not pd.isna(x) else None)
    df["headOfState"] = df["headOfStateLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df["headOfGovernment"] = df["headOfGovernmentLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df = df[(df["headOfState"].notna()) & (df["headOfGovernment"].notna())]
    return df[
        [
            "country",
            "headOfState",
            "headOfGovernment",
            "fetch_datetime",
        ]
    ]


def _generate_questions(df):
    """Generate questions based on the pulled data."""

    def _formulate_question(country, head_type, heads):
        """
        Formulate the appropriate wikidata question given the inputs.

        country: country name
        head_type: either "state" or "government"
        heads: a string array with the given heads of state/government
        """
        if not country or not head_type or not heads:
            return ""

        isare = "is"
        plural = ""
        head_list = heads[0]
        if len(heads) > 1:
            isare = "are"
            plural = "s"
            if len(heads) == 2:
                head_list = ", ".join(heads[:-1]) + " and " + heads[-1]
            else:
                head_list = ", ".join(heads[:-1]) + ", and " + heads[-1]

        return (
            f"The current head{plural} of {head_type} of {country} {isare} {head_list}. "
            f"Will {head_list} be the head{plural} of {head_type} of {country} at the end of "
            "the forecast horizon?"
        )

    def _make_market_id(country, head_type):
        return f"{country}_head_of_{head_type}"

    def _create_market(country, head_type, heads):
        return (
            {
                "id": _make_market_id(country, head_type),
                "question": _formulate_question(country, head_type, heads),
                "probability": heads,
            }
            if heads
            else {}
        )

    markets = []
    for country in df["country"].unique():
        dfc = df[df["country"] == country]
        if not dfc["headOfState"].empty:
            markets.append(_create_market(country, "state", dfc["headOfState"].unique().tolist()))
        if not dfc["headOfGovernment"].empty:
            markets.append(
                _create_market(country, "government", dfc["headOfGovernment"].unique().tolist())
            )

    fetch_datetime = df["fetch_datetime"].unique()[0]
    df = pd.DataFrame(
        markets,
        columns=data_utils.QUESTION_FILE_COLUMNS
        + [
            "fetch_datetime",
            "probability",
        ],
    )

    df["fetch_datetime"] = fetch_datetime
    df["background"] = "N/A"
    df["source_resolution_criteria"] = "N/A"
    df["begin_datetime"] = "N/A"
    df["close_datetime"] = "N/A"
    df["url"] = (
        "https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/"
        "current_heads"
    )
    df["resolved"] = False
    df["resolution_datetime"] = "N/A"
    df = df.astype(
        {
            **data_utils.QUESTION_FILE_COLUMN_DTYPE,
            "fetch_datetime": str,
        }
    )
    return df[
        data_utils.QUESTION_FILE_COLUMNS
        + [
            "fetch_datetime",
            "probability",
        ]
    ].sort_values(by="id")


def driver(_):
    """Fetch Wikidata data and update question file in GCP Cloud Storage."""
    # Get the latest Wikidata data
    df = _get_data()

    # Create questions based on the pulled data
    df = _generate_questions(df)

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
