"""
Generate questions from Wikidata Heads of state table.

https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads
"""

import json
import os
import sys
from datetime import datetime, timezone

import backoff
import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

json_market_filename = "wikidata_hos_hog.json"
local_market_filename = f"/tmp/{json_market_filename}"
json_market_values_filename = "wikidata_hos_hog_values.json"
local_market_values_filename = f"/tmp/{json_market_values_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
hos_hog_url = (
    "https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/"
    "lists/current_heads"
)


def _get_stored_question_data():
    """Download Wikidata Heads of State/Gov question data from cloud storage."""
    dfq = pd.DataFrame(
        columns=[
            "id",
            "question",
            "resolution_criteria",
        ]
    )
    dfmv = pd.DataFrame(
        columns=[
            "id",
            "datetime",
            "value",
        ]
    )
    try:
        print(f"Get questions from {bucket_name}/{json_market_filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_market_filename,
            local_filename=local_market_filename,
        )
        dfq_tmp = pd.read_json(local_market_filename, lines=True)
        if not dfq_tmp.empty:
            dfq = dfq_tmp

        print(f"Get market values from {bucket_name}/{json_market_values_filename}")
        gcp.storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_market_values_filename,
            local_filename=local_market_values_filename,
        )
        dfmv_tmp = pd.read_json(local_market_values_filename, lines=True)
        if not dfmv_tmp.empty:
            dfmv = dfmv_tmp
    except Exception:
        pass
    return dfq, dfmv


def _print_error_info_handler(details):
    print(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=_print_error_info_handler,
)
def _get_wikidata_data():
    """
    Download data used to create the Wikidata Heads of state and government table.

    https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads
    """
    query = """
    SELECT ?country ?countryLabel
           ?headOfGovernment ?headOfGovernmentLabel
           ?headOfState ?headOfStateLabel
    WHERE {
           ?country p:P31/ps:P31 wd:Q6256 . ?country p:P1813 [ pq:P31 wd:Q28840786 ]
           OPTIONAL { ?country wdt:P35 ?headOfState. }
           OPTIONAL { ?country wdt:P6 ?headOfGovernment. }
           SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """

    endpoint = "https://query.wikidata.org/sparql"
    params = {"format": "json", "query": query}
    response = requests.get(endpoint, params=params, verify=certifi.where())
    utc_datetime_obj = datetime.now(timezone.utc)
    if not response.ok:
        print("Request failed with status code:", response.status_code)
        print("Response text:", response.text[:500])
        response.raise_for_status()

    df_hos_hog = pd.DataFrame(response.json()["results"]["bindings"])
    df_hos_hog["country"] = df_hos_hog["countryLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df_hos_hog["headOfState"] = df_hos_hog["headOfStateLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df_hos_hog["headOfGovernment"] = df_hos_hog["headOfGovernmentLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df_hos_hog = df_hos_hog[
        (df_hos_hog["headOfState"].notna()) & (df_hos_hog["headOfGovernment"].notna())
    ]
    df_hos_hog = df_hos_hog[["country", "headOfState", "headOfGovernment"]]

    return utc_datetime_obj, df_hos_hog


def _update_questions(dfq, dfmv, datetime_and_markets):
    """Update the data in `df` given the latest Wikidata info contained in `response`."""
    utc_datetime_obj, df_hos_hog = datetime_and_markets
    utc_date_str = utc_datetime_obj.strftime("%Y-%m-%d")

    def _formulate_wikidata_question(country, head_type, heads):
        """
        Formulate the appropriate wikidata question given the inputs.

        country: country name
        head_type: either "state" or "government"
        heads: a string array with the given heads of state/government
        """
        if not country or not head_type or not heads:
            return ""

        if len(heads) == 1:
            plural = ""
            head_list = heads[0]
        else:
            plural = "s"
            if len(heads) == 2:
                head_list = ", ".join(heads[:-1]) + " and " + heads[-1]
            else:
                head_list = ", ".join(heads[:-1]) + ", and " + heads[-1]

        return f"Will {head_list} still be the head{plural} of {head_type} of {country}?"

    def _get_market_value_entry(market_id, heads):
        return {
            "id": market_id,
            "datetime": utc_datetime_obj,
            "value": heads,
        }

    def _entry_exists_for_today(market_values, utc_date_str):
        return market_values["datetime"].dt.strftime("%Y-%m-%d").eq(utc_date_str).any()

    def _make_market_id(country, head_type):
        return f"{country}_head_of_{head_type}"

    def _add_or_update_country_market(dfq, dfmv, df_country, head_type):
        heads = df_country[f"headOf{head_type.capitalize()}"].unique().tolist()
        if not heads:
            return dfq, dfmv

        country = df_country["country"].iloc[0]
        market_id = _make_market_id(country, head_type)
        market_for_id = dfq[dfq["id"] == market_id]
        if market_for_id.empty:
            print(f"Adding new market `{market_id}`")
            new_market = {
                "id": market_id,
                "question": _formulate_wikidata_question(country, head_type, heads),
                "resolution_criteria": (
                    f"Resolves to the value of 'head of {head_type}' for {country} "
                    f"on {hos_hog_url} at 12AM UTC."
                ),
            }
            dfq = pd.concat([dfq, pd.DataFrame([new_market])], ignore_index=True)
            dfmv = pd.concat(
                [
                    dfmv,
                    pd.DataFrame([_get_market_value_entry(market_id, heads)]),
                ],
                ignore_index=True,
            )
        else:
            print(f"Updating market `{market_id}`")
            if not _entry_exists_for_today(dfmv[dfmv["id"] == market_id], utc_date_str):
                dfmv.loc[len(dfmv)] = _get_market_value_entry(market_id, heads)
        return dfq, dfmv

    for country in df_hos_hog["country"].unique():
        dfc = df_hos_hog[df_hos_hog["country"] == country]
        dfq, dfmv = _add_or_update_country_market(dfq, dfmv, dfc, "state")
        dfq, dfmv = _add_or_update_country_market(dfq, dfmv, dfc, "government")

    dfq = dfq.sort_values(by=["id"])
    dfmv = dfmv.sort_values(by=["id", "datetime"])
    return dfq, dfmv


def driver(event, context):
    """Generate questions from Wikidata and update question file in GCP Cloud Storage."""
    # Download existing questions from cloud storage
    dfq, dfmv = _get_stored_question_data()

    # Get the latest Wikidata data
    response = _get_wikidata_data()

    # Update the existing questions
    dfq, dfmv = _update_questions(dfq, dfmv, response)

    # Save and upload
    with open(local_market_filename, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfmv.to_json(local_market_values_filename, orient="records", lines=True, date_format="iso")

    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_filename,
    )
    gcp.storage.upload(
        bucket_name=bucket_name,
        local_filename=local_market_values_filename,
    )
    print("Done.")


if __name__ == "__main__":
    driver(None, None)
