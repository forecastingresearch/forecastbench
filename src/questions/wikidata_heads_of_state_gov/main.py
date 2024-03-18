"""
Generate questions from Wikidata Heads of state table.

https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads
"""

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
import gcp.storage.run as storage  # noqa: E402

json_filename = "wikidata_hos_hog.json"
local_filename = f"/tmp/{json_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")


def _get_stored_question_data():
    """Download Manifold question data from cloud storage."""
    print(f"Get questions from {bucket_name}/{json_filename}")
    df = pd.DataFrame(
        columns=[
            "id",
            "question",
            "resolution_criteria",
            "market_values",
        ]
    )
    try:
        storage.download_no_error_message_on_404(
            bucket_name=bucket_name,
            filename=json_filename,
            local_filename=local_filename,
        )
        df = pd.read_json(local_filename)
    except Exception:
        pass
    return df


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

    url = "https://query.wikidata.org/sparql"
    response = requests.get(
        url, headers={"User-Agent": "Mozilla/5.0"}, params={"format": "json", "query": query}
    )

    if not response.ok:
        print("Request failed with status code:", response.status_code)
        print("Response text:", response.text[:500])
        raise Exception("Problem downloading Wikidata Heads of State data.")

    try:
        return response.json()
    except Exception:
        raise Exception("Error decoding Wikidata JSON.")


def _update_questions(df, response):
    """Update the data in `df` given the latest Wikidata info contained in `response`."""
    utc_datetime_obj = datetime.now(timezone.utc)
    utc_datetime_str = utc_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    utc_date_str = utc_datetime_obj.strftime("%Y-%m-%d")

    # Process results from Wikidata
    results = response["results"]["bindings"]
    df_hos_hog = pd.DataFrame(results)
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

    def _get_market_values(head_type, heads):
        return {
            "datetime": utc_datetime_str,
            f"heads_of_{head_type}": heads,
        }

    def _entry_exists_for_today(market_values):
        for entry in market_values:
            if entry["datetime"].startswith(utc_date_str):
                return True
        return False

    def _make_market_id(country, head_type):
        return f"{country}_head_of_{head_type}"

    def _add_or_update_country_market(df, df_country, head_type):
        heads = df_country[f"headOf{head_type.capitalize()}"].unique().tolist()
        country = df_country["country"].iloc[0]
        market_id = _make_market_id(country, head_type)
        market_w_id = df[df["id"] == market_id]
        if market_w_id.empty:
            if len(heads) > 0:
                # There is an actual value for this position (sometimes it's empty)
                print(f"Adding new market `{market_id}`")
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            {
                                "id": market_id,
                                "question": _formulate_wikidata_question(country, head_type, heads),
                                "resolution_criteria": (
                                    f"Resolves to the value of 'head of {head_type}' for {country} "
                                    "on "
                                    "https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads"  # noqa: E501
                                    " at 12AM UTC."
                                ),
                                "market_values": [
                                    _get_market_values(head_type, heads),
                                ],
                            }
                        ),
                    ],
                    ignore_index=True,
                )
        else:
            print(f"Updating market `{market_id}`")
            index = market_w_id.index[0]
            market_values = market_w_id.at[index, "market_values"].copy()
            if isinstance(market_values, dict):
                market_values = [market_values]
            if not _entry_exists_for_today(market_values):
                market_values.append(_get_market_values(head_type, heads))
                df.at[index, "market_values"] = market_values
        return df

    for country in df_hos_hog["country"].unique():
        dfc = df_hos_hog[df_hos_hog["country"] == country]
        df = _add_or_update_country_market(df, dfc, "state")
        df = _add_or_update_country_market(df, dfc, "government")
    return df


def driver(event, context):
    """Generate questions from Wikidata and update question file in GCP Cloud Storage."""
    # Download existing questions from cloud storage
    df = _get_stored_question_data()

    # Get the latest Wikidata data
    response = _get_wikidata_data()

    # Update the existing questions
    df = _update_questions(df, response)

    # Save and upload
    records = df.to_dict(orient="records")
    with open(local_filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(records, ensure_ascii=False))
    storage.upload(
        bucket_name=bucket_name,
        local_filename=local_filename,
    )
    print("Done.")


if __name__ == "__main__":
    driver(None, None)
