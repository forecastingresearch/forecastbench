"""
Generate questions from Wikidata Heads of state table.

https://www.wikidata.org/wiki/Wikidata:WikiProject_Heads_of_state_and_government/lists/current_heads
"""

import os
import sys

import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
import gcp.storage.run as storage  # noqa: E402

params = {
    "sort": "most-popular",
    "contractType": "BINARY",
    "filter": "open",
    "limit": 100,
}

json_filename = "wikidata.json"
local_filename = f"/tmp/{json_filename}"
ref_storage_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")
destination_folder = "questions"
bucket_file = f"{destination_folder}/{json_filename}"


def _formulate_wikidata_question(heads, head_name, country):
    """
    Formulate the appropriate wikidata question given the inputs.

    heads: a string array with the given heads of state/government
    head_name: either "state" or "government"
    country: country name
    """
    if not heads:
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

    return f"Will {head_list} be the head{plural} of {head_name} of {country}?"


def _generate_questions_from_wikidata(df):
    def _make_row(country: str, head_type: str) -> dict:
        dfc = df[df["country"] == country]
        heads = dfc[f"headOf{head_type.capitalize()}"].unique().tolist()
        question = _formulate_wikidata_question(heads, head_type, country)
        return {
            "id": f"{country}_head_of_{head_type}",
            "country": country,
            "heads": heads,
            "head_type": head_type,
            "question": question,
        }

    question_rows = []
    for country in sorted(df["country"].unique()):
        print(f"Generating questions for {country}")
        question_rows.append(_make_row(country, "state"))
        question_rows.append(_make_row(country, "government"))
    return pd.DataFrame(question_rows)


def _get_wikidata():
    """
    Download data used to create the Wikidata  Heads of state and government table.

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
        raise Exception("Problem downloading Wikidata.")

    try:
        data = response.json()
    except Exception:
        raise Exception("Error decoding Wikidata JSON")

    results = data["results"]["bindings"]

    df = pd.DataFrame(results)
    df["country"] = df["countryLabel"].apply(lambda x: x["value"] if not pd.isna(x) else None)
    df["headOfState"] = df["headOfStateLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df["headOfGovernment"] = df["headOfGovernmentLabel"].apply(
        lambda x: x["value"] if not pd.isna(x) else None
    )
    df = df[(df["headOfState"].notna()) & (df["headOfGovernment"].notna())]

    return df[["country", "headOfState", "headOfGovernment"]]


def driver(_):
    """Google Cloud Function Driver."""
    df = _get_wikidata()
    df = _generate_questions_from_wikidata(df)
    df.to_json(local_filename, orient="records")
    storage.upload(
        bucket_name=ref_storage_bucket,
        local_filename=local_filename,
        destination_folder=destination_folder,
    )
    return "OK", 200


if __name__ == "__main__":
    driver(None)
