"""Generate questions from Manifold API."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pprint import pprint

import backoff
import certifi
import numpy as np
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

basename = "acled"
json_market_filename = f"{basename}.json"
local_market_filename = f"/tmp/{json_market_filename}"
json_market_values_filename = f"{basename}_values.json"
local_market_values_filename = f"/tmp/{json_market_values_filename}"
bucket_name = os.environ.get("CLOUD_STORAGE_BUCKET")
API_KEY = os.environ.get("API_KEY_ACLED")
API_EMAIL = os.environ.get("API_EMAIL_ACLED")


def _get_stored_question_data():
    """Download Acled question data from cloud storage."""
    dfq = pd.DataFrame(
        columns=[
            "id",
            "question",
            "description",
            "close_time",
            "resolution_criteria",
            "resolved",
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
def _get_data():
    """Get ACLED data for the past 30 days."""
    print("Calling Acled search-markets endpoint")
    endpoint = "https://api.acleddata.com/acled/read"
    utc_datetime_obj = datetime.now(timezone.utc)
    start_date = (utc_datetime_obj - timedelta(days=30)).strftime("%Y-%m-%d")
    regions = [
        "1|2|3|4|5", # Africa
        "7|8|9|13|17", # Asia
        "11", # Middle East
        "12", # Europe
        "14|15|16|18", # Americas
        "19|20", # Oceana, Antarctica
    ]
    params = {
        "key": API_KEY,
        "email": API_EMAIL,
        "event_date": start_date,
        "event_date_where":"<",
    }

    for region in regions:
        params["region"] = region
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            print(f"ERROR: Request to Acled Markets API endpoint {endpoint} failed.")
            response.raise_for_status()
        print()
        print(region)
        print(len(response.json()["data"]))

    aoeu
    return [(utc_datetime_obj, response.json())]


def _update_questions(dfq, dfmv, datetime_and_markets):
    """Update the dataframes given the latest Acled market info."""

    def _get_market_value_entry(market_id, utc_datetime_obj, value):
        return {
            "id": market_id,
            "datetime": utc_datetime_obj,
            "value": value,
        }

    def _entry_exists_for_today(market_values, utc_date_str):
        return market_values["datetime"].dt.strftime("%Y-%m-%d").eq(utc_date_str).any()

    new_markets = []
    new_market_values = []
    utc_datetime_obj, markets = datetime_and_markets[0]
    utc_date_str = utc_datetime_obj.strftime("%Y-%m-%d")
    df_acled = pd.DataFrame(markets["data"])
    print(df_acled.columns)
    print(df_acled["admin1"].unique())
    print(df_acled["admin2"].unique())
    print(df_acled["admin3"].unique())
    aoeu
    countries = sorted(df_acled["country"].unique())
    
    for country in countries:
        dfc = df_acled[df_acled["country"] == country]
        pprint(dfc)
        continue
        for market in markets.get("data",[]):
            print()
            pprint(market)
            print()
            return
            market_for_id = dfq[dfq["id"] == market["id"]]
            description, close_time = _get_market_description_and_close_time(market["id"])
            if market_for_id.empty:
                print(f"Adding new market `{market['id']}`")
                new_markets.append(
                    {
                        "id": market["id"],
                        "question": market["question"],
                        "description": description,
                        "close_time": close_time,
                        "resolution_criteria": (
                            "Resolves to the resolved value according to Acled. "
                            "If the question is unresolved, resolves to the market value on "
                            f"{market['url']} at 12AM UTC."
                        ),
                        "resolved": False,
                    }
                )
                new_market_values.append(
                    _get_market_value_entry(market["id"], utc_datetime_obj, market["probability"])
                )
            else:
                index = market_for_id.index[0]
                if not market_for_id.at[index, "resolved"]:
                    print(f"Updating market `{market['id']}`")
                    dfq.at[index, "resolved"] = market["isResolved"]
                    dfq.at[index, "description"] = description
                    if not _entry_exists_for_today(dfmv[dfmv["id"] == market["id"]], utc_date_str):
                        dfmv.loc[len(dfmv)] = _get_market_value_entry(
                            market["id"],
                            utc_datetime_obj,
                            _get_potentially_resolved_market_value(market),
                        )

    if new_markets:
        dfq = (
            pd.DataFrame(new_markets)
            if dfq.empty
            else pd.concat([dfq, pd.DataFrame(new_markets)], ignore_index=True)
        )
        dfmv = (
            pd.DataFrame(new_market_values)
            if dfmv.empty
            else pd.concat([dfmv, pd.DataFrame(new_market_values)], ignore_index=True)
        )

    if isinstance(dfq.iloc[0]["close_time"], datetime):
        dfq["close_time"] = dfq["close_time"].apply(lambda x: x.isoformat())

    dfmv["id"] = dfmv["id"].astype(str)
    dfmv["datetime"] = pd.to_datetime(dfmv["datetime"], utc=True, errors="coerce")
    dfmv = dfmv.sort_values(by=["id", "datetime"])
    return dfq, dfmv


def driver(event, context):
    """Generate questions from Acled API and update question file in GCP Cloud Storage."""
    # Get the latest Acled data
    pickle_file_path = 'pik.pik'
    import pickle
    if os.path.exists(pickle_file_path):
        with open(pickle_file_path, 'rb') as file:
            response = pickle.load(file)
    else:
        response = _get_data()
        with open(pickle_file_path, 'wb') as file:
            pickle.dump(response, file)

    # Download existing questions from cloud storage
    dfq, dfmv = _get_stored_question_data()

    # Update the existing questions
    dfq, dfmv = _update_questions(dfq, dfmv, response)

    # Save and upload
    with open(local_market_filename, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfmv.to_json(local_market_values_filename, orient="records", lines=True, date_format="iso")
    return
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
