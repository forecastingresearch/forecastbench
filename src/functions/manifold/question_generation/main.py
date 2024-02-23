"""Generate questions from Manifold API."""

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

json_filename = "manifold.json"
local_filename = f"/tmp/{json_filename}"
ref_storage_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")
destination_folder = "questions"
bucket_file = f"{destination_folder}/{json_filename}"


def _extract_text(d) -> str:
    """
    Extract 'text' values from a nested dictionary/list structure.

    :param d: Input dictionary or list.
    :return: Concatenated string of all 'text' values.
    """
    if isinstance(d, dict):
        return " ".join(
            _extract_text(v) for k, v in d.items() if k == "text" or isinstance(v, (dict, list))
        )
    elif isinstance(d, list):
        return " ".join(_extract_text(item) for item in d)
    else:
        return d


def _get_market(market: str) -> tuple:
    """Get details on a singlne market."""
    url = f"https://api.manifold.markets/v0/market/{market}"
    response = requests.get(url)
    if not response.ok:
        Exception(f"Error getting specific data for market {market}.")
    market = response.json()
    return (_extract_text(market["description"]), market["closeTime"])


def _get_search_markets(params: dict):
    """Generate questions from Manifold API and update question file in GCP Cloud Storage."""
    print("Downloading json file from storage")
    try:
        storage.download(
            bucket_name=ref_storage_bucket,
            filename=bucket_file,
            local_filename=local_filename,
        )
        df = pd.read_json(local_filename)
    except Exception:
        df = pd.DataFrame(columns=["id", "resolved", "question", "description", "close_time"])

    print("Calling Manifold search-markets endpoint")
    url = "https://api.manifold.markets/v0/search-markets"
    response = requests.get(url, params=params)
    if not response.ok:
        raise Exception("Requset to Manifold Markets API failed")

    new_markets = []
    for market in response.json():
        if market["id"] not in df["id"].values:
            print(f"Getting Manifold market info for {market['id']}")
            new_market = {
                "id": market["id"],
                "resolved": False,
                "question": market["question"],
            }
            new_market["description"], new_market["close_time"] = _get_market(market["id"])
            new_markets.append(new_market)

    df = pd.concat([df, pd.DataFrame(new_markets)], ignore_index=True)
    df.to_json(local_filename)
    storage.upload(
        bucket_name=ref_storage_bucket,
        local_filename=local_filename,
        destination_folder=destination_folder,
    )


def driver(request):
    """Google Cloud Function Driver."""
    _get_search_markets(params)
    return "OK", 200


if __name__ == "__main__":
    driver(None, None)
