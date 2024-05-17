"""FRED fetch new questions script."""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

import backoff
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    dates,
    decorator,
    env,
    fred,
    keys,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "fred"
PARAMS = {
    "api_key": keys.API_KEY_FRED,
    "file_type": "json",
}


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def fetch_paginated_data(url, params, field_name, pagination):
    """
    Fetch data from a paginated API endpoint.

    Args:
        url (str): The API endpoint URL.
        params (dict): Parameters to include in the API request.
        field_name (str): The key in the response JSON containing the data.
        pagination (Union[bool, int]): Control pagination behavior.
            - If False, fetch only the first page.
            - If True, fetch all available pages.
            - If int, fetch up to that many pages.

    Returns:
        list: A list containing all the fetched data.
    """
    all_data = []
    params["offset"] = 0  # Start from the first page
    pages_fetched = 0

    while True:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get(field_name, []):
            break

        all_data.extend(data[field_name])
        pages_fetched += 1

        if pagination is False:
            break
        elif pagination > 1 and pages_fetched >= pagination:
            break

        # Increment the offset for the next page
        params["offset"] += params["limit"]

    return all_data


def fetch_all_releases(params, series_id=None, single=False):
    """
    Fetch release data from the FRED API.

    Args:
        params (dict): Parameters to include in the API request.
        series_id (str, optional): The ID of a specific series to fetch releases for.
            Required if single is True.
        single (bool, optional): Control whether to fetch data for a single series.
            - If False, fetch all releases.
            - If True, fetch releases for the specified series_id.

    Returns:
        list: A list containing all the fetched release data.
    """
    url = "https://api.stlouisfed.org/fred/releases?"
    pagination = True
    params["limit"] = 100

    if single:
        url = "https://api.stlouisfed.org/fred/series/release?"
        params["series_id"] = series_id
        pagination = False

    return fetch_paginated_data(url, params, "releases", pagination)


def fetch_all_series(params, all_releases=None, series_id=None):
    """
    Fetch series data from the FRED API.

    Args:
        params (dict): Parameters to include in the API request.
        all_releases (list, optional): A list of releases to fetch series data for.
            If provided, fetch series data for each release in the list.
        series_id (str, optional): The ID of a specific series to fetch.
            Required if all_releases is None.

    Returns:
        list: A list of releases with their associated series data
            if all_releases is provided.
        list: A list of series data if series_id is provided.
    """
    url = "https://api.stlouisfed.org/fred/release/series?"

    total_series_cnt = 0

    if all_releases:
        for release in tqdm(all_releases, desc="Fetching series"):
            release_id = release["id"]
            params["release_id"] = release_id
            params["limit"] = 100

            logger.info(f"Fetching release_id: {release_id}")
            series_fetch = fetch_paginated_data(
                url,
                params,
                "seriess",
                pagination=20,
            )
            release["series"] = [
                series
                for series in series_fetch
                if series["popularity"] > 50  # popularity score ranges from 0 to 100
                and series["frequency_short"]
                in ["D", "W", "M"]  # frequency is daily, weekly, monthly
                and "Not Applicable" not in series["frequency"]
                and "DISCONTINUED" not in series["title"]
            ]

            total_series_cnt += len(release["series"])

            logger.info(f"Current valid series count: {total_series_cnt}")
            if total_series_cnt > 500:
                break

        return all_releases
    else:
        url = "https://api.stlouisfed.org/fred/series?"
        params["series_id"] = series_id
        logger.info(f"Fetching series_id: {series_id}")
        series_fetch = fetch_paginated_data(url, params, "seriess", pagination=False)

        return series_fetch


def fetch_all_observations(params, series_id):
    """
    Fetch all observations for a given series ID from the FRED.

    Parameters:
    - params (dict): Include necessary API parameters like API key.
    - series_id (str): The ID of the series to fetch observations for.

    Steps:
    1. Set the series ID and limit in the params.
    2. Fetch the paginated data from the API.
    3. Parse the current date and the fetch date from the last observation.
    4. Check if the latest record is at least from last month.
    5. Filter out observations with missing values and format the data.

    Returns:
    - list: A list of dictionaries containing the series ID, date, and value.
    """
    url = "https://api.stlouisfed.org/fred/series/observations?"

    params["series_id"] = series_id
    params["limit"] = 10000

    observations = fetch_paginated_data(url, params, "observations", pagination=True)

    current_dt = datetime.strptime(str(dates.get_date_today()), "%Y-%m-%d")
    fetch_dt = datetime.strptime(observations[-1]["date"], "%Y-%m-%d")

    # a safety check: check the latest record is at least from last month
    # (some monthly updated observations seems to be lagging for a few months)
    one_month_ago = current_dt - relativedelta(months=1)
    is_at_least_last_month = one_month_ago <= fetch_dt

    if is_at_least_last_month and len(observations) > 0:
        # save only when it's not lagging and there's data in it
        observations = [
            {
                "id": series_id,
                "date": observation["date"],
                "value": float(observation["value"]),
            }
            for observation in observations
            if observation["value"] != "."
        ]

        return observations

    return None


def combine_dicts(dict1, dict2):
    """Combine 2 dict."""
    combined_dict = {}

    # Add all keys from dict1 to combined_dict
    for key, value in dict1.items():
        if key not in combined_dict:
            combined_dict[key] = {}
        combined_dict[key].update(value)

    # Add all keys from dict2 to combined_dict, merging nested dictionaries
    for key, value in dict2.items():
        if key not in combined_dict:
            combined_dict[key] = {}
        combined_dict[key].update(value)

    return combined_dict


def fetch_all(dfq, FRED_QUESTIONS_NAMES):
    """
    Fetch and process all data for given FRED questions.

    Steps:
    1. Convert FRED_QUESTIONS_NAMES to a dictionary for easy access.
    2. Fetch release, series, and observations data for each series ID.
    3. Log the total number of questions.
    4. Iterate through the fetched data, process it, and prepare a list of dictionaries for each series.
    5. Return the processed data as a pandas DataFrame.

    Returns:
    - DataFrame: A DataFrame containing the processed data with detailed information for each series.
    """
    current_time = dates.get_date_today()
    yesterday = current_time - timedelta(days=1)

    # get the dict version of FRED_QUESTIONS_NAMES for easy acceess
    fred_questions = {q["id"]: q for q in FRED_QUESTIONS_NAMES}

    # get current series ids that are not in newly fetched set
    dfq_dict = dfq.to_dict(orient="records")
    questions_bank_dict = {q["id"]: q for q in dfq_dict}
    questions_bank_dict = {
        id: questions_bank_dict[id] for id in questions_bank_dict if id not in fred_questions
    }

    logger.info(f"# of questions in the new list: {len(fred_questions.keys())}")
    logger.info(
        f"# of questions in the bank but not in the new list: {len(questions_bank_dict.keys())}"
    )

    combined_questions = combine_dicts(fred_questions, questions_bank_dict)

    logger.info(f"# of combined questions: {len(combined_questions.keys())}")
    ids_to_delete = []
    # fetch release, series, and background
    for series_id in combined_questions:
        combined_questions[series_id]["release"] = fetch_all_releases(
            PARAMS, series_id=series_id, single=True
        )[0]
        combined_questions[series_id]["series"] = fetch_all_series(PARAMS, series_id=series_id)
        combined_questions[series_id]["observations"] = fetch_all_observations(
            PARAMS, series_id=series_id
        )
        if not combined_questions[series_id]["observations"]:
            ids_to_delete.append(series_id)
        else:
            # fill in missing dates
            final_resolutions_df = pd.DataFrame(combined_questions[series_id]["observations"])
            final_resolutions_df["date"] = pd.to_datetime(final_resolutions_df["date"].str[:10])

            # Sort and remove duplicates to keep the latest entry per date
            final_resolutions_df.drop_duplicates(subset=["date"], keep="last", inplace=True)

            # Reindex to fill in missing dates including weekends
            all_dates = pd.date_range(
                start=final_resolutions_df["date"].min(), end=yesterday, freq="D"
            )
            final_resolutions_df = (
                final_resolutions_df.set_index("date")
                .reindex(all_dates, method="ffill")
                .reset_index()
            )

            final_resolutions_df.rename(columns={"index": "date"}, inplace=True)
            final_resolutions_df["date"] = final_resolutions_df["date"].dt.strftime("%Y-%m-%d")

            final_resolutions_df = final_resolutions_df[["id", "date", "value"]]
            combined_questions[series_id]["observations"] = final_resolutions_df.to_dict(
                orient="records"
            )

    logger.info(f"questions-to-delete cnt because no observations fetched: {len(ids_to_delete)}")

    for id in ids_to_delete:
        del combined_questions[id]

    series_list = []

    for series_id in tqdm(combined_questions.keys(), desc="Saving fetched FRED data"):
        observations = combined_questions[series_id]["observations"]
        current_value = observations[-1]["value"]

        # check if this is only from the "new" FRED Questions in fred.py
        # If the questions in fred.py are updated at some point in the future,
        # and the questions that exist in the old fred.py but not exist in new fred.py
        # will not have a question_name field (because they only have all fields defined
        # in fred_questions.json
        question = None
        if "series_name" in combined_questions[series_id]:
            series_name = combined_questions[series_id]["series_name"]
            question = (
                f"Will {series_name} have increased by the resolution date, "
                "as compared to its value at the forecast due date?"
            )
        else:
            question = combined_questions[series_id]["question"]

        release = combined_questions[series_id]["release"]
        series = combined_questions[series_id]["series"][0]
        series_list.append(
            {
                "id": series_id,
                "question": question,
                "background": (
                    f"The notes from the release: {release.get('notes', 'N/A')}. "
                    f" The notes from the series: {series.get('notes', 'N/A')}. "
                    " Additional background of the series: "
                    f" 1. the units of the series: {series.get('units', 'N/A')}. "
                    " 2. the seasonal adjustments of the series: "
                    f" {series.get('seasonal_adjustment', 'N/A')} "
                    f" 3. the update frequency: {series.get('frequency', 'N/A')} "
                ),
                "market_info_resolution_criteria": "N/A",
                "market_info_open_datetime": "N/A",
                "market_info_close_datetime": "N/A",
                "url": f"https://fred.stlouisfed.org/series/{series_id}",
                "resolved": False,
                "market_info_resolution_datetime": "N/A",
                "fetch_datetime": dates.get_datetime_now(),
                "probability": current_value,
                "forecast_horizons": (
                    constants.FORECAST_HORIZONS_IN_DAYS
                    if series["frequency_short"] != "M"
                    else constants.FORECAST_HORIZONS_IN_DAYS[1:]
                ),
                "freeze_datetime_value": current_value,
                "freeze_datetime_value_explanation": (
                    "The latest value released in "
                    f"{series['title']} from the "
                    f"release {release['name']}."
                ),
                "resolutions": observations,
            }
        )
    logger.info(f"Final questions count: {len(series_list)}")

    return pd.DataFrame(series_list)


@decorator.log_runtime
def driver(_):
    """Fetch all series from FRED and then upload to gcp."""
    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    # get the FRED_QUESTIONS_NAMES provided by @zach
    FRED_QUESTIONS_NAMES = fred.fred_questions

    all_questions = fetch_all(dfq, FRED_QUESTIONS_NAMES)
    filenames = data_utils.generate_filenames(SOURCE)

    # Save and upload
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in all_questions.to_dict("records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")

    logger.info("Uploading to GCP...")
    # Upload
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
