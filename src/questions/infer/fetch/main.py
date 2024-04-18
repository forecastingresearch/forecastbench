"""INFER fetch new questions script."""

import json
import logging
import os
import sys
from datetime import datetime

import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Project_ID = os.environ.get("CLOUD_PROJECT")
SOURCE = "infer"
INFER_URL = "https://www.infer-pub.com"
BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")


def fetch_questions(base_url, params, headers):
    """
    Fetch all questions from a specified API endpoint.

    Iterates over pages of questions from the given base URL, authenticating
    with the provided headers. Continues fetching until no more questions are
    available.

    Parameters:
    - base_url (str): The base URL for the questions API endpoint.
    - params (dict): parameters for the API request.
    - headers (dict): Authentication headers for the API request.

    Returns:
    - list: A list of all questions fetched from the API.
    """
    all_questions = []
    page_count = 1
    while True:
        url = f"{base_url}?page={page_count}"
        response = requests.get(url, params=params, headers=headers, verify=certifi.where())
        response.raise_for_status()  # Proper error handling
        new_questions = response.json().get("questions", [])
        if not new_questions:
            break
        all_questions.extend(new_questions)
        page_count += 1
    return all_questions


def get_data(current_data):
    """
    Fetch and prepare question data for processing.

    This function performs several key operations:
    - Retrieves unresolved question IDs from the current data.
    - Fetches all active, unresolved, and binary questions using provided API endpoints.
    - Filters out duplicated questions.
    - Augments and restructures question data for further processing.

    Parameters:
    - current_data (list of dict): A list of dictionaries containing data on questions,
      where each dictionary represents a question and must have keys 'id' and 'resolved'.

    Returns:
    - DataFrame: Each row representing a question ready for processing.
      This includes a mix of newly fetched binary questions and existing unresolved questions,
      with additional metadata and reformatted fields for consistency.
    """
    API_KEY_INFER = keys.get_secret(Project_ID, "API_KEY_INFER")
    HEADERS = {"Authorization": f"Bearer {API_KEY_INFER}"}

    start_time = datetime.now()
    logger.info(f"Scraping start time: {start_time}")

    unresolved_ids = (
        current_data[~current_data["resolved"]]["id"].tolist() if not current_data.empty else []
    )
    logger.info(f"unresolved_ids: {unresolved_ids}")

    if unresolved_ids:
        params = {"status": "closed", "ids": ", ".join(unresolved_ids)}
        all_existing_unresolved_questions = fetch_questions(
            INFER_URL + "/api/v1/questions", params=params, headers=HEADERS
        )
    else:
        all_existing_unresolved_questions = []

    all_active_questions = fetch_questions(
        INFER_URL + "/api/v1/questions", params=None, headers=HEADERS
    )
    all_binary_questions = [
        q
        for q in all_active_questions
        if q["state"] == "active"
        and len(q["answers"]) == 2
        and {q["answers"][0]["name"], q["answers"][1]["name"]} == {"No", "Yes"}
    ]

    # Convert all_new_questions to a set of IDs for faster lookup
    all_binary_questions_ids = set(q["id"] for q in all_binary_questions)

    # Filter out questions from all_existing_unresolved_questions if their IDs are in all_new_questions_ids
    all_existing_unresolved_questions = [
        q for q in all_existing_unresolved_questions if q["id"] not in all_binary_questions_ids
    ]

    all_questions_to_add = all_binary_questions + all_existing_unresolved_questions

    end_time = datetime.now()

    logger.info(f"Scraping end time: {end_time}")
    logger.info(f"Total scraping duration: {end_time - start_time}")
    logger.info(f"Number of questions fetched: {len(all_questions_to_add)}")
    current_time = dates.get_datetime_now()
    for i in range(len(all_questions_to_add)):
        q = all_questions_to_add[i]
        # Check if 'closed_at', 'created_at', and 'resolved_at' are not None before calling strftime
        closed_at_str = dates.convert_zulu_to_iso(q["closed_at"]) if q["closed_at"] else "N/A"
        ended_at_str = dates.convert_zulu_to_iso(q["ends_at"]) if q["ends_at"] else "N/A"
        final_closed_at_str = None
        if closed_at_str == "N/A" and ended_at_str == "N/A":
            final_closed_at_str = "N/A"
        elif closed_at_str == "N/A":
            final_closed_at_str = ended_at_str
        elif ended_at_str == "N/A":
            final_closed_at_str = closed_at_str
        else:
            final_closed_at_str = min(closed_at_str, ended_at_str)

        created_at_str = (
            dates.convert_zulu_to_iso(q["published_at"]) if q["published_at"] else "N/A"
        )
        resolved_at_str = dates.convert_zulu_to_iso(q["resolved_at"]) if q["resolved_at"] else "N/A"

        # calculate horizons
        if final_closed_at_str != "N/A":
            current_time_ = datetime.fromisoformat(current_time)
            close_time_ = datetime.fromisoformat(final_closed_at_str)
            days_until_close = (close_time_ - current_time_).days
            forecast_horizons = [
                h for h in constants.FORECAST_HORIZONS_IN_DAYS if h < days_until_close
            ]
            next_horizon_index = (
                constants.FORECAST_HORIZONS_IN_DAYS.index(forecast_horizons[-1]) + 1
                if forecast_horizons
                else 0
            )
            if next_horizon_index < len(constants.FORECAST_HORIZONS_IN_DAYS):
                forecast_horizons.append(constants.FORECAST_HORIZONS_IN_DAYS[next_horizon_index])
        else:
            forecast_horizons = constants.FORECAST_HORIZONS_IN_DAYS

        forecast_yes = "N/A"
        if q["answers"]:
            forecast_yes = q["answers"][0]
            if forecast_yes["name"] == "No":
                forecast_yes = q["answers"][1]
            forecast_yes = forecast_yes["probability"]

        all_questions_to_add[i] = {
            "id": str(q["id"]),
            "question": q["name"],
            "background": q["description"],
            "source_resolution_criteria": (
                " ".join([content["content"] for content in q["clarifications"]])
                if q["clarifications"]
                else "N/A"
            ),
            "begin_datetime": created_at_str,
            "close_datetime": final_closed_at_str,
            "url": f"{INFER_URL}/questions/{q['id']}",
            "resolved": q.get("resolved?", False),
            "resolution_datetime": resolved_at_str,
            "fetch_datetime": current_time,
            "probability": forecast_yes,
            "continual_resolution": False,
            "forecast_horizons": forecast_horizons,
        }

    return pd.DataFrame(all_questions_to_add)


@decorator.log_runtime
def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    filenames = data_utils.generate_filenames(SOURCE)

    # Get the latest data
    all_questions_to_add = get_data(dfq)

    # Save and upload
    with open(filenames["local_fetch"], "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in all_questions_to_add.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")

    # Upload
    logger.info("Uploading to GCP...")
    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")
    return "OK", 200


if __name__ == "__main__":
    driver(None)
