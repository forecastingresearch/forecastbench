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
from helpers import constants, data_utils, dates, decorator, env, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"
INFER_URL = "https://www.infer-pub.com"


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
    - Retrieves IDs of unresolved question and resolved questions without resolution files
        from the current data.
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
    HEADERS = {"Authorization": f"Bearer {keys.API_KEY_INFER}"}

    unresolved_ids = (
        current_data[~current_data["resolved"]]["id"].tolist() if not current_data.empty else []
    )
    logger.info(f"unresolved_ids: {unresolved_ids}")

    resolved_ids = (
        current_data[current_data["resolved"]]["id"].tolist() if not current_data.empty else []
    )

    resolved_files = gcp.storage.list_with_prefix(
        bucket_name=env.QUESTION_BANK_BUCKET, prefix=SOURCE
    )

    resolved_ids_without_resolution_files = [
        id for id in resolved_ids if f"{SOURCE}/{id}.jsonl" not in resolved_files
    ]

    logger.info(f"resolved_ids_without_resolution_files: {resolved_ids_without_resolution_files}")

    all_existing_ids_to_fetch = unresolved_ids + resolved_ids_without_resolution_files

    if all_existing_ids_to_fetch:
        params = {"status": "closed", "ids": ", ".join(all_existing_ids_to_fetch)}
        all_existing_questions = fetch_questions(
            INFER_URL + "/api/v1/questions", params=params, headers=HEADERS
        )
    else:
        all_existing_questions = []

    all_active_questions = fetch_questions(
        INFER_URL + "/api/v1/questions", params=None, headers=HEADERS
    )
    all_binary_questions = [
        q
        for q in all_active_questions
        if q["state"] == "active"
        and (
            len(q["answers"]) == 2
            and {q["answers"][0]["name"], q["answers"][1]["name"]} == {"No", "Yes"}
        )
        or (len(q["answers"]) == 1 and q["answers"][0]["name"] == "Yes")
    ]

    # Convert all_new_questions to a set of IDs for faster lookup
    all_binary_questions_ids = set(q["id"] for q in all_binary_questions)

    # Filter out questions from all_existing_questions if their IDs are in all_new_questions_ids
    all_existing_questions = [
        q for q in all_existing_questions if q["id"] not in all_binary_questions_ids
    ]

    all_questions_to_add = all_binary_questions + all_existing_questions

    logger.info(f"Number of questions fetched: {len(all_questions_to_add)}")
    current_time = dates.get_datetime_now()
    for i in range(len(all_questions_to_add)):
        q = all_questions_to_add[i]
        # We use 'scoring_end_time_str' to ensure the closure time reflects when forecasts were
        # actually scored. This is crucial because sometimes an administrator may resolve
        # questions after the actuall resolution is happened.
        scoring_end_time_str = (
            dates.convert_datetime_str_to_iso_utc(q["scoring_end_time"])
            if q["scoring_end_time"]
            else "N/A"
        )
        ended_at_str = dates.convert_zulu_to_iso(q["ends_at"]) if q["ends_at"] else "N/A"
        final_closed_at_str = (
            "N/A"
            if scoring_end_time_str == "N/A" and ended_at_str == "N/A"
            else (
                ended_at_str
                if scoring_end_time_str == "N/A"
                else (
                    scoring_end_time_str
                    if ended_at_str == "N/A"
                    else min(scoring_end_time_str, ended_at_str)
                )
            )
        )

        scoring_start_time_str = (
            dates.convert_datetime_str_to_iso_utc(q["scoring_start_time"])
            if q["scoring_start_time"]
            else "N/A"
        )
        resolved_at_str = dates.convert_zulu_to_iso(q["resolved_at"]) if q["resolved_at"] else "N/A"
        final_resolved_str = (
            "N/A"
            if resolved_at_str == "N/A" and final_closed_at_str == "N/A"
            else (
                final_closed_at_str
                if resolved_at_str == "N/A"
                else (
                    resolved_at_str
                    if final_closed_at_str == "N/A"
                    else min(resolved_at_str, final_closed_at_str)
                )
            )
        )

        # get horizons
        forecast_horizons = constants.FORECAST_HORIZONS_IN_DAYS
        if q.get("resolved?", False):
            forecast_horizons = []
        elif final_closed_at_str != "N/A":
            forecast_horizons = data_utils.get_horizons(datetime.fromisoformat(final_closed_at_str))

        forecast_yes = "N/A"
        if len(q["answers"]) == 2:
            forecast_yes = q["answers"][0]
            if forecast_yes["name"] == "No":
                forecast_yes = q["answers"][1]
            forecast_yes = forecast_yes["probability"]
        elif len(q["answers"]) == 1:
            forecast_yes = q["answers"][0]["probability"]

        all_questions_to_add[i] = {
            "id": str(q["id"]),
            "question": q["name"],
            "background": q["description"],
            "source_resolution_criteria": (
                " ".join([content["content"] for content in q["clarifications"]])
                if q["clarifications"]
                else "N/A"
            ),
            "source_begin_datetime": scoring_start_time_str,
            "source_close_datetime": final_closed_at_str,
            "url": f"{INFER_URL}/questions/{q['id']}",
            "resolved": q.get("resolved?", False),
            "source_resolution_datetime": (
                "N/A" if not q.get("resolved?", False) else final_resolved_str
            ),
            "fetch_datetime": current_time,
            "probability": forecast_yes,
            "continual_resolution": False,
            "forecast_horizons": forecast_horizons,
            "value_at_freeze_datetime": forecast_yes,
            "value_at_freeze_datetime_explanation": "The aggregated community forecast.",
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
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_fetch"],
    )

    logger.info("Done.")
    return "OK", 200


if __name__ == "__main__":
    driver(None)
