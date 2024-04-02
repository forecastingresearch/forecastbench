"""INFER script."""

import json
import logging
import os
import sys
from datetime import datetime

import certifi
import pandas as pd
import requests
from google.cloud import secretmanager

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import data_utils  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Project_ID = "fri-llm-benchmark"
SOURCE = "infer"
INFER_URL = "https://www.infer-pub.com"
BUCKET_NAME = os.environ.get("CLOUD_STORAGE_BUCKET")
JSON_MARKET_FILENAME = f"{SOURCE}_questions.json"
LOCAL_MARKET_FILENAME = f"/tmp/{JSON_MARKET_FILENAME}"
JSON_MARKET_VALUE_FILENAME = f"{SOURCE}_resolutions.json"
LOCAL_MARKET_VALUES_FILENAME = f"/tmp/{JSON_MARKET_VALUE_FILENAME}"


def get_secret(project_id, secret_name, version_id="latest"):
    """
    Retrieve the payload of a specified secret version from Secret Manager.

    Accesses the Google Cloud Secret Manager to fetch the payload of a secret version
    identified by `project_id`, `secret_name`, and `version_id`. Decodes the payload
    from bytes to a UTF-8 string and returns it.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def fetch_questions(base_url, headers):
    """
    Fetch all questions from a specified API endpoint.

    Iterates over pages of questions from the given base URL, authenticating
    with the provided headers. Continues fetching until no more questions are
    available.

    Parameters:
    - base_url (str): The base URL for the questions API endpoint.
    - headers (dict): Authentication headers for the API request.

    Returns:
    - list: A list of all questions fetched from the API.
    """
    all_questions = []
    page_count = 1
    while True:
        url = f"{base_url}?page={page_count}"
        response = requests.get(url, headers=headers, verify=certifi.where())
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
    - list of dict: A list of dictionaries, each representing a question ready for processing.
      This includes a mix of newly fetched binary questions and existing unresolved questions,
      with additional metadata and reformatted fields for consistency.
    """
    API_KEY_INFER = get_secret(Project_ID, "API_KEY_INFER")
    HEADERS = {"Authorization": f"Bearer {API_KEY_INFER}"}

    start_time = datetime.now()
    logger.info(f"Scraping start time: {start_time}")

    unresolved_ids = (
        current_data[~current_data["resolved"]]["id"].tolist() if not current_data.empty else []
    )
    logger.info(f"unresolved_ids: {unresolved_ids}")

    if not unresolved_ids:
        all_existing_unresolved_questions = fetch_questions(
            INFER_URL
            + "/api/v1/questions?status={'closed'}&ids={'"
            + ", ".join(unresolved_ids)
            + "'}",
            HEADERS,
        )
    else:
        all_existing_unresolved_questions = []

    all_active_questions = fetch_questions(INFER_URL + "/api/v1/questions", HEADERS)
    all_binary_questions = [
        q for q in all_active_questions if q["state"] == "active" and "Will " in q["name"]
    ]

    all_binary_questions_ids = [q["id"] for q in all_binary_questions]
    if all_existing_unresolved_questions:
        for q in all_existing_unresolved_questions:
            if q["id"] in all_binary_questions_ids:
                index_to_delete = all_existing_unresolved_questions.index(q)
                del all_existing_unresolved_questions[index_to_delete]

    all_questions_to_add = all_binary_questions + all_existing_unresolved_questions

    end_time = datetime.now()

    logger.info(f"Scraping end time: {end_time}")
    logger.info(f"Total scraping duration: {end_time - start_time}")
    logger.info(f"Number of questions fetched: {len(all_questions_to_add)}")

    for i in range(len(all_questions_to_add)):
        q = all_questions_to_add[i]
        # Check if 'closed_at', 'created_at', and 'resolved_at' are not None before calling strftime
        closed_at_str = q["closed_at"] if q["closed_at"] else None
        created_at_str = q["created_at"] if q["created_at"] else None
        resolved_at_str = q["resolved_at"] if q["resolved_at"] else None
        all_questions_to_add[i] = {
            "id": str(q["id"]),
            "question": q["name"],
            "background": q["description"],
            "source_resolution_criteria": "N/A",
            "begin_datetime": closed_at_str,
            "close_datetime": created_at_str,
            "url": f"{INFER_URL}/questions/{q['id']}",
            "resolved": q.get("resolved?", False),
            "resolution_datetime": resolved_at_str,
            "resolution_or_current_community_prediction": q["answers"][0]["probability"]
            if q["answers"]
            else None,
        }

    return all_questions_to_add


def update_questions(dfq, dfmv, all_questions_to_add):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dfmv (pd.DataFrame): DataFrame containing all historical community predictions.
    - all_questions_to_add (list of dict): List of dictionaries, each representing a
        question with updated data.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfmv for each question in all_questions_to_add.
    """
    current_time_stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for question in all_questions_to_add:
        # Prepare a new row as a dictionary for dfmv
        new_mv_row = pd.DataFrame(
            [
                {
                    "id": question["id"],
                    "datetime": current_time_stamp,
                    "value": question["resolution_or_current_community_prediction"],
                }
            ]
        )
        # Append new row to dfmv using pd.concat
        dfmv = pd.concat([dfmv, new_mv_row], ignore_index=True)

        # Check if the question exists in dfq
        if question["id"] in dfq["id"].values:
            # Case 1: Update existing question
            dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
            for key, value in question.items():
                dfq.at[dfq_index, key] = value
        else:
            # Case 2: Append new question
            new_q_row = pd.DataFrame([question])
            dfq = pd.concat([dfq, new_q_row], ignore_index=True)

    return dfq, dfmv


def driver(event, context):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq, dfmv = data_utils.get_stored_question_data(
        BUCKET_NAME,
        JSON_MARKET_FILENAME,
        LOCAL_MARKET_FILENAME,
        JSON_MARKET_VALUE_FILENAME,
        LOCAL_MARKET_VALUES_FILENAME,
    )

    # Get the latest data
    all_questions_to_add = get_data(dfq)

    # Update the existing questions
    dfq, dfmv = update_questions(dfq, dfmv, all_questions_to_add)

    # Save and upload
    with open(LOCAL_MARKET_FILENAME, "w", encoding="utf-8") as f:
        # can't use `dfq.to_json` because we don't want escape chars
        for record in dfq.to_dict(orient="records"):
            json_str = json.dumps(record, ensure_ascii=False)
            f.write(json_str + "\n")
    dfmv.to_json(LOCAL_MARKET_VALUES_FILENAME, orient="records", lines=True, date_format="iso")

    logger.info("Uploading to GCP...")
    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=LOCAL_MARKET_FILENAME,
    )
    gcp.storage.upload(
        bucket_name=BUCKET_NAME,
        local_filename=LOCAL_MARKET_VALUES_FILENAME,
    )
    logger.info("Done.")


if __name__ == "__main__":
    driver(None, None)
