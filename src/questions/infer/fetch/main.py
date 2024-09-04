"""INFER fetch new questions script."""

import json
import logging
import os
import sys

import backoff
import certifi
import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import data_utils, dates, decorator, env, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"
INFER_URL = "https://www.infer-pub.com"


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=300,
    on_backoff=data_utils.print_error_info_handler,
)
def fetch_questions(potentially_closed_ids=None):
    """
    Fetch all questions from a specified API endpoint.

    Iterates over pages of questions from the given base URL, authenticating
    with the provided headers. Continues fetching until no more questions are
    available.

    Parameters:
    - potentially_closed_ids (dict): ids for questions that may or may not have been closed.

    Returns:
    - list: A list of all questions fetched from the API.
    """
    endpoint = INFER_URL + "/api/v1/questions"
    headers = {"Authorization": f"Bearer {keys.API_KEY_INFER}"}
    params = {
        "page": 0,
        "status": "active",
    }
    if potentially_closed_ids is not None:
        params.update(
            {
                "status": "all",
                "ids": ",".join(sorted(potentially_closed_ids)),
            }
        )

    questions = []
    seen_ids = set()
    while True:
        response = requests.get(endpoint, params=params, headers=headers, verify=certifi.where())
        if not response.ok:
            logger.error(f"Request to Infer questions endpoint failed with params: {params}")
            response.raise_for_status()

        new_questions = response.json().get("questions", [])
        if not new_questions:
            break

        for q in new_questions:
            if q["id"] not in seen_ids:
                questions.append(q)
                seen_ids.add(q["id"])

        params["page"] += 1

    return questions


def get_data(dfq):
    """
    Fetch and prepare question data for processing.

    This function performs several key operations:
    - Retrieves IDs of unresolved question and resolved questions without resolution files
        from the current data.
    - Fetches all active, unresolved, and binary questions using provided API endpoints.
    - Filters out duplicated questions.
    - Augments and restructures question data for further processing.

    Parameters:
    - dfq (list of dict): A list of dictionaries containing data on questions,
      where each dictionary represents a question and must have keys 'id' and 'resolved'.

    Returns:
    - DataFrame: Each row representing a question ready for processing.
      This includes a mix of newly fetched binary questions and existing unresolved questions,
      with additional metadata and reformatted fields for consistency.
    """
    resolved_ids = dfq[dfq["resolved"]]["id"].tolist() if not dfq.empty else []
    unresolved_ids = dfq[~dfq["resolved"]]["id"].tolist() if not dfq.empty else []
    logger.info(f"Number resolved_ids: {len(resolved_ids)}")
    logger.info(f"Number unresolved_ids: {len(unresolved_ids)}")

    files_in_storage = gcp.storage.list_with_prefix(
        bucket_name=env.QUESTION_BANK_BUCKET, prefix=SOURCE
    )

    resolved_ids_without_files_in_storage = [
        id for id in resolved_ids if f"{SOURCE}/{id}.jsonl" not in files_in_storage
    ]
    logger.info(f"resolved_ids_without_resolution_files: {resolved_ids_without_files_in_storage}")

    all_existing_ids_to_fetch = unresolved_ids + resolved_ids_without_files_in_storage
    all_existing_questions = (
        fetch_questions(potentially_closed_ids=all_existing_ids_to_fetch)
        if all_existing_ids_to_fetch
        else []
    )

    all_active_binary_questions = [
        q
        for q in fetch_questions()
        if q["state"] == "active" and q["type"] == "Forecast::YesNoQuestion"
    ]

    # Convert all_new_questions to a set of IDs for faster lookup
    all_active_binary_question_ids = set(q["id"] for q in all_active_binary_questions)

    # Filter out questions from all_existing_questions if their IDs are in all_new_questions_ids
    all_existing_questions = [
        q for q in all_existing_questions if q["id"] not in all_active_binary_question_ids
    ]

    all_questions_to_add = all_active_binary_questions + all_existing_questions

    logger.info(f"Number of questions fetched: {len(all_questions_to_add)}")
    current_time = dates.get_datetime_now()
    questions_to_add = []
    for q in all_questions_to_add:
        # There was a bug that pulled questions that we do not want to include in the question set.
        # This field nullifies those questions and ensures the questions will not be resolved, even though
        # they were included in the 2024-07-21 question set.
        nullify_question = q["type"] != "Forecast::YesNoQuestion"

        # We use 'scoring_end_time_str' to ensure the closure time reflects when forecasts were
        # actually scored. This is crucial because sometimes an administrator may resolve
        # questions after the actual resolution is happened.
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

        forecast_yes = "N/A"
        if len(q["answers"]) == 2 and not nullify_question:
            yes_index = 0 if q["answers"][0]["name"].lower() == "yes" else 1
            forecast_yes = q["answers"][yes_index]["probability"]

        questions_to_add.append(
            {
                "id": str(q["id"]),
                "question": q["name"],
                "background": q["description"],
                "market_info_resolution_criteria": (
                    " ".join([content["content"] for content in q["clarifications"]])
                    if q["clarifications"]
                    else "N/A"
                ),
                "market_info_open_datetime": scoring_start_time_str,
                "market_info_close_datetime": final_closed_at_str,
                "url": f"{INFER_URL}/questions/{q['id']}",
                "resolved": q.get("resolved?", False),
                "market_info_resolution_datetime": (
                    "N/A" if not q.get("resolved?", False) else final_resolved_str
                ),
                "fetch_datetime": current_time,
                "probability": forecast_yes,
                "forecast_horizons": "N/A",
                "freeze_datetime_value": forecast_yes,
                "freeze_datetime_value_explanation": "The crowd forecast.",
                "nullify_question": nullify_question,
            }
        )

    return pd.DataFrame(questions_to_add)


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
