"""INFER update question script."""

import logging
import os
import sys
import time

import pandas as pd
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from helpers import constants, data_utils, dates, decorator, keys  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"


def get_historical_forecasts(current_df, id):
    """
    Fetch historical forecasts from a specified API endpoint and integrate them with a given DataFrame.

    This function retrieves all forecast records for a specific question identified by 'id' from the
    API until it reaches a record before the latest date in 'current_df'. It processes these forecasts
    and merges them with the existing DataFrame, handling cases where the DataFrame is initially empty.

    Parameters:
        current_df (pd.DataFrame): The existing DataFrame containing previous forecast data.
        id (int): The unique identifier for the forecast question.

    Returns:
        pd.DataFrame: A DataFrame containing combined old and new forecast data, sorted.
    """
    API_KEY_INFER = keys.get_secret("API_KEY_INFER")
    params = {"question_id": id}
    headers = {"Authorization": f"Bearer {API_KEY_INFER}"}
    endpoint = "https://www.infer-pub.com/api/v1/prediction_sets"
    all_responses = []
    page_count = 1
    last_date = None

    # Check if 'current_df' is not empty and contains the 'datetime' column
    if not current_df.empty and "datetime" in current_df.columns:
        last_date = pd.to_datetime(current_df["datetime"].iloc[-1]).tz_localize("UTC")

    while True:
        try:
            logger.info(f"Fetched page: {page_count}, for question ID: {id}")
            url = f"{endpoint}?page={page_count}"
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            new_responses = response.json().get("prediction_sets", [])
            all_responses.extend(new_responses)
            if not new_responses or (
                last_date and pd.to_datetime(new_responses[-1]["created_at"], utc=True) <= last_date
            ):
                break

            page_count += 1
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("Rate limit reached, waiting 10s before retrying...")
                time.sleep(10)  # Wait longer if rate limited
            else:
                raise  # Re-raise the exception if it's not a rate limit issue

    all_forecasts = []

    for forecast in all_responses:
        if current_df.empty or (
            last_date and pd.to_datetime(forecast["created_at"], utc=True) > last_date
        ):
            if len(forecast["predictions"]) == 2:
                forecast_yes = forecast["predictions"][0]
                if forecast_yes["answer_name"] == "No":
                    forecast_yes = forecast["predictions"][1]
            elif len(forecast["predictions"]) == 1:
                forecast_yes = forecast["predictions"][0]

            all_forecasts.append(
                (
                    dates.convert_zulu_to_iso(forecast["created_at"]),
                    forecast_yes["final_probability"],
                )
            )

    df = pd.DataFrame(all_forecasts, columns=["datetime", "value"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["id"] = id

    # Sort by datetime first
    df_sorted = df.sort_values("datetime")

    # Reset index after sorting
    df_sorted.reset_index(drop=True, inplace=True)

    # Convert datetime to date only after sorting
    df_sorted["datetime"] = df_sorted["datetime"].dt.date
    df_final = df_sorted[["id", "datetime", "value"]]

    # Check if the existing dataframe is empty
    if current_df.empty:
        # Directly return if there's no existing data to merge
        result_df = df_final.drop_duplicates(subset=["id", "datetime"], keep="last")
    else:
        # Process current dataframe similarly
        current_df["datetime"] = pd.to_datetime(current_df["datetime"]).dt.date
        current_df_final = current_df[["id", "datetime", "value"]]
        # Concatenate and remove duplicates
        result_df = (
            pd.concat([current_df_final, df_final], axis=0)
            .sort_values(by=["datetime"], ascending=True)  # Ensure sorting by date for consistency
            .drop_duplicates(
                subset=["id", "datetime"], keep="last"
            )  # Keep the last entry for each id-date combination
            .reset_index(drop=True)
        )

    result_df.set_index("datetime", inplace=True)
    date_range = pd.date_range(start=result_df.index.min(), end=result_df.index.max())
    result_df = result_df.reindex(date_range)
    result_df["value"] = result_df["value"].ffill()
    result_df["id"] = id
    result_df.reset_index(inplace=True)
    result_df.rename(columns={"index": "datetime"}, inplace=True)

    return result_df


def create_resolution_file(
    question, resolved, get_historical_forecasts_func=get_historical_forecasts, source=SOURCE
):
    """
    Create or update a resolution file based on the question ID provided. Download the existing file, if any.

    Check the last entry date, and update with new data if there's no entry for today. Upload the updated file
    back to the specified Google Cloud Platform bucket.

    Args:
    - question (dict): A dictionary containing at least the 'id' of the question.
    - get_historical_forecasts_func (function, optional): A function to retrieve historical forecasts.
      Defaults to `get_historical_forecasts`.
    - source (str): The source directory path within the bucket where files are stored.

    Returns:
    - DataFrame: Return the current state of the resolution file as a DataFrame if no update is needed.
      If an update occurs, the function returns None after uploading the updated file.
    """
    basename = f"{question['id']}.jsonl"
    remote_filename = f"{source}/{basename}"
    local_filename = "/tmp/tmp.jsonl"
    TODAY = pd.Timestamp(dates.get_datetime_today_midnight()).normalize()

    gcp.storage.download_no_error_message_on_404(
        bucket_name=constants.BUCKET_NAME,
        filename=remote_filename,
        local_filename=local_filename,
    )
    df = pd.read_json(
        local_filename,
        lines=True,
        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
        convert_dates=False,
    )

    if (not df.empty and pd.to_datetime(df["datetime"].iloc[-1]).tz_localize("UTC") >= TODAY) or (
        not df.empty and df["datetime"].iloc[-1][:10] == question["resolution_datetime"][:10]
    ):
        # Check last datetime to see if we've already gotten the resolution value for today
        # If we have, return to avoid unnecessary API calls
        return df

    df = get_historical_forecasts_func(df, question["id"])
    df.datetime = df.datetime.dt.date

    if resolved:
        df = df[df.datetime < pd.to_datetime(question["resolution_datetime"][:10]).date()]
        resolution_row = pd.DataFrame(
            {
                "id": [question["id"]],
                "datetime": [question["resolution_datetime"][:10]],
                "value": [question["probability"]],
            }
        )
        df = pd.concat([df, resolution_row], ignore_index=True)

    logger.info(df[["id", "datetime", "value"]])
    df = df[["id", "datetime", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME,
        local_filename=local_filename,
        filename=remote_filename,
    )


def update_questions(dfq, dff):
    """
    Update the dataframes with new or modified question data and new community predictions.

    Parameters:
    - dfq (pd.DataFrame): DataFrame containing all existing questions.
    - dfr (pd.DataFrame): DataFrame containing all historical community predictions.
    - dff  (pd.DataFrame): DataFrame containing all newly fetched questions.

    The function updates dfq by either replacing existing questions with new data or adding new questions.
    It also appends new community predictions to dfr for each question in all_questions_to_add.
    """
    dff_list = dff.to_dict("records")
    for question in dff_list:
        create_resolution_file(question, resolved=question["resolved"])

        del question["fetch_datetime"]
        del question["probability"]

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

    return dfq


@decorator.log_runtime
def driver(_):
    """Execute the main workflow of fetching, processing, and uploading questions."""
    # Download existing questions from cloud storage
    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    # Update the existing questions
    dfq = update_questions(dfq, dff)

    logger.info("Uploading to GCP...")
    # Save and upload
    data_utils.upload_questions(dfq, SOURCE)
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
