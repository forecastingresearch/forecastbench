"""Generate DBnomics questions."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "dbnomics"
filenames = data_utils.generate_filenames(source=source)


def _initialize_question_table():
    # Define the column names and types using a dictionary
    columns = {
        "id": pd.StringDtype(),
        "question": pd.StringDtype(),
        "background": pd.StringDtype(),
        "source_resolution_criteria": pd.StringDtype(),
        "begin_datetime": pd.StringDtype(),
        "close_datetime": pd.StringDtype(),
        "url": pd.StringDtype(),
        "resolved": "bool",
        "resolution_datetime": pd.StringDtype(),
        "continual_resolution": "bool",
        "forecast_horizons": "object",
    }
    # Create an empty DataFrame with specified columns
    series = pd.DataFrame({key: pd.Series(dtype=type) for key, type in columns.items()})
    return series


def _construct_questions(seriesIds, dff):
    """Construction question and resolution tables."""
    # For each seriesIds, construct question data from request
    series = _initialize_question_table()
    for row in seriesIds.itertuples():
        question_data = dff
        id = row.id
        provider_name = question_data["provider_name"][0]
        dataset_name = question_data["dataset_name"][0]
        series_name = question_data["series_name"][0]
        question = f"The dataseries {id} is provided by {provider_name}. The dataset's name is '{dataset_name}', and the series' name is '{series_name}'. What is the probability that the value of this series be higher at resolution than at the freeze datetime (time of last recorded value)?"
        url = f"https://db.nomics.world/{id}"
        background = f"The history of {dataset_name} - {series_name} from {provider_name} is available at {url}."
        source_resolution_criteria = "N/A"
        source_begin_datetime = "N/A"
        source_close_datetime = "N/A"
        source_resolution_datetime = "N/A"
        resolved = False
        continual_resolution = True
        forecast_horizons = row.forecast_horizons
        values = question_data["value"]
        highest_non_na_index = 0
        for i in range(len(values) - 1, -1, -1):
            if values[i] != "NA":
                highest_non_na_index = i
                break
        value_at_freeze_datetime = values[highest_non_na_index]
        value_at_freeze_datetime_explanation = "The timeseries value."
        new_row = {
            "id": id,
            "question": question,
            "background": background,
            "source_resolution_criteria": source_resolution_criteria,
            "source_begin_datetime": source_begin_datetime,
            "source_close_datetime": source_close_datetime,
            "url": url,
            "source_resolution_datetime": source_resolution_datetime,
            "resolved": resolved,
            "continual_resolution": continual_resolution,
            "forecast_horizons": forecast_horizons,
            "value_at_freeze_datetime": value_at_freeze_datetime,
            "value_at_freeze_datetime_explanation": value_at_freeze_datetime_explanation,
        }
        new_row = pd.DataFrame(new_row, index=[0])
        series = series._append(new_row, ignore_index=True)
    return series


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    logger.info("Downloading previously-fetched DBnomics data from Cloud.")
    dff = data_utils.download_and_read(
        filename=filenames["jsonl_fetch"],
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=constants.DBNOMICS_FETCH_COLUMNS_FETCH_COLUMNS),
        dtype=constants.DBNOMICS_FETCH_COLUMN_DTYPE,
    )

    # Update questions file
    seriesIds = constants.DBNOMICS_DATA
    dfq = _construct_questions(seriesIds=seriesIds, dff=dff)

    logger.info(f"Found {len(seriesIds):,} questions.")

    # Save
    with open(filenames["local_question"], "w", encoding="utf-8") as f:
        for record in dfq.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME, local_filename=filenames["local_question"],
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
