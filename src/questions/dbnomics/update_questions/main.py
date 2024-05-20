"""Generate DBnomics questions."""

import json
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dbnomics, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "dbnomics"
filenames = data_utils.generate_filenames(source=source)

""" Some dataseries with regular updates have large numbers of NA values during
periods in which data is not being reported. observations_without_data is
designed to detect these quiet periods and exclude the series from being
formed into a question during them (since it's unclear if we'll be able to
resolve them and the freeze values become increasingly irrelevant). """
observations_without_data = 10


def create_resolution_file(id, df):
    """
    Create or update a resolution file for a given question.

    Args:
        id (str): Identifier for the question
        df (DataFrame): dataframe containing fetch information related to the question
    """
    basename = f"{id}.jsonl"
    remote_filename = f"{source}/{basename}"
    local_filename = "/tmp/tmp.jsonl"

    df = df[["id", "period", "value"]].rename(columns={"period": "date"})
    df = df.astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    df["value"] = df["value"].replace("NA", "N/A")

    df.to_json(local_filename, orient="records", lines=True, date_format="iso")
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local_filename,
        filename=remote_filename,
    )


def _construct_questions(dff, dfq):
    """Construct question and resolution tables."""
    # For each seriesIds, construct question data from request
    new_series = None
    for row in dbnomics.CONSTANTS:
        id = row["id"].replace("/", "_")
        create_resolution_file(id, df=dff[dff["id"] == id])
        provider_name = dff[dff["id"] == id]["provider_name"].iloc[0]
        dataset_name = dff[dff["id"] == id]["dataset_name"].iloc[0]
        series_name = dff[dff["id"] == id]["series_name"].iloc[0]
        question = row["question_text"]
        url = f"https://db.nomics.world/{id}"
        background = (
            f"The history of {dataset_name} - {series_name} from {provider_name} is available at "
            f"{url}."
        )
        freeze_datetime_value_explanation = row["freeze_datetime_value_explanation"]
        values = dff[dff["id"] == id]["value"]
        if (values.tail(observations_without_data) != "NA").any():
            new_row = {
                "id": id,
                "question": question,
                "background": background,
                "market_info_resolution_criteria": "N/A",
                "market_info_open_datetime": "N/A",
                "market_info_close_datetime": "N/A",
                "url": url,
                "market_info_resolution_datetime": "N/A",
                "resolved": False,
                "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                "freeze_datetime_value": float(values[values != "NA"].iloc[-1]),
                "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
            }
            new_row = pd.DataFrame([new_row])
            if id not in dfq["id"].tolist():
                new_series = (
                    new_row
                    if new_series is None
                    else pd.concat([new_series, new_row], ignore_index=True)
                )
            else:
                dfq.loc[dfq["id"] == id, "freeze_datetime_value"] = float(
                    values[values != "NA"].iloc[-1]
                )
    new_series = new_series if new_series is not None else pd.DataFrame()
    return new_series


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    logger.info("Downloading previously-fetched DBnomics data from Cloud.")
    dff = data_utils.download_and_read(
        filename=filenames["jsonl_fetch"],
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=constants.DBNOMICS_FETCH_COLUMNS),
        dtype=constants.DBNOMICS_FETCH_COLUMN_DTYPE,
    )
    dfq = data_utils.download_and_read(
        filename=filenames["jsonl_question"],
        local_filename=filenames["local_question"],
        df_tmp=pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS),
        dtype=constants.QUESTION_FILE_COLUMN_DTYPE,
    )

    # Update questions file
    new_series = _construct_questions(dff=dff, dfq=dfq)
    dfq = pd.concat([dfq, new_series])

    logger.info(f"Found {len(dfq):,} questions of {len(dbnomics.CONSTANTS):,} possible.")

    # Save
    with open(filenames["local_question"], "w", encoding="utf-8") as f:
        for record in dfq.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=filenames["local_question"],
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
