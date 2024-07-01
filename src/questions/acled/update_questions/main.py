"""Generate ACLED questions."""

import json
import logging
import os
import sys

import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import acled, constants, data_utils, dates, decorator, env  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "acled"
filenames = data_utils.generate_filenames(source=source)


def generate_forecast_questions(dfq, dfr, countries, event_types):
    """Generate forecast questions given fetch data."""
    logger.info(f"Found {len(countries)} countries.")
    logger.info(f"Found {len(event_types)} event_types.")

    TODAY = dates.get_date_today()

    def fill_template(template, fields, values):
        fill_values = {field: values[field] for field in fields}
        # Always maintain resolution_date and forecast_due_date when formatting the string
        default_values = {
            "resolution_date": "{resolution_date}",
            "forecast_due_date": "{forecast_due_date}",
        }
        combined_fill_values = {**default_values, **fill_values}
        return template.format(**combined_fill_values)

    def create_question(question_key, country, event_type, dfr):
        question, variables = acled.QUESTIONS.get(question_key).get("question")
        event_type_quoted = event_type if event_type == "fatalities" else f"'{event_type}'"
        question = fill_template(
            template=question,
            fields=variables,
            values={"event_type": event_type_quoted, "country": country},
        )
        aid = acled.id_hash(
            {"key": question_key, "event_type": event_type, "country": country},
        )
        freeze_datetime_value_explanation, variables = acled.QUESTIONS.get(question_key).get(
            "freeze_datetime_value_explanation"
        )
        freeze_datetime_value_explanation = fill_template(
            template=freeze_datetime_value_explanation,
            fields=variables,
            values={"event_type": event_type_quoted, "country": country},
        )
        freeze_datetime_value = acled.get_freeze_value(
            key=question_key, dfr=dfr, country=country, event_type=event_type, today=TODAY
        )
        return {
            "id": aid,
            "question": question,
            "background": acled.BACKGROUND,
            "freeze_datetime_value": freeze_datetime_value,
            "freeze_datetime_value_explanation": freeze_datetime_value_explanation,
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": "N/A",
            "market_info_close_datetime": "N/A",
            "market_info_resolution_datetime": "N/A",
            "url": "https://acleddata.com/",
            "resolved": False,
            "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
        }

    questions = []
    for country in tqdm(countries, "Creating questions"):
        for event_type in event_types:
            questions.append(
                create_question(
                    question_key="last30Days.gt.30DayAvgOverPast360Days",
                    country=country,
                    event_type=event_type,
                    dfr=dfr,
                )
            )
            questions.append(
                create_question(
                    question_key="last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1",
                    country=country,
                    event_type=event_type,
                    dfr=dfr,
                )
            )

    df = pd.DataFrame(questions)

    if dfq.empty:
        return df
    rows_to_append = df[~df["id"].isin(dfq["id"])]
    dfq = pd.concat([dfq, rows_to_append], ignore_index=True).sort_values(
        by="id", ignore_index=True
    )
    return dfq


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    # Download pertinent files from Cloud Storage
    logger.info("Downloading previously-fetched ACLED data from Cloud.")

    acled.populate_hash_mapping()
    dfr, countries, event_types = acled.download_dff_and_prepare_dfr()
    dfq = data_utils.get_data_from_cloud_storage(source="acled", return_question_data=True)

    # Update the existing questions
    dfq = generate_forecast_questions(dfq, dfr, countries, event_types)
    dfq = dfq[constants.QUESTION_FILE_COLUMNS]
    logger.info(f"Found {len(dfq):,} questions.")

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
    acled.upload_hash_mapping()

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
