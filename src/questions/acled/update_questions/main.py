"""Generate ACLED questions."""

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

source = "acled"
filenames = data_utils.generate_filenames(source=source)

ACLED_DEFINITION = (
    "The Armed Conflict Location & Event Data Project (ACLED) collects real-time data "
    "on the locations, dates, actors, fatalities, and types of all reported political "
    "violence and protest events around the world."
)


def _generate_forecast_questions(dfq, dff):
    countries = dff["country"].unique()
    event_types_acled = dff["event_type"].unique()
    event_types = list(event_types_acled) + ["fatalities"]

    logger.info(f"Found {len(countries)} countries.")
    logger.info(f"Found {len(event_types)} event_types.")

    common_question_fields = {
        "background": "N/A",
        "source_resolution_criteria": "N/A",
        "begin_datetime": "N/A",
        "close_datetime": "N/A",
        "url": "https://acleddata.com/",
        "resolution_datetime": "N/A",
        "resolved": False,
        "continual_resolution": True,
        "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
    }

    def _get_event_type_str(event_type):
        return event_type if event_type == "fatalities" else f"'{event_type}'"

    def _acled_intro(event_type):
        if event_type in event_types_acled:
            event_type_str = _get_event_type_str(event_type)
            return f"{ACLED_DEFINITION} {event_type_str} is a type of event determined by ACLED."
        return ACLED_DEFINITION

    def _create_question_0(country, event_type):
        intro = _acled_intro(event_type)
        event_type_str = _get_event_type_str(event_type)
        question = (
            f"{intro} According to ACLED, will there be more {event_type_str} in {country} for the 30 "
            f"days before resolution than the 30-day average of {event_type_str} over the past 360 days?"
        )
        return {
            "id": f"{country}.{event_type}.last30Days.gt.{event_type}.30DayAvgOverPast360Days",
            "question": question,
            "lhs_func": "_sum_over_last_30_days",
            "lhs_args": {
                "country": country,
                "col": event_type,
                "ref_date": "get_resolution_date()",
            },
            "comparison_operator": ">",
            "rhs_func": "_30_day_avg_over_past_360_days",
            "rhs_args": {
                "country": country,
                "col": event_type,
                "ref_date": "get_freeze_date()",
            },
            **common_question_fields,
        }

    def _create_question_1(country, event_type):
        intro = _acled_intro(event_type)
        event_type_str = _get_event_type_str(event_type)
        question = (
            f"{intro} According to ACLED, will there be more than ten times as many "
            f"{event_type_str} in {country} for the 30 days before resolution than one plus the "
            f"30-day average of {event_type_str} over the past 360 days?"
        )
        return {
            "id": (
                f"{country}.{event_type}.last30DaysTimes10.gt.{event_type}"
                ".30DayAvgOverPast360DaysPlus1"
            ),
            "question": question,
            "lhs_func": "_sum_over_last_30_days_times_10",
            "lhs_args": {
                "country": country,
                "col": event_type,
                "ref_date": "get_resolution_date()",
            },
            "comparison_operator": ">",
            "rhs_func": "_30_day_avg_over_past_360_days_plus_1",
            "rhs_args": {
                "country": country,
                "col": event_type,
                "ref_date": "get_freeze_date()",
            },
            **common_question_fields,
        }

    questions = []
    for country in countries:
        for event_type in event_types:
            questions.append(_create_question_0(country, event_type))
            questions.append(_create_question_1(country, event_type))

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
    dff = data_utils.download_and_read(
        filename=f"{source}/{filenames['jsonl_fetch']}",
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=constants.ACLED_FETCH_COLUMNS),
        dtype=constants.ACLED_FETCH_COLUMN_DTYPE,
    )

    dfq = data_utils.download_and_read(
        filename=filenames["jsonl_question"],
        local_filename=filenames["local_question"],
        df_tmp=pd.DataFrame(columns=constants.ACLED_QUESTION_FILE_COLUMNS),
        dtype=constants.ACLED_QUESTION_FILE_COLUMN_DTYPE,
    )

    # Update the existing questions
    dfq = _generate_forecast_questions(dfq, dff)
    logger.info(f"Found {len(dfq):,} questions.")

    # Save
    with open(filenames["local_question"], "w", encoding="utf-8") as f:
        for record in dfq.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    # Upload
    gcp.storage.upload(
        bucket_name=constants.BUCKET_NAME,
        local_filename=filenames["local_question"],
        destination_folder=source,
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
