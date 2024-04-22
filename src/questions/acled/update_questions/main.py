"""Generate ACLED questions."""

import json
import logging
import os
import sys
from datetime import timedelta

import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, dates, decorator  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "acled"
filenames = data_utils.generate_filenames(source=source)


def _prep_dff(dff):
    """Modify dff for resolution."""
    dff = dff[["country", "event_date", "event_type", "fatalities"]].copy()
    dff["event_date"] = pd.to_datetime(dff["event_date"])
    return (
        pd.get_dummies(dff, columns=["event_type"], prefix="", prefix_sep="")
        .groupby(["country", "event_date"])
        .sum()
        .reset_index()
    )


def _30_day_avg_over_past_360_days(dfr, country, col, ref_date):
    """Get the 30 day average over the 360 days before the ref_date."""
    dfc = dfr[dfr["country"] == country].copy()
    if dfc.empty:
        return 0
    start_date = ref_date - timedelta(days=360)
    dfc = dfc[(dfc["event_date"].dt.date > start_date) & (dfc["event_date"].dt.date <= ref_date)]
    dfc.set_index("event_date", inplace=True)
    dfc_30d = dfc[col].resample("30D").mean()
    return dfc_30d.mean()


def _30_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
    return 1 + _30_day_avg_over_past_360_days(dfr, country, col, ref_date)


def _generate_forecast_questions(dfq, dff):
    countries = dff["country"].unique()
    event_types_acled = dff["event_type"].unique()
    event_types = list(event_types_acled) + ["fatalities"]

    logger.info(f"Found {len(countries)} countries.")
    logger.info(f"Found {len(event_types)} event_types.")

    TODAY = dates.get_date_today()

    common_question_fields = {
        "background": "N/A",
        "source_resolution_criteria": "N/A",
        "source_begin_datetime": "N/A",
        "source_close_datetime": "N/A",
        "url": "https://acleddata.com/",
        "source_resolution_datetime": "N/A",
        "resolved": False,
        "continual_resolution": True,
        "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
    }

    def _get_event_type_str(event_type):
        return event_type if event_type == "fatalities" else f"'{event_type}'"

    def _acled_event_type_explanation(event_type):
        if event_type in event_types_acled:
            event_type_str = _get_event_type_str(event_type)
            return f"{event_type_str} is a type of event determined by ACLED."
        return ""

    def _create_question_0(country, event_type, dfr):
        event_type_explanation = _acled_event_type_explanation(event_type)
        event_type_str = _get_event_type_str(event_type)
        question = (
            f"According to ACLED, will there be more {event_type_str} in {country} for the 30 "
            f"days before resolution than the 30-day average of {event_type_str} over the past 360 days?"
            + event_type_explanation
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
            "value_at_freeze_datetime": _30_day_avg_over_past_360_days(
                dfr, country, event_type, TODAY
            ),
            "value_at_freeze_datetime_explanation": (
                f"The 30-day average of {event_type_str} over the past 360 days in {country}. "
                "This reference value will potentially change as ACLED updates its dataset."
            ),
            **common_question_fields,
        }

    def _create_question_1(country, event_type, dfr):
        event_type_explanation = _acled_event_type_explanation(event_type)
        event_type_str = _get_event_type_str(event_type)
        question = (
            f"According to ACLED, will there be more than ten times as many {event_type_str} in "
            f"{country} for the 30 days before resolution than one plus the 30-day average of "
            f"{event_type_str} over the past 360 days?" + event_type_explanation
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
            "value_at_freeze_datetime": _30_day_avg_over_past_360_days_plus_1(
                dfr, country, event_type, TODAY
            ),
            "value_at_freeze_datetime_explanation": (
                f"One plus the 30-day average of {event_type_str} over the past 360 days "
                f"in {country}. "
                "This reference value will potentially change as ACLED updates its dataset."
            ),
            **common_question_fields,
        }

    questions = []
    dfr = _prep_dff(dff)
    for country in tqdm(countries, "Creating questions"):
        for event_type in event_types:
            questions.append(_create_question_0(country, event_type, dfr))
            questions.append(_create_question_1(country, event_type, dfr))

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
        filename=filenames["jsonl_fetch"],
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
    )

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
