"""Resolve ACLED questions."""

import logging
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import constants, data_utils, decorator  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source = "acled"
filenames = data_utils.generate_filenames(source=source)


def get_resolution_date():
    """Return current resolution date."""
    return datetime.strptime("2024-04-14", "%Y-%m-%d")


def get_freeze_date():
    """Return the question freeze date."""
    return datetime.strptime("2024-04-01", "%Y-%m-%d")


def _sum_over_last_30_days(dff, country, col, ref_date):
    """Sum over the 30 days before the ref_date."""
    dfc = dff[dff["country"] == country].copy()
    if dfc.empty:
        return np.nan
    start_date = ref_date - timedelta(days=30)
    dfc = dfc[(dfc["event_date"] > start_date) & (dfc["event_date"] <= ref_date)]
    return dfc[col].sum()


def _30_day_avg_over_past_360_days(dff, country, col, ref_date):
    """Get the 30 day average over the 360 days before the ref_date."""
    dfc = dff[dff["country"] == country].copy()
    if dfc.empty:
        return np.nan
    start_date = ref_date - timedelta(days=360)
    dfc = dfc[(dfc["event_date"] > start_date) & (dfc["event_date"] <= ref_date)]
    dfc.set_index("event_date", inplace=True)
    dfc_30d = dfc[col].resample("30D").mean()
    return dfc_30d.mean()


def _30_day_avg_over_past_360_days_plus_1(dff, country, col, ref_date):
    return 1 + _30_day_avg_over_past_360_days(dff, country, col, ref_date)


def _sum_over_last_30_days_times_10(dff, country, col, ref_date):
    return 10 * _sum_over_last_30_days(dff, country, col, ref_date)


def resolve_hand_side(hand_side, forecast_question, dff):
    """Resolve the left hand side or right hand side of the resolution comparison."""
    hs = forecast_question[f"{hand_side}_func"]
    key = f"{hand_side}_args"
    if (
        forecast_question[key].get("ref_date", "") == "get_resolution_date()"
        or forecast_question[key].get("ref_date", "") == "get_freeze_date()"
    ):
        forecast_question[key]["ref_date"] = eval(forecast_question[key]["ref_date"])

    hs_args = ", ".join(
        f"{key}='{value}'"
        for key, value in forecast_question["lhs_args"].items()
        if key != "ref_date"
    )

    hs = f"{hs}({hs_args}, dff=dff, ref_date=forecast_question[key]['ref_date'])"
    return eval(hs)


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


@decorator.log_runtime
def driver(_):
    """Resolve an ACLED forecast question.

    This is test function, to be integrated when we're ready to resolve questions.
    """
    return "OK", 200


if __name__ == "__main__":
    logger.info("Downloading previously-fetched ACLED data from Cloud.")
    file_name = "dff.pkl"
    if os.path.exists(file_name):
        dff = pd.read_pickle(file_name)
    else:
        dff = data_utils.download_and_read(
            filename=f"{source}/{filenames['jsonl_fetch']}",
            local_filename=filenames["local_fetch"],
            df_tmp=pd.DataFrame(columns=constants.ACLED_FETCH_COLUMNS),
            dtype=constants.ACLED_FETCH_COLUMN_DTYPE,
        )
        dff.to_pickle(file_name)

    forecast_question_0 = {
        "id": "Aruba.Protests.last30Days.gt.Protests.30DayAvgOverPast360Days",
        "question": (
            "The Armed Conflict Location & Event Data Project (ACLED) collects real-time "
            "data on the locations, dates, actors, fatalities, and types of all reported "
            "political violence and protest events around the world. 'Protests' is a type "
            "of event determined by ACLED. According to ACLED, will there be more "
            "'Protests' in Aruba for the 30 days before resolution than the 30-day average"
            " of 'Protests' over the past 360 days?"
        ),
        "lhs_func": "_sum_over_last_30_days",
        "lhs_args": {"country": "Aruba", "col": "Protests", "ref_date": "get_resolution_date()"},
        "comparison_operator": ">",
        "rhs_func": "_30_day_avg_over_past_360_days",
        "rhs_args": {"country": "Aruba", "col": "Protests", "ref_date": "get_freeze_date()"},
        "background": "N/A",
        "source_resolution_criteria": "N/A",
        "begin_datetime": "N/A",
        "close_datetime": "N/A",
        "url": "https://acleddata.com/",
        "resolution_datetime": "N/A",
        "resolved": False,
        "continual_resolution": True,
        "forecast_horizons": [7, 30, 90, 180, 365, 1095, 1825, 3650],
    }

    forecast_question_1 = {
        "id": "Cambodia.Riots.last30DaysTimes10.gt.Riots.30DayAvgOverPast360DaysPlus1",
        "question": (
            "The Armed Conflict Location & Event Data Project (ACLED) collects real-time "
            "data on the locations, dates, actors, fatalities, and types of all reported "
            "political violence and protest events around the world. 'Riots' is a type of"
            " event determined by ACLED. According to ACLED, will there be more than ten "
            "times as many 'Riots' in Cambodia for the 30 days before resolution than "
            "one plus the 30-day average of 'Riots' over the past 360 days?"
        ),
        "lhs_func": "_sum_over_last_30_days_times_10",
        "lhs_args": {"country": "Cambodia", "col": "Riots", "ref_date": "get_resolution_date()"},
        "comparison_operator": ">",
        "rhs_func": "_30_day_avg_over_past_360_days_plus_1",
        "rhs_args": {"country": "Cambodia", "col": "Riots", "ref_date": "get_freeze_date()"},
        "background": "N/A",
        "source_resolution_criteria": "N/A",
        "begin_datetime": "N/A",
        "close_datetime": "N/A",
        "url": "https://acleddata.com/",
        "resolution_datetime": "N/A",
        "resolved": False,
        "continual_resolution": True,
        "forecast_horizons": [7, 30, 90, 180, 365, 1095, 1825, 3650],
    }

    forecast_question = forecast_question_0

    dff = _prep_dff(dff)
    lhs = resolve_hand_side(
        hand_side="lhs",
        forecast_question=forecast_question,
        dff=dff,
    )
    print(f"LHS: {lhs}")

    rhs = resolve_hand_side(
        hand_side="rhs",
        forecast_question=forecast_question,
        dff=dff,
    )
    print(f"RHS: {rhs}")

    operator = forecast_question["comparison_operator"]
    print(f"{lhs}{operator}{rhs}")
    print(eval(f"{lhs}{operator}{rhs}"))
