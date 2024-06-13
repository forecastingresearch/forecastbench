"""ACLED resolution functions."""

import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import acled, data_utils, resolution  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "acled"


def make_resolution_df():
    """Prepare ACLED data for resolution."""
    filenames = data_utils.generate_filenames(source=SOURCE)
    df = data_utils.download_and_read(
        filename=filenames["jsonl_fetch"],
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=acled.FETCH_COLUMNS),
        dtype=acled.FETCH_COLUMN_DTYPE,
    )

    df = df[["country", "event_date", "event_type", "fatalities"]].copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    return (
        pd.get_dummies(df, columns=["event_type"], prefix="", prefix_sep="")
        .groupby(["country", "event_date"])
        .sum()
        .reset_index()
    )


def _sum_over_last_30_days(dfr, country, col, ref_date):
    """Sum over the 30 days before the ref_date."""
    dfc = dfr[dfr["country"] == country].copy()
    if dfc.empty:
        return 0

    start_date = ref_date - timedelta(days=29)  # 30 inlcusive
    dfc = dfc[(dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date <= ref_date)]
    return dfc[col].sum() if not dfc.empty else 0


def _sum_over_last_30_days_times_10(dfr, country, col, ref_date):
    """Multiply 10 to function it wraps."""
    return 10 * _sum_over_last_30_days(dfr, country, col, ref_date)


def _30_day_avg_over_past_360_days(dfr, country, col, ref_date):
    """Get the 30 day average over the 360 days before the ref_date."""
    dfc = dfr[dfr["country"] == country].copy()
    if dfc.empty:
        return 0

    start_date = ref_date - timedelta(days=359)  # 360 inclusive
    dfc = dfc[(dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date <= ref_date)]
    if dfc.empty:
        return 0

    dfc.set_index("event_date", inplace=True)
    all_dates = pd.date_range(start=start_date, end=ref_date, freq="D")
    dfc = dfc[col].reindex(all_dates, fill_value=0)
    dfc = dfc.resample("30D").mean()
    return dfc.mean()


def _30_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
    """Add 1 to function it wraps."""
    return 1 + _30_day_avg_over_past_360_days(dfr, country, col, ref_date)


def resolve_hand_side(hand_side, forecast_question, dfr, forecast_date_func, resolution_date_func):
    """Resolve the left hand side or right hand side of the resolution comparison."""
    hs = forecast_question[f"{hand_side}_func"]
    key_args = f"{hand_side}_args"
    if forecast_question[key_args].get("ref_date", "") == "get_resolution_date()":
        forecast_question[key_args]["ref_date"] = resolution_date_func()
    elif forecast_question[key_args].get("ref_date", "") == "get_freeze_date()":
        forecast_question[key_args]["ref_date"] = forecast_date_func()
    hs_args = ", ".join(
        f"{key}='{value}'"
        for key, value in forecast_question[key_args].items()
        if key != "ref_date"
    )

    hs = f"{hs}({hs_args}, dfr=dfr, ref_date=forecast_question[key_args]['ref_date'])"
    return eval(hs)


def resolve_eq(question, dfr, forecast_date_func, resolution_date_func):
    """Resolve acled questions."""
    lhs = resolve_hand_side(
        hand_side="lhs",
        forecast_question=question,
        dfr=dfr,
        forecast_date_func=forecast_date_func,
        resolution_date_func=resolution_date_func,
    )
    rhs = resolve_hand_side(
        hand_side="rhs",
        forecast_question=question,
        dfr=dfr,
        forecast_date_func=forecast_date_func,
        resolution_date_func=resolution_date_func,
    )
    operator = question["comparison_operator"]
    return int(eval(f"{lhs}{operator}{rhs}"))


def resolve_question(mid, forecast_submitted_date, forecast_evaluation_date, dfq, dfr):
    """Resolve an individual ACLED question."""
    question = resolution.get_question(dfq, mid)
    if question is None:
        logger.warn(f"ACLED: could NOT find {mid}")
        return np.nan

    def get_forecast_date():
        """Return the forecast date."""
        return forecast_submitted_date

    def get_resolution_date():
        """Return the forecast date."""
        return forecast_evaluation_date

    return resolve_eq(
        question=question,
        dfr=dfr,
        forecast_date_func=get_forecast_date,
        resolution_date_func=get_resolution_date,
    )


def resolve(df, dfq, dfr):
    """Resolve ACLED questions."""
    logger.info("Resolving ACLED questions.")
    max_date = dfr["event_date"].max()
    mask = (df["source"] == "acled") & (df["forecast_evaluation_date"] <= max_date)
    for index, row in df[mask].iterrows():
        forecast_submitted_date = row["forecast_submitted_date"].date()
        forecast_evaluation_date = row["forecast_evaluation_date"].date()
        if not resolution.is_combo(row):
            value = resolve_question(
                mid=row["id"],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
        else:
            value1 = resolve_question(
                mid=row["id"][0],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
            value2 = resolve_question(
                mid=row["id"][1],
                forecast_submitted_date=forecast_submitted_date,
                forecast_evaluation_date=forecast_evaluation_date,
                dfq=dfq,
                dfr=dfr,
            )
            value = resolution.combo_change_sign(
                value1, row["direction"][0]
            ) * resolution.combo_change_sign(value2, row["direction"][1])
        df.at[index, "resolved_to"] = value
    df.loc[mask, "resolved"] = True
    return df
