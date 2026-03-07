"""Prepare and validate forecast files for resolution.

Port of resolve_forecasts/main.py lines 497-596 and 421-462.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd
from pandas._libs.tslibs.np_datetime import OutOfBoundsDatetime

from helpers_new import constants, dates
from sources import DATA_SOURCE_NAMES, MARKET_SOURCE_NAMES
from sources._base import BaseSource

logger = logging.getLogger(__name__)

_VALID_FORECAST_KEYS = [
    "id",
    "source",
    "direction",
    "forecast",
    "resolution_date",
    "reasoning",
]

_ALL_SOURCE_NAMES = MARKET_SOURCE_NAMES + DATA_SOURCE_NAMES


def convert_and_bound_dates(date_str):
    """Safely convert dates to datetime, setting max date for dates that are too large."""
    try:
        return pd.to_datetime(date_str)
    except (OutOfBoundsDatetime, OverflowError):
        return pd.Timestamp("2262-04-11")


def check_and_prepare_forecast_file(
    df: pd.DataFrame,
    forecast_due_date: str,
    organization: str,
) -> pd.DataFrame:
    """Check and prepare the organization's forecast file.

    Args:
        df: Organization's forecasts DataFrame.
        forecast_due_date: Date as YYYY-MM-DD.
        organization: The organization that created the forecasts.

    Returns:
        Validated DataFrame ready for resolution.
    """
    df = df.drop(columns=[col for col in df.columns if col not in _VALID_FORECAST_KEYS])
    if "reasoning" in df.columns:
        df = df.drop(columns=["reasoning"])

    # Drop invalid sources
    df_len = len(df)
    df["source"] = df["source"].str.lower()
    df = df[df["source"].isin(_ALL_SOURCE_NAMES)]
    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len - len(df)} rows because of "
            "invalid data sources."
        )

    # Drop invalid forecasts
    df_len = len(df)
    df = df[~df["forecast"].isna()]
    df = df[(df["forecast"] >= 0) & (df["forecast"] <= 1)]
    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len - len(df)} rows because of "
            "invalid forecasts."
        )

    # Drop invalid resolution dates for dataset questions
    df_len = len(df)
    forecast_due_date_date = dates.convert_iso_str_to_date(forecast_due_date)
    valid_resolution_dates = [
        (forecast_due_date_date + timedelta(days=horizon)).strftime("%Y-%m-%d")
        for horizon in constants.FORECAST_HORIZONS_IN_DAYS
    ]
    df["resolution_date"] = df["resolution_date"].str.slice(0, 10)
    df = df[
        df["source"].isin(MARKET_SOURCE_NAMES)
        | (
            (df["source"].isin(DATA_SOURCE_NAMES))
            & (df["resolution_date"].isin(valid_resolution_dates))
        )
    ]
    df["resolution_date"] = df["resolution_date"].apply(convert_and_bound_dates)
    if df_len != len(df):
        logger.warning(
            f"Preparing {organization} dataframe: Dropped {df_len - len(df)} rows because of "
            "invalid dates."
        )

    # Add forecast due date
    df["forecast_due_date"] = pd.to_datetime(forecast_due_date)

    # Make columns hashable
    df = BaseSource._make_columns_hashable(df)

    # Ensure no duplicate forecasts for dataset questions
    df_data = df[df["source"].isin(DATA_SOURCE_NAMES)]
    df_tmp = df_data.drop_duplicates(
        subset=["id", "source", "resolution_date", "direction"], keep="first", ignore_index=True
    )
    if len(df_tmp) != len(df_data):
        dropped_rows = (
            df_data.merge(
                df_tmp,
                on=["id", "source", "resolution_date", "direction"],
                how="left",
                indicator=True,
            )
            .query('_merge == "left_only"')
            .drop("_merge", axis=1)
        )
        print(dropped_rows)
        msg = f"Duplicate Rows encountered in {organization} forecast file."
        logger.error(msg)
        raise ValueError(msg)

    return df


def set_resolution_dates(df: pd.DataFrame, df_question_resolutions: pd.DataFrame) -> pd.DataFrame:
    """Set resolution dates by merging forecast file with resolved question set.

    Args:
        df: Prepared forecast DataFrame.
        df_question_resolutions: Resolved question set DataFrame.

    Returns:
        Merged DataFrame with resolution dates set.
    """
    logger.info("Setting resolution dates.")

    df_market_sources = df[df["source"].isin(MARKET_SOURCE_NAMES)].copy()
    df_data_sources = df[df["source"].isin(DATA_SOURCE_NAMES)].copy()

    # Market questions: drop resolution_date, join on existing resolution dates
    df_market_sources = df_market_sources.drop(
        columns=["resolution_date"] if "resolution_date" in df_market_sources.columns else []
    )
    df_market_sources = pd.merge(
        df_question_resolutions[df_question_resolutions["source"].isin(MARKET_SOURCE_NAMES)],
        df_market_sources,
        how="left",
        on=["id", "source", "direction", "forecast_due_date"],
    )

    # Data questions: match on resolution_date too
    df_data_sources = pd.merge(
        df_question_resolutions[df_question_resolutions["source"].isin(DATA_SOURCE_NAMES)],
        df_data_sources,
        how="left",
        on=["id", "source", "direction", "forecast_due_date", "resolution_date"],
    )

    df = pd.concat([df_market_sources, df_data_sources], ignore_index=True)
    return df
