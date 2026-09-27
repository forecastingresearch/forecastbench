"""Explode a question set into one row per (question x resolution_date x direction)."""

from __future__ import annotations

import itertools
import logging

import pandas as pd

from helpers import dates
from sources import MARKET_SOURCE_NAMES

logger = logging.getLogger(__name__)


def get_resolution_dates(question_set_df: pd.DataFrame) -> list[str]:
    """Return the sorted union of resolution dates asked in a question set.

    Args:
        question_set_df: DataFrame with a `resolution_dates` column (list of ISO dates or "N/A").

    Returns:
        Sorted ISO date strings.
    """
    resolution_dates = set()
    for question_resolution_dates in question_set_df["resolution_dates"]:
        if question_resolution_dates != "N/A" and isinstance(question_resolution_dates, list):
            resolution_dates.update(question_resolution_dates)
    return sorted(resolution_dates)


def explode_question_set(question_set_df: pd.DataFrame, forecast_due_date: str) -> pd.DataFrame:
    """Explode a question set DataFrame into resolvable rows.

    Args:
        question_set_df: DataFrame with columns [id, source, resolution_dates].
        forecast_due_date: ISO date string (YYYY-MM-DD).

    Returns:
        Exploded DataFrame with columns [id, source, direction, forecast_due_date, resolution_date].
    """
    df = question_set_df[["id", "source", "resolution_dates"]].copy()
    logger.info(f"LLM question set starting with {len(df):,} questions.")

    df["forecast_due_date"] = pd.to_datetime(forecast_due_date)

    all_resolution_dates = get_resolution_dates(df)

    # Market questions get all resolution dates
    df["resolution_dates"] = df.apply(
        lambda x: (
            all_resolution_dates if x["source"] in MARKET_SOURCE_NAMES else x["resolution_dates"]
        ),
        axis=1,
    )

    # Explode resolution dates
    df = df.explode("resolution_dates", ignore_index=True)
    df.rename(columns={"resolution_dates": "resolution_date"}, inplace=True)
    df["resolution_date"] = pd.to_datetime(df["resolution_date"]).dt.date
    df = df[df["resolution_date"] < dates.get_date_today()]

    # Expand combo question directions
    df["direction"] = df.apply(
        lambda x: (
            list(itertools.product((1, -1), repeat=len(x["id"])))
            if isinstance(x["id"], tuple)
            else [()]
        ),
        axis=1,
    )
    df = df.explode("direction", ignore_index=True)
    df = df.sort_values(by=["source", "resolution_date"], ignore_index=True)

    # Convert resolution_date to datetime for downstream merging
    df["resolution_date"] = pd.to_datetime(df["resolution_date"])

    return df
