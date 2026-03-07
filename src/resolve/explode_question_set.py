"""Explode a question set into one row per (question x resolution_date x direction).

Port of resolve_forecasts/main.py:get_resolutions_for_llm_question_set() lines 234-276.
"""

from __future__ import annotations

import itertools
import logging

import pandas as pd

from helpers_new import dates
from sources import MARKET_SOURCE_NAMES

logger = logging.getLogger(__name__)


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

    # Collect all resolution dates across all questions
    all_resolution_dates = set()
    for resolution_date in df["resolution_dates"]:
        if resolution_date != "N/A" and isinstance(resolution_date, list):
            all_resolution_dates.update(resolution_date)
    all_resolution_dates = sorted(all_resolution_dates)

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
