"""Resolve all questions across sources."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from sources import DATASET_SOURCE_NAMES

if TYPE_CHECKING:
    from _types import QuestionBank
    from sources._base import BaseSource

logger = logging.getLogger(__name__)


def resolve_all(
    df: pd.DataFrame,
    question_bank: "QuestionBank",
    sources: dict[str, "BaseSource"],
    forecast_due_date: date | None = None,
) -> pd.DataFrame:
    """Resolve all questions in the exploded question set.

    Args:
        df: Exploded question set DataFrame.
        question_bank: {source_name: SourceQuestionBank}.
        sources: {source_name: BaseSource instance}.
        forecast_due_date: Date for nullification gating.

    Returns:
        Resolved DataFrame with resolved_to, resolved, market_value_on_due_date columns.
    """
    df = df.assign(
        resolved=False,
        resolved_to=np.nan,
        market_value_on_due_date=np.nan,
    )

    for source_name in df["source"].unique():
        logger.info(f"Resolving {source_name}.")
        if source_name not in sources:
            msg = f"Error in `resolve_all()`: not able to resolve {source_name}."
            logger.error(msg)
            raise ValueError(msg)

        source = sources[source_name]
        sqb = question_bank.get(source_name)
        if sqb is None:
            msg = f"Error in `resolve_all()`: {source_name}: question bank not found."
            logger.error(msg)
            raise ValueError(msg)

        dfq = sqb.dfq.copy()
        dfr = sqb.dfr.copy()
        if dfq.empty or dfr.empty:
            msg = (
                f"Error in `resolve_all()`: {source_name}: "
                f"dfq empty: {dfq.empty}. dfr empty: {dfr.empty}."
            )
            logger.error(msg)
            raise ValueError(msg)

        df = source.resolve(df.copy(), dfq, dfr, as_of=forecast_due_date)

        # Log stats
        df_tmp = df[df["source"] == source_name]
        n_na = len(df_tmp[df_tmp["resolved_to"].isna()])
        n_dates = len(df_tmp["resolution_date"].unique())
        combo_mask = df_tmp["id"].apply(lambda x: isinstance(x, tuple))
        n_combo = int(len(df_tmp[combo_mask]) / n_dates) if n_dates > 0 else 0
        n_single = int(len(df_tmp[~combo_mask]) / n_dates) if n_dates > 0 else 0
        logger.info(
            f"* Resolving {source_name}: #NaN {n_na}/{len(df_tmp)} Total for "
            f"{n_dates} dates, {n_single} single & {n_combo} combo questions."
        )

    # Remove unresolved data-source rows
    n_pre_drop = len(df)
    df = df[~(df["source"].isin(DATASET_SOURCE_NAMES) & (~df["resolved"]))]
    unresolved_drop = n_pre_drop - len(df)
    if unresolved_drop > 0:
        logger.info(f"Dropped {unresolved_drop:,} dataset questions that have not yet resolved.")

    # Remove NaN resolved_to rows
    n_pre_drop = len(df)
    df = df[~df["resolved_to"].isna()]
    na_drop = n_pre_drop - len(df)
    if na_drop > 0:
        logger.warning(f"! WARNING ! Dropped {na_drop:,} questions that have resolved to NaN.")

    return df.reset_index(drop=True)
