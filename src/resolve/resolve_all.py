"""Resolve all questions across sources."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from _schemas import ExplodedQuestionSetFrame, ResolveReadyFrame
from sources import DATASET_SOURCE_NAMES

if TYPE_CHECKING:
    from pandera.typing import DataFrame

    from _types import QuestionBank
    from sources._base import BaseSource

logger = logging.getLogger(__name__)


def resolve_all(
    df: DataFrame[ExplodedQuestionSetFrame],
    question_bank: QuestionBank,
    sources: dict[str, "BaseSource"],
    forecast_due_date: date | None = None,
) -> DataFrame[ResolveReadyFrame]:
    """Resolve all questions in the exploded question set.

    Args:
        df: Exploded question set DataFrame.
        question_bank: {source_name: SourceQuestionBank}.
        sources: {source_name: BaseSource instance}.
        forecast_due_date: Date for nullification gating.

    Returns:
        Resolved DataFrame with resolved_to, resolved, market_value_on_due_date columns.
    """
    ExplodedQuestionSetFrame.validate(df)
    df = df.assign(
        resolved=False,
        resolved_to=np.nan,
        market_value_on_due_date=np.nan,
    )

    parts = []
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

        df_source = df[df["source"] == source_name].copy()
        df_source = source.resolve(df_source, dfq, dfr, as_of=forecast_due_date)
        parts.append(df_source)

        # Log stats
        n_na = len(df_source[df_source["resolved_to"].isna()])
        n_dates = len(df_source["resolution_date"].unique())
        combo_mask = df_source["id"].apply(lambda x: isinstance(x, tuple))
        n_combo = int(len(df_source[combo_mask]) / n_dates) if n_dates > 0 else 0
        n_single = int(len(df_source[~combo_mask]) / n_dates) if n_dates > 0 else 0
        logger.info(
            f"* Resolving {source_name}: #NaN {n_na}/{len(df_source)} Total for "
            f"{n_dates} dates, {n_single} single & {n_combo} combo questions."
        )

    df = pd.concat(parts, ignore_index=True)

    # Propagate _resolve_warnings from parts
    warnings = []
    for part in parts:
        warnings.extend(part.attrs.get("_resolve_warnings", []))
    if warnings:
        df.attrs["_resolve_warnings"] = warnings

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
