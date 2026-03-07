"""Impute missing forecasts."""

from __future__ import annotations

import logging

import pandas as pd

from helpers_new import constants
from sources import MARKET_SOURCE_NAMES

logger = logging.getLogger(__name__)


def impute_missing_forecasts(
    df: pd.DataFrame,
    organization: str,
    model_organization: str,
    model: str,
) -> pd.DataFrame:
    """Fill in np.nan forecast values with context-appropriate forecasts.

    - Default imputation: 0.5
    - Imputed Forecaster: market_value_on_due_date
    - Naive Forecaster: market_value_on_due_date_minus_one
    """
    logger.info("Impute missing forecasts.")
    df["forecast"] = df["forecast"].astype(float)
    df["imputed"] = False
    n_orig = df["forecast"].isna().sum()
    if n_orig == 0:
        logger.info("No missing values → nothing to impute.")
        return df
    logger.info(f" Found {n_orig:,} missing values to impute.")

    df.loc[df["forecast"].isna(), "imputed"] = True
    df.loc[df["imputed"], "forecast"] = 0.5
    if organization == constants.BENCHMARK_NAME and model_organization == constants.BENCHMARK_NAME:
        market_imputed_mask = (df["source"].isin(MARKET_SOURCE_NAMES)) & df["imputed"]
        if model == "Imputed Forecaster":
            df.loc[market_imputed_mask, "forecast"] = df["market_value_on_due_date"]
        elif model == "Naive Forecaster":
            df.loc[market_imputed_mask, "forecast"] = df["market_value_on_due_date_minus_one"]
    return df
