"""Market resolution functions."""

import logging
import os
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import constants, env, resolution  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MARKET_SOURCES = ("manifold", "metaculus", "infer")


def make_resolution_df(source):
    """Prepare market data for resolution."""
    files = gcp.storage.list_with_prefix(bucket_name=env.QUESTION_BANK_BUCKET, prefix=source)
    df = pd.concat(
        [
            pd.read_json(
                f"gs://{env.QUESTION_BANK_BUCKET}/{f}",
                lines=True,
                dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                convert_dates=False,
            )
            for f in tqdm(files, f"downloading `{source}` resoultion files")
            if f.startswith(f"{source}/")
        ],
        ignore_index=True,
    )
    df = resolution.make_columns_hashable(df)
    df["date"] = pd.to_datetime(df["date"])
    df["id"] = df["id"].astype(str)
    return df


def resolve(source, df, dfq, dfr):
    """Resolve market-based questions.

    NB: For the moment, don't account for combo questions as we didn't collect forecasts on these
    for market-based qusetions.

    Params:
    - source: string representing this data source
    - df: dataframe of questions to resolve
    - dfq: dataframe of all questions for this source
    - dfr: market values and resolutions for all markets belonging to `source`
    """
    logger.info(f"Resolving Market `{source}.`")
    forecast_submitted_date = df["forecast_submitted_date"].unique()[0]
    df_market, df = resolution.split_dataframe_on_source(df=df, source=source)
    df_market["id"] = df_market["id"].astype(str)

    # Check that we're getting market info for all markets in the dataset
    unique_ids_for_resolved_markets = dfr["id"].unique()
    for mid in df_market["id"].unique():
        if mid not in unique_ids_for_resolved_markets:
            logger.error(f"MISSING resolution values in dfr for (source: {source}, id: {mid})!!!")

    # Get market values at forecast_evaluation_date
    df_market = pd.merge(
        df_market,
        dfr,
        left_on=["id", "forecast_evaluation_date"],
        right_on=["id", "date"],
        how="left",
    )
    df_market["resolved_to"] = df_market["value"]
    df_market = df_market.drop(columns=["date", "value"])

    # Get market values at forecast_submitted_date
    # These values are assigned to any forecasts the organization may have omitted.
    df_market = pd.merge(
        df_market,
        dfr,
        left_on=["id", "forecast_submitted_date"],
        right_on=["id", "date"],
        how="left",
    )
    df_market["market_value_at_submission"] = df_market["value"]
    df_market = df_market.drop(columns=["date", "value"])

    # Overwrite resolved_to values with resolved_value if question has resolved
    for mid in dfq.loc[dfq["resolved"], "id"]:
        if (df_market["id"] == mid).any():
            resolved_value = dfr.loc[dfr["id"] == mid, "value"].iat[-1]
            resolution_date = dfr.loc[dfr["id"] == mid, "date"].iat[-1]
            df_market.loc[df_market["id"] == mid, "resolved"] = True
            df_market.loc[df_market["id"] == mid, "resolved_to"] = resolved_value

            if resolved_value != 0 and resolved_value != 1:
                # Print warning if market resolved to something other than 0 or 1. This can be
                # valid, just want to be aware when this happens to ensure we're handling it
                # correctly.
                logger.warning(
                    f"Question resolved to {resolved_value} (not 0 or 1). Check to ensure data "
                    "pulled correctly."
                )

            if resolution_date <= forecast_submitted_date:
                # Discard all forecasts that resolved <= forecast_submitted_date
                df_market.loc[df_market["id"] == mid, "resolved_to"] = np.nan
                rd = resolution_date.strftime("%Y-%m-%d")
                fd = forecast_submitted_date.strftime("%Y-%m-%d")
                logger.info(
                    f"Nullifying qusetion {mid}; it was resolved on {rd} but the forecast date is "
                    f"{fd}."
                )

    df_market.sort_values(by=["id", "forecast_evaluation_date"], inplace=True, ignore_index=True)
    df = pd.concat([df, df_market], ignore_index=True)

    return df
