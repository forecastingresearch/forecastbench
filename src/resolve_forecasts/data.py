"""Resolution functions for basic data questions.

Does the basic comprison to see if the numeric value on the resolution date is greater than the
numeric value on the forecast due date.

Works for DBnomics, FRED, Yahoo Finance.
"""

import logging
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import resolution  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def resolve(source, df, dfq, dfr):
    """Resolve data-based questions.

    Params:
    - df: dataframe of questions to resolve
    - dfq: dataframe of all questions
    - dfr: dataframe of all resolution values
    """
    logger.info(f"Resolving {source}.")
    df_data, df = resolution.split_dataframe_on_source(df=df, source=source)

    # Check that we have stock info for all stocks in the dataset
    unique_ids_for_resolved_stocks = dfr["id"].unique()

    def check_id(mid):
        if resolution.is_combo(mid):
            for midi in mid:
                check_id(midi)
        elif mid not in unique_ids_for_resolved_stocks:
            msg = f"Missing resolution values in dfr for {source} id: {mid})!!!"
            logger.error(msg)
            raise ValueError(msg)

    df_data["id"].apply(lambda x: check_id(x))

    # Handle single stocks first: split into standard and combo questions
    combo_mask = df_data["id"].apply(lambda x: resolution.is_combo(x))
    df_standard = df_data[~combo_mask].copy()
    df_combo = df_data[combo_mask].copy()

    # Get stock values at resolution_date
    df_standard = pd.merge(
        df_standard,
        dfr,
        left_on=["id", "resolution_date"],
        right_on=["id", "date"],
        how="left",
    )
    df_standard["resolved_to"] = df_standard["value"]
    df_standard = df_standard.drop(columns=["date", "value"])

    # Get stock values at forecast_due_date
    # These values are assigned to any forecasts the organization may have omitted.
    df_standard = pd.merge(
        df_standard,
        dfr,
        left_on=["id", "forecast_due_date"],
        right_on=["id", "date"],
        how="left",
    )
    df_standard["market_value_on_due_date"] = df_standard["value"]
    df_standard = df_standard.drop(columns=["date", "value"])
    df_standard.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)

    # DBnomics stores N/A if no weather data was reported, so the `resolved_to` and
    # `market_value_on_due_date` columns potentially have "N/A" values. Replace with `np.nan`
    # (using coerce) to nullify.
    df_standard[["resolved_to", "market_value_on_due_date"]] = df_standard[
        ["resolved_to", "market_value_on_due_date"]
    ].apply(pd.to_numeric, errors="coerce")

    # resolve questions to 0 or 1. If np.nan is encountered in comparison, resolve to np.nan
    df_standard["resolved_to"] = df_standard.apply(
        lambda row: (
            np.nan
            if pd.isna(row["resolved_to"]) or pd.isna(row["market_value_on_due_date"])
            else float(row["resolved_to"] > row["market_value_on_due_date"])
        ),
        axis=1,
    )

    # Setup combo resolutions given df_standard
    for index, row in df_combo.iterrows():
        id0, id1 = row["id"]
        dir0, dir1 = row["direction"]
        date_mask = df_standard["resolution_date"] == row["resolution_date"]
        try:
            value_id0 = df_standard.loc[(df_standard["id"] == id0) & date_mask, "resolved_to"].iloc[
                0
            ]
            value_id1 = df_standard.loc[(df_standard["id"] == id1) & date_mask, "resolved_to"].iloc[
                0
            ]
            df_combo.at[index, "resolved_to"] = resolution.combo_change_sign(
                value_id0, dir0
            ) * resolution.combo_change_sign(value_id1, dir1)
        except IndexError:
            df_combo.at[index, "resolved_to"] = np.nan

    df_combo.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)
    df_standard["resolved"] = True
    df_combo["resolved"] = True
    df = pd.concat([df, df_standard, df_combo], ignore_index=True)
    return df
