"""Base class for data-based question sources."""

from __future__ import annotations

import logging
from typing import ClassVar

import numpy as np
import pandas as pd

from _types import SourceType

from ._base import BaseSource

logger = logging.getLogger(__name__)


class DataSource(BaseSource):
    """Base class for data-based question sources (dbnomics, fred, yfinance, etc.)."""

    source_type: ClassVar[SourceType] = SourceType.DATA

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve data-based questions via binary comparison of resolution vs due-date values."""
        logger.info(f"Resolving {self.name}.")
        df_data, df = self._split_dataframe_on_source(df=df, source=self.name)

        # Check that we have data for all IDs
        unique_ids = dfr["id"].unique()

        def check_id(mid):
            if self._is_combo(mid):
                for midi in mid:
                    check_id(midi)
            elif mid not in unique_ids:
                msg = f"Missing resolution values in dfr for {self.name} id: {mid})!!!"
                logger.error(msg)
                raise ValueError(msg)

        df_data["id"].apply(lambda x: check_id(x))

        # Split into standard and combo questions
        combo_mask = df_data["id"].apply(lambda x: self._is_combo(x))
        df_standard = df_data[~combo_mask].copy()
        df_combo = df_data[combo_mask].copy()

        # Get values at resolution_date
        df_standard = pd.merge(
            df_standard,
            dfr,
            left_on=["id", "resolution_date"],
            right_on=["id", "date"],
            how="left",
        )
        df_standard["resolved_to"] = df_standard["value"]
        df_standard = df_standard.drop(columns=["date", "value"])

        # Get values at forecast_due_date (for imputation)
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

        # Coerce N/A to NaN
        df_standard[["resolved_to", "market_value_on_due_date"]] = df_standard[
            ["resolved_to", "market_value_on_due_date"]
        ].apply(pd.to_numeric, errors="coerce")

        # Binary comparison: resolved_to = float(resolved_to > market_value_on_due_date)
        df_standard["resolved_to"] = df_standard.apply(
            lambda row: (
                np.nan
                if pd.isna(row["resolved_to"]) or pd.isna(row["market_value_on_due_date"])
                else float(row["resolved_to"] > row["market_value_on_due_date"])
            ),
            axis=1,
        )

        # Combo resolutions
        for index, row in df_combo.iterrows():
            id0, id1 = row["id"]
            dir0, dir1 = row["direction"]
            date_mask = df_standard["resolution_date"] == row["resolution_date"]
            try:
                value_id0 = df_standard.loc[
                    (df_standard["id"] == id0) & date_mask, "resolved_to"
                ].iloc[0]
                value_id1 = df_standard.loc[
                    (df_standard["id"] == id1) & date_mask, "resolved_to"
                ].iloc[0]
                df_combo.at[index, "resolved_to"] = self._combo_change_sign(
                    value_id0, dir0
                ) * self._combo_change_sign(value_id1, dir1)
            except IndexError:
                df_combo.at[index, "resolved_to"] = np.nan

        df_combo.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)
        df_standard["resolved"] = True
        df_combo["resolved"] = True
        df = pd.concat([df, df_standard, df_combo], ignore_index=True)
        return df
