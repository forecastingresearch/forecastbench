"""Base class for dataset-based question sources."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import numpy as np
import pandas as pd

from _types import SourceType

from ._base import BaseSource

if TYPE_CHECKING:
    from pandera.typing import DataFrame

    from _schemas import QuestionFrame, ResolutionFrame, ResolveReadyFrame

logger = logging.getLogger(__name__)


class DatasetSource(BaseSource):
    """Base class for dataset-based question sources (dbnomics, fred, yfinance, etc.)."""

    source_type: ClassVar[SourceType] = SourceType.DATASET

    def _resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: DataFrame[ResolutionFrame],
    ) -> DataFrame[ResolveReadyFrame]:
        """Resolve data-based questions via binary comparison of resolution vs due-date values."""
        logger.info(f"Resolving {self.name}.")
        self._validate_ids(df, dfr)

        # Split into standard and combo questions
        combo_mask = df["id"].apply(lambda x: self._is_combo(x))
        df_standard = df[~combo_mask].copy()
        df_combo = df[combo_mask].copy()

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
        df_standard["resolved"] = ~df_standard["resolved_to"].isna()
        df_combo["resolved"] = ~df_combo["resolved_to"].isna()
        return pd.concat([df_standard, df_combo], ignore_index=True)
