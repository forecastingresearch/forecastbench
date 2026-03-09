"""Base class for market-based question sources."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import ClassVar

import numpy as np
import pandas as pd

from _types import SourceType
from helpers_new import dates

from ._base import BaseSource

logger = logging.getLogger(__name__)


class MarketSource(BaseSource):
    """Base class for market-based question sources (manifold, metaculus, etc.)."""

    source_type: ClassVar[SourceType] = SourceType.MARKET

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve market-based questions using market prices and resolution status."""
        logger.info(f"Resolving Market `{self.name}`.")
        forecast_due_date = df["forecast_due_date"].unique()[0]
        df_market, df = self._split_dataframe_on_source(df=df, source=self.name)

        # Check that we have market info for all markets
        unique_ids = dfr["id"].unique()

        def check_id(mid):
            if self._is_combo(mid):
                for midi in mid:
                    check_id(midi)
            elif mid not in unique_ids:
                msg = f"Missing resolution values in dfr for (source: {self.name}, id: {mid})!!!"
                logger.error(msg)
                raise ValueError(msg)

        df_market["id"].apply(lambda x: check_id(x))

        # Split into standard and combo questions
        combo_mask = df_market["id"].apply(lambda x: self._is_combo(x))
        df_standard = df_market[~combo_mask].copy()
        df_combo = df_market[combo_mask].copy()

        # Resolve to yesterday's market value
        yesterday = dates.get_date_today() - timedelta(days=1)
        df_standard["yesterday"] = pd.to_datetime(yesterday)
        df_standard = pd.merge(
            df_standard,
            dfr,
            left_on=["id", "yesterday"],
            right_on=["id", "date"],
            how="left",
        )
        df_standard["resolved_to"] = df_standard["value"]
        df_standard = df_standard.drop(columns=["date", "value", "yesterday"])

        # Set all resolution dates to yesterday
        df_combo["resolution_date"] = pd.to_datetime(yesterday)
        df_standard["resolution_date"] = pd.to_datetime(yesterday)

        # Get market values at forecast_due_date (for imputation)
        df_standard = pd.merge(
            df_standard,
            dfr,
            left_on=["id", "forecast_due_date"],
            right_on=["id", "date"],
            how="left",
        )
        df_standard["market_value_on_due_date"] = df_standard["value"]
        df_standard = df_standard.drop(columns=["date", "value"])

        # Get market values at forecast_due_date - 1 (for naive forecaster)
        df_standard["forecast_due_date_minus_one"] = pd.to_datetime(
            df_standard["forecast_due_date"]
        ) - pd.Timedelta(days=1)
        df_standard = pd.merge(
            df_standard,
            dfr,
            left_on=["id", "forecast_due_date_minus_one"],
            right_on=["id", "date"],
            how="left",
        )
        df_standard["market_value_on_due_date_minus_one"] = df_standard["value"]
        df_standard = df_standard.drop(columns=["date", "value", "forecast_due_date_minus_one"])

        # Overwrite with final resolved value for resolved markets
        warnings = []
        for mid in dfq.loc[dfq["resolved"], "id"]:
            if not (df_standard["id"] == mid).any():
                continue

            resolved_value = dfr.loc[dfr["id"] == mid, "value"].iat[-1]
            resolution_date = self._get_market_resolution_date(dfq[dfq["id"] == mid])
            df_standard.loc[df_standard["id"] == mid, "resolved"] = True
            df_standard.loc[df_standard["id"] == mid, "resolved_to"] = resolved_value
            df_standard.loc[df_standard["id"] == mid, "resolution_date"] = resolution_date

            if resolved_value != 0 and resolved_value != 1:
                url = dfq[dfq["id"] == mid]["url"].iloc[0]
                message = (
                    f"`{self.name}` question {mid} resolved to {resolved_value} (not 0 or 1). "
                    "Resolving to NaN for now. Check to ensure data pulled correctly.\n"
                    f"{url}\n"
                )
                logger.warning(message)
                df_standard.loc[df_standard["id"] == mid, "resolved_to"] = np.nan
                if not pd.isna(resolved_value):
                    warnings.append(message)

            if resolution_date <= forecast_due_date.date():
                df_standard.loc[df_standard["id"] == mid, "resolved_to"] = np.nan
                rd = resolution_date.strftime("%Y-%m-%d")
                fd = forecast_due_date.strftime("%Y-%m-%d")
                url = dfq[dfq["id"] == mid]["url"].iloc[0]
                logger.warning(
                    f"`{self.name} question {mid}; was resolved on {rd} but the forecast date is "
                    f"{fd}. Nullifying!\n     {url}"
                )

        df_standard["resolution_date"] = pd.to_datetime(
            df_standard["resolution_date"], errors="coerce"
        )
        df_standard.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)

        # Combo resolutions
        def update_col(index, value0, value1, dir0, dir1, col):
            df_combo.at[index, col] = self._combo_change_sign(
                value0, dir0
            ) * self._combo_change_sign(value1, dir1)

        for index, row in df_combo.iterrows():
            id0, id1 = row["id"]
            try:
                id0_data = df_standard[df_standard["id"] == id0].iloc[0]
                id1_data = df_standard[df_standard["id"] == id1].iloc[0]
            except IndexError:
                df_combo.at[index, "resolved_to"] = np.nan
                continue
            dir0, dir1 = row["direction"]

            for col in ["resolved_to", "market_value_on_due_date"]:
                update_col(
                    index=index,
                    value0=id0_data[col],
                    value1=id1_data[col],
                    dir0=dir0,
                    dir1=dir1,
                    col=col,
                )

            resolution_date = self._get_combo_question_resolution_date(
                is_resolved0=id0_data["resolved"],
                is_resolved1=id1_data["resolved"],
                dir0=dir0,
                dir1=dir1,
                resolved_to0=id0_data["resolved_to"],
                resolved_to1=id1_data["resolved_to"],
                resolution_date0=id0_data["resolution_date"],
                resolution_date1=id1_data["resolution_date"],
            )
            if resolution_date:
                df_combo.at[index, "resolved"] = True
                df_combo.at[index, "resolution_date"] = resolution_date

        df_combo.sort_values(by=["id", "resolution_date"], inplace=True, ignore_index=True)
        df_source = pd.concat([df_standard, df_combo]).drop_duplicates()
        df = pd.concat([df, df_source], ignore_index=True)

        # Attach warnings for orchestration to send via Slack
        if warnings:
            existing = df.attrs.get("_resolve_warnings", [])
            df.attrs["_resolve_warnings"] = existing + warnings

        return df

    # ------------------------------------------------------------------
    # Market-specific static methods
    # ------------------------------------------------------------------

    @staticmethod
    def _get_market_resolution_date(row):
        """Return the minimum of the market close date and the resolution date."""

        def to_date_or_max(s):
            try:
                return dates.convert_zulu_to_datetime(s).date()
            except (ValueError, TypeError):
                return date.max

        close_date = to_date_or_max(row["market_info_close_datetime"].iloc[0])
        resolution_date = to_date_or_max(row["market_info_resolution_datetime"].iloc[0])
        return min(close_date, resolution_date)

    @staticmethod
    def _get_combo_question_resolution_date(
        is_resolved0,
        is_resolved1,
        dir0,
        dir1,
        resolved_to0,
        resolved_to1,
        resolution_date0,
        resolution_date1,
    ):
        """Return the resolution date if a combo question has resolved. None otherwise."""
        try:
            return MarketSource._get_combo_question_resolution_date_helper(
                is_resolved0,
                is_resolved1,
                dir0,
                dir1,
                resolved_to0,
                resolved_to1,
                resolution_date0,
                resolution_date1,
            )
        except ValueError:
            pass
        return None

    @staticmethod
    def _get_combo_question_resolution_date_helper(
        is_resolved0,
        is_resolved1,
        dir0,
        dir1,
        resolved_to0,
        resolved_to1,
        resolution_date0,
        resolution_date1,
    ):
        """Determine when a combo question is resolved based on its two sub-questions."""
        if not is_resolved0 and not is_resolved1:
            return None

        def same_dir(is_resolved, direction, resolved_to):
            return bool(
                is_resolved
                and (
                    (direction == 1 and resolved_to == 1) or (direction == -1 and resolved_to == 0)
                )
            )

        def diff_dir(is_resolved, direction, resolved_to):
            return bool(
                is_resolved
                and (
                    (direction == 1 and resolved_to == 0) or (direction == -1 and resolved_to == 1)
                )
            )

        zero_same_dir = same_dir(is_resolved0, dir0, resolved_to0)
        zero_diff_dir = diff_dir(is_resolved0, dir0, resolved_to0)
        one_same_dir = same_dir(is_resolved1, dir1, resolved_to1)
        one_diff_dir = diff_dir(is_resolved1, dir1, resolved_to1)

        # When one or more questions resolve NaN
        if np.isnan(resolved_to0) and np.isnan(resolved_to1):
            return min(resolution_date0, resolution_date1)
        elif np.isnan(resolved_to0):
            if one_diff_dir:
                return min(resolution_date0, resolution_date1)
            else:
                return resolution_date0
        elif np.isnan(resolved_to1):
            if zero_diff_dir:
                return min(resolution_date0, resolution_date1)
            else:
                return resolution_date1

        # When no questions resolve NaN
        if zero_same_dir and one_same_dir:
            return max(resolution_date0, resolution_date1)
        if zero_diff_dir and one_diff_dir:
            return min(resolution_date0, resolution_date1)
        if zero_same_dir and one_diff_dir:
            return resolution_date1
        if one_same_dir and zero_diff_dir:
            return resolution_date0

        # When only one question has resolved
        if zero_diff_dir:
            return resolution_date0
        if one_diff_dir:
            return resolution_date1

        raise ValueError(
            "\n\nCombo question should have a resolution date:\n"
            f"{(zero_same_dir, zero_diff_dir, is_resolved0, dir0, resolved_to0)}\n"
            f"{(one_same_dir, one_diff_dir, is_resolved1, dir1, resolved_to1)}\n\n"
        )
