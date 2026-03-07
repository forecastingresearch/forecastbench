"""ACLED question source — custom resolution logic."""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from enum import Enum
from typing import ClassVar

import numpy as np
import pandas as pd

from _schemas import AcledResolutionFrame
from _types import SourceType

from ._base import DataSource

logger = logging.getLogger(__name__)


class QuestionType(Enum):
    """Types of ACLED questions."""

    N_30_DAYS_GT_30_DAY_AVG_OVER_PAST_360_DAYS = 0
    N_30_DAYS_X_10_GT_30_DAY_AVG_OVER_PAST_360_DAYS_PLUS_1 = 1


class AcledSource(DataSource):
    """ACLED data source — custom row-by-row resolution logic."""

    name: ClassVar[str] = "acled"
    display_name: ClassVar[str] = "ACLED"
    source_type: ClassVar[SourceType] = SourceType.DATA
    source_intro: ClassVar[str] = (
        "The Armed Conflict Location & Event Data Project (ACLED) collects real-time data on the "
        "locations, dates, actors, fatalities, and types of all reported political violence and "
        "protest events around the world. You're going to predict how questions based on this data "
        "will resolve."
    )
    resolution_criteria: ClassVar[str] = (
        "Resolves to the value calculated from the ACLED dataset once the data is published."
    )
    resolution_schema: ClassVar[type] = AcledResolutionFrame

    # ------------------------------------------------------------------
    # Custom _resolve (port of resolve_forecasts/acled.py:resolve)
    # ------------------------------------------------------------------

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve ACLED questions row by row."""
        logger.info("Resolving ACLED questions.")
        max_date = dfr["event_date"].max()
        mask = (df["source"] == "acled") & (df["resolution_date"] <= max_date)
        for index, row in df[mask].iterrows():
            forecast_due_date = row["forecast_due_date"].date()
            resolution_date = row["resolution_date"].date()
            if not self._is_combo(row):
                value = self._resolve_single_question(
                    mid=row["id"],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
            else:
                value1 = self._resolve_single_question(
                    mid=row["id"][0],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
                value2 = self._resolve_single_question(
                    mid=row["id"][1],
                    forecast_due_date=forecast_due_date,
                    resolution_date=resolution_date,
                    dfq=dfq,
                    dfr=dfr,
                )
                value = self._combo_change_sign(
                    value1, row["direction"][0]
                ) * self._combo_change_sign(value2, row["direction"][1])
            df.at[index, "resolved_to"] = value
        df.loc[mask, "resolved"] = True
        return df

    def _resolve_single_question(self, mid, forecast_due_date, resolution_date, dfq, dfr):
        """Resolve an individual ACLED question."""
        question = self._get_question(dfq, mid)
        if question is None:
            logger.warning(f"ACLED: could NOT find {mid}")
            return np.nan

        d = self._id_unhash(mid)

        return self._acled_resolve(
            **d,
            dfr=dfr,
            forecast_due_date=forecast_due_date,
            resolution_date=resolution_date,
        )

    # ------------------------------------------------------------------
    # ACLED-specific resolution logic (port of helpers/acled.py)
    # ------------------------------------------------------------------

    @staticmethod
    def _acled_resolve(key, dfr, country, event_type, forecast_due_date, resolution_date):
        """Resolve given the QuestionType."""
        lhs = AcledSource._sum_over_past_30_days(
            dfr=dfr,
            country=country,
            col=event_type,
            ref_date=resolution_date,
        )
        rhs = AcledSource._get_base_comparison_value(
            key=key,
            dfr=dfr,
            country=country,
            col=event_type,
            ref_date=forecast_due_date,
        )
        return int(lhs > rhs)

    @staticmethod
    def _sum_over_past_30_days(dfr, country, col, ref_date):
        """Sum over the 30 days before the ref_date."""
        dfc = dfr[dfr["country"] == country].copy()
        if dfc.empty:
            return 0
        start_date = ref_date - timedelta(days=30)
        dfc = dfc[
            (dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date < ref_date)
        ]
        return dfc[col].sum() if not dfc.empty else 0

    @staticmethod
    def _thirty_day_avg_over_past_360_days(dfr, country, col, ref_date):
        """Get the 30 day average over the 360 days before the ref_date."""
        dfc = dfr[dfr["country"] == country].copy()
        if dfc.empty:
            return 0
        start_date = ref_date - timedelta(days=360)
        dfc = dfc[
            (dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date < ref_date)
        ]
        return dfc[col].sum() / 12 if not dfc.empty else 0

    @staticmethod
    def _thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
        """Get 1 plus the 30 day average over the 360 days before the ref_date."""
        return 1 + AcledSource._thirty_day_avg_over_past_360_days(dfr, country, col, ref_date)

    @staticmethod
    def _get_base_comparison_value(key, dfr, country, col, ref_date):
        """Get the base comparison value given the question type."""
        if key == "last30Days.gt.30DayAvgOverPast360Days":
            return AcledSource._thirty_day_avg_over_past_360_days(
                dfr=dfr,
                country=country,
                col=col,
                ref_date=ref_date,
            )
        elif key == "last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1":
            return 10 * AcledSource._thirty_day_avg_over_past_360_days_plus_1(
                dfr=dfr,
                country=country,
                col=col,
                ref_date=ref_date,
            )
        raise ValueError("Invalid key.")

    # ------------------------------------------------------------------
    # Hash mapping
    # ------------------------------------------------------------------

    def _load_hash_mapping(self, raw_json: str) -> None:
        """Parse hash mapping from raw JSON string."""
        self.hash_mapping = json.loads(raw_json) if raw_json else {}

    def _dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON string."""
        return json.dumps(self.hash_mapping, indent=4)

    def _id_unhash(self, hash_key: str):
        """Decode ACLED Ids."""
        return self.hash_mapping.get(hash_key)

    def fetch(self, **kwargs):
        """Fetch raw data (stub for later phases)."""
        raise NotImplementedError

    def update(self, dfq: pd.DataFrame, **kwargs):
        """Update questions (stub for later phases)."""
        raise NotImplementedError
