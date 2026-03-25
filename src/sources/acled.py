"""ACLED question source."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import ClassVar

import numpy as np
import pandas as pd

from _fb_types import SourceType
from _schemas import AcledResolutionFrame

from ._dataset import DatasetSource

logger = logging.getLogger(__name__)


class AcledSource(DatasetSource):
    """Armed Conflict Location & Event Data source with custom resolution logic."""

    name: ClassVar[str] = "acled"
    display_name: ClassVar[str] = "ACLED"
    source_type: ClassVar[SourceType] = SourceType.DATASET
    resolution_schema: ClassVar[type] = AcledResolutionFrame

    def _resolve(self, df: pd.DataFrame, dfq: pd.DataFrame, dfr: pd.DataFrame) -> pd.DataFrame:
        """Resolve ACLED questions row by row."""
        logger.info("Resolving ACLED questions.")
        max_date = dfr["event_date"].max()
        mask = df["resolution_date"] <= max_date
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
        return df, []

    def _resolve_single_question(self, mid, forecast_due_date, resolution_date, dfq, dfr):
        """Resolve an individual ACLED question by unhashing the ID and comparing aggregates."""
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

    @staticmethod
    def _acled_resolve(key, dfr, country, event_type, forecast_due_date, resolution_date):
        """Compare 30-day sum at resolution_date against baseline at forecast_due_date."""
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
        """Sum of col for country over the 30 days before ref_date."""
        from helpers.acled import sum_over_past_30_days

        return sum_over_past_30_days(dfr, country, col, ref_date)

    @staticmethod
    def _thirty_day_avg_over_past_360_days(dfr, country, col, ref_date):
        """30-day average (total/12) over the 360 days before ref_date."""
        from helpers.acled import thirty_day_avg_over_past_360_days

        return thirty_day_avg_over_past_360_days(dfr, country, col, ref_date)

    @staticmethod
    def _thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
        """1 + 30-day average over the 360 days before ref_date."""
        from helpers.acled import thirty_day_avg_over_past_360_days_plus_1

        return thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date)

    @staticmethod
    def _get_base_comparison_value(key, dfr, country, col, ref_date):
        """Return the baseline value for comparison given the question key string."""
        from helpers.acled import get_base_comparison_value

        return get_base_comparison_value(key, dfr, country, col, ref_date)

    # ------------------------------------------------------------------
    # Hash mapping
    # ------------------------------------------------------------------

    def populate_hash_mapping(self, raw_json: str) -> None:
        """Parse hash mapping from raw JSON string."""
        self.hash_mapping = json.loads(raw_json) if raw_json else {}

    def dump_hash_mapping(self) -> str | None:
        """Serialize hash mapping to JSON string."""
        return json.dumps(self.hash_mapping, indent=4)

    def _id_hash(self, d: dict) -> str:
        """Encode ACLED Ids and store in hash_mapping."""
        dictionary_json = json.dumps(d, sort_keys=True)
        hash_key = hashlib.sha256(dictionary_json.encode()).hexdigest()
        self.hash_mapping[hash_key] = d
        return hash_key

    def _id_unhash(self, hash_key: str):
        """Look up the original question dict from a hash key."""
        return self.hash_mapping.get(hash_key)
