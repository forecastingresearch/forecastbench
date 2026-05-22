"""Manifold question source."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, ClassVar

import backoff
import certifi
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import ManifoldFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates

from ._market import MarketSource

logger = logging.getLogger(__name__)

_MANIFOLD_API_BASE = "https://api.manifold.markets/v0"

_TOPIC_SLUGS = [
    "ai",
    "biotech",
    "business",
    "celebrities",
    "chess",
    "china",
    "climate",
    "culture-default",
    "economics-default",
    "entertainment",
    "europe",
    "finance",
    "gaming",
    "geopolitics",
    "health",
    "india",
    "mathematics",
    "middle-east",
    "movies",
    "music-f213cbf1eab5",
    "politics-default",
    "programming",
    "russia",
    "science-default",
    "space",
    "sports-default",
    "stocks",
    "technical-ai-timelines",
    "technology-default",
    "uk-politics",
    "ukraine",
    "us-politics",
    "wars",
    "world-default",
]

_MAX_RESOLUTION_DATE_IN_DAYS = 365 * 2
_MIN_BETTOR_COUNT = 17
_MIN_LIQUIDITY = 120


class ManifoldSource(MarketSource):
    """Manifold prediction market source."""

    name: ClassVar[str] = "manifold"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(self, **kwargs: Any) -> DataFrame[ManifoldFetchFrame]:
        """Fetch market IDs from Manifold search-markets endpoint.

        Calls search-markets (1 global + N topic slugs), filters by
        min bettors, min liquidity, and max resolution date.
        """
        ids = self._search_markets()
        logger.info(f"Discovered {len(ids)} candidate market IDs from search.")
        return pd.DataFrame({"id": sorted(ids)})

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[ManifoldFetchFrame],
        *,
        existing_resolution_files: dict[str, DataFrame[ResolutionFrame]] | None = None,
        files_in_storage: list[str] | None = None,
    ) -> UpdateResult:
        """Process fetched IDs into updated questions and resolution files.

        For each new ID in dff, appends to dfq. Then for each unresolved question,
        fetches market details and builds resolution files. Finally regenerates
        missing resolution files for resolved questions.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[ManifoldFetchFrame]): Freshly fetched market IDs.
            existing_resolution_files (dict | None): Per-question existing resolution data.
            files_in_storage (list[str] | None): Existing resolution file paths in storage.
        """
        existing_resolution_files = existing_resolution_files or {}
        files_in_storage = files_in_storage or []
        resolution_files: dict[str, pd.DataFrame] = {}

        # --- Append new IDs from dff to dfq ---
        new_ids = dff[~dff["id"].isin(dfq["id"])]["id"]
        if not new_ids.empty:
            df_new = pd.DataFrame({"id": new_ids}).assign(
                **{col: None for col in dfq.columns if col != "id"}
            )
            df_new["resolved"] = False
            df_new["freeze_datetime_value_explanation"] = "The market value."
            df_new["market_info_resolution_datetime"] = "N/A"
            dfq = pd.concat([dfq, df_new], ignore_index=True)

        # --- Update all unresolved questions ---
        dfq["resolved"] = dfq["resolved"].astype(bool)
        for index, row in dfq[~dfq["resolved"]].iterrows():
            market = self._get_market(row["id"])
            if market is None:
                continue

            # Assign market details to dfq row
            dfq.at[index, "question"] = market["question"]
            dfq.at[index, "background"] = market["textDescription"]
            dfq.at[index, "market_info_resolution_criteria"] = "N/A"
            dfq.at[index, "market_info_open_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
                market["createdTime"]
            )
            dfq.at[index, "market_info_close_datetime"] = dates.convert_epoch_time_in_ms_to_iso(
                market["closeTime"]
            )
            dfq.at[index, "url"] = market["url"]
            if market["isResolved"]:
                dfq.at[index, "resolved"] = True
                dfq.at[index, "market_info_resolution_datetime"] = (
                    dates.convert_epoch_time_in_ms_to_iso(market["resolutionTime"])
                )
            dfq.at[index, "forecast_horizons"] = "N/A"

            # Build resolution file
            existing_df = existing_resolution_files.get(row["id"])
            df_res = self._build_resolution_file(
                market=market,
                market_info_resolution_datetime=dfq.at[index, "market_info_resolution_datetime"],
                existing_df=existing_df,
            )
            if df_res is not None:
                dfq.at[index, "freeze_datetime_value"] = df_res["value"].iloc[-1]
                # if rebuilt, then write; else - skip
                if df_res is not existing_df:
                    logger.info(f"Rebuilt, will write - id={row['id']}")
                    resolution_files[row["id"]] = df_res
                else:
                    logger.info(f"Skipped writing to resolution files, not changed -id={row['id']}")

        # --- Regenerate missing resolution files for resolved questions ---
        for _index, row in dfq[dfq["resolved"]].iterrows():
            filename = f"{self.name}/{row['id']}.jsonl"
            if filename not in files_in_storage and row["id"] not in resolution_files:
                market = self._get_market(row["id"])
                if market is None:
                    continue
                df_res = self._build_resolution_file(
                    market=market,
                    market_info_resolution_datetime=row["market_info_resolution_datetime"],
                    existing_df=None,
                )
                if df_res is not None:
                    resolution_files[row["id"]] = df_res

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: search-markets API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=500,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _call_search_endpoint(self, additional_params: dict | None = None) -> set[str]:
        """Call search-markets and return qualifying market IDs."""
        endpoint = f"{_MANIFOLD_API_BASE}/search-markets"
        params: dict[str, Any] = {
            "sort": "most-popular",
            "contractType": "BINARY",
            "filter": "open",
            "limit": 100,
        }
        if additional_params:
            params.update(additional_params)
        logger.info(f"Calling {endpoint} with additional params {additional_params}")

        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(
                f"Request to endpoint failed for {endpoint}: {response.status_code} Error. "
                f"{response.text}"
            )
            response.raise_for_status()

        today = dates.get_date_today()
        max_resolution_date = today + timedelta(days=_MAX_RESOLUTION_DATE_IN_DAYS)

        def resolves_by(close_time_epoch_ms: int) -> bool:
            close_sec = min(close_time_epoch_ms / 1000, dates.MAX_EPOCH_SEC)
            close_date = dates.convert_epoch_time_in_sec_to_datetime(close_sec).date()
            return close_date <= max_resolution_date

        return {
            market["id"]
            for market in response.json()
            if market["uniqueBettorCount"] >= _MIN_BETTOR_COUNT
            and market["totalLiquidity"] >= _MIN_LIQUIDITY
            and resolves_by(market["closeTime"])
        }

    def _search_markets(self) -> set[str]:
        """Discover market IDs across all topic slugs."""
        logger.info("Calling Manifold search-markets endpoint")
        ids = self._call_search_endpoint()
        for topic in _TOPIC_SLUGS:
            ids |= self._call_search_endpoint({"topicSlug": topic})
        return ids

    # ------------------------------------------------------------------
    # Private: market detail API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=200,
        max_tries=10,
        factor=2,
        base=2,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_market(self, market_id: str) -> dict:
        """Fetch full market details from /market/{id}."""
        logger.info(f"Calling market endpoint for {market_id}")
        endpoint = f"{_MANIFOLD_API_BASE}/market/{market_id}"
        response = requests.get(endpoint, verify=certifi.where())
        if not response.ok:
            logger.error(f"Request to market endpoint failed for {market_id}.")
            response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Private: bets API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=200,
        max_tries=10,
        factor=2,
        base=2,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_market_bets(self, market_id: str) -> list[dict]:
        """Fetch all bets for a market with pagination."""
        logger.info(f"Calling bets endpoint for {market_id}")
        endpoint = f"{_MANIFOLD_API_BASE}/bets"
        max_bets_to_return = 1000
        params: dict[str, Any] = {
            "contractId": market_id,
            "limit": max_bets_to_return,
        }

        all_bets: list[dict] = []
        n_requests = 0
        while True:
            n_requests += 1
            if n_requests % 100 == 0:
                logger.info(f"Request number {n_requests} for {market_id}.")
            response = requests.get(endpoint, params=params, verify=certifi.where())
            if not response.ok:
                logger.error(f"Request to bets endpoint failed for {market_id}.")
                response.raise_for_status()
            new_bets = response.json()
            if not new_bets:
                break

            all_bets += new_bets
            if (
                all_bets[-1]["createdTime"] < constants.BENCHMARK_START_DATE_EPOCHTIME_MS
                or len(new_bets) < max_bets_to_return
            ):
                break
            params["before"] = all_bets[-1]["id"]
        return all_bets

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    def _build_resolution_file(
        self,
        market: dict,
        market_info_resolution_datetime: str,
        existing_df: DataFrame[ResolutionFrame] | None = None,
    ) -> DataFrame[ResolutionFrame] | None:
        """Build or update a resolution file for a single market."""
        yesterday = dates.get_date_today() - timedelta(days=1)
        market_id = market["id"]

        # --- Already up-to-date check ---
        if (
            existing_df is not None
            and not existing_df.empty
            and pd.to_datetime(existing_df["date"].max()).date() >= yesterday
        ):
            return existing_df

        # --- Fetch bets and build daily series ---
        forecasts = self._get_market_bets(market_id)
        df = pd.DataFrame(
            [
                {
                    "datetime": dates.convert_epoch_time_in_ms_to_iso(forecast["createdTime"]),
                    "value": forecast["probAfter"],
                }
                for forecast in forecasts
                if forecast.get("isFilled")
            ]
        )
        if df.empty:
            return None

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values(by="datetime")
        df["date"] = df["datetime"].dt.date
        df = df[df["date"] <= yesterday]
        if df.empty:
            return None

        df = df.groupby(by="date").last().reset_index()
        df = df[["date", "value"]]

        # --- Forward-fill missing dates ---
        date_range = pd.date_range(start=df["date"].min(), end=yesterday, freq="D")
        if market["isResolved"]:
            resolved_date = pd.Timestamp(market_info_resolution_datetime).date()
            df = df[df["date"] < resolved_date]
            df.loc[len(df)] = {
                "date": resolved_date,
                "value": self._get_resolved_market_value(market),
            }
            date_range = pd.date_range(start=df["date"].min(), end=resolved_date, freq="D")

        df_dates = pd.DataFrame(date_range, columns=["date"])
        df_dates["date"] = df_dates["date"].dt.date
        df = pd.merge(left=df_dates, right=df, on="date", how="left")

        if market["isResolved"]:
            # Don't forward-fill last row (could be NaN for CANCEL)
            df.iloc[:-1] = df.iloc[:-1].ffill()
        else:
            df = df.ffill()

        df["id"] = market_id
        return self._finalize_resolution_df(df)

    @staticmethod
    def _finalize_resolution_df(df: pd.DataFrame) -> DataFrame[ResolutionFrame]:
        """Filter to benchmark period and validate as ResolutionFrame."""
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.date >= constants.BENCHMARK_START_DATE_DATETIME_DATE]
        df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)
        return ResolutionFrame.validate(df)

    @staticmethod
    def _get_resolved_market_value(market: dict) -> float:
        """Map resolution outcome to numeric value.

        YES -> 1, NO -> 0, MKT -> market probability, CANCEL -> NaN
        """
        return {"YES": 1, "NO": 0, "MKT": market["resolutionProbability"]}.get(
            market["resolution"], np.nan
        )
