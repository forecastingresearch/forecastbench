"""Polymarket question source."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, ClassVar

import backoff
import certifi
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import PolymarketFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates

from ._market import MarketSource

logger = logging.getLogger(__name__)

_GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"
_CLOB_API_URL = "https://clob.polymarket.com/prices-history"
_MIN_MARKET_LIQUIDITY = 25000

# Set CHECK_AND_FIX_RESOLVED_DATA=1 to re-fetch resolved questions whose resolution files are
# missing or have non-contiguous dates. This needs every resolved file downloaded, so it's costly
# and off by default (matches the legacy job behaviour).


class ConditionIdMarketNotFoundError(ValueError):
    """Raised when the Gamma API cannot find a market for one condition ID."""

    def __init__(self, condition_id):
        """Initialize the error with the condition ID that could not be fetched."""
        self.condition_id = condition_id
        super().__init__(f"Problem getting market for condition id {condition_id}.")


class FailedConditionIdsError(ValueError):
    """Raised when one or more unresolved condition IDs cannot be fetched."""

    def __init__(self, condition_ids):
        """Initialize the error with the complete failed condition ID list."""
        self.condition_ids = list(condition_ids)
        super().__init__(
            "Problem getting markets for condition ids: "
            f"{json.dumps(self.condition_ids, indent=2)}"
        )


class PolymarketSource(MarketSource):
    """Polymarket prediction market source."""

    name: ClassVar[str] = "polymarket"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(
        self,
        *,
        dfq: DataFrame[QuestionFrame] | None = None,
        existing_resolution_files: dict[str, pd.DataFrame] | None = None,
    ) -> DataFrame[PolymarketFetchFrame]:
        """Fetch questions from the Polymarket API.

        Fetches new active binary markets and re-fetches existing unresolved markets, embedding
        each market's daily price history in the returned rows for ``update()`` to write.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
            existing_resolution_files (dict | None): Per-question resolution data, only consulted
                when ``CHECK_AND_FIX_RESOLVED_DATA`` is set, to detect resolved questions with
                missing/incomplete resolution files.
        """
        existing_resolution_files = existing_resolution_files or {}
        today = dates.get_date_today()
        fetch_datetime = dates.get_datetime_now()

        # --- Fetch new active markets (price history attached per market) ---
        all_new_markets = self._fetch_active_markets_from_api()
        all_newly_fetched_ids = {m["conditionId"] for m in all_new_markets}
        logger.info(f"Number of newly fetched questions: {len(all_newly_fetched_ids)}")

        # --- Determine existing unresolved questions to re-fetch ---
        unresolved_ids: set[str] = set()
        resolved_ids: set[str] = set()
        if dfq is not None and not dfq.empty:
            resolved_ids = set(dfq.loc[dfq["resolved"], "id"])
            unresolved_ids = set(dfq.loc[~dfq["resolved"], "id"])

        # Optional, costly: re-fetch resolved questions whose resolution files are missing or have
        # non-contiguous dates. Off by default. Mirrors the legacy CHECK_AND_FIX_RESOLVED_DATA path.
        if os.environ.get("CHECK_AND_FIX_RESOLVED_DATA"):
            for mid in resolved_ids:
                dfr = existing_resolution_files.get(mid)
                if dfr is None or dfr.empty:
                    unresolved_ids.add(mid)
                    continue
                dfr_sorted = dfr.copy()
                dfr_sorted["date"] = pd.to_datetime(dfr_sorted["date"])
                dfr_sorted = dfr_sorted.sort_values(by="date")
                date_diff = dfr_sorted["date"].diff().dt.days
                if not date_diff.iloc[1:].eq(1).all():
                    unresolved_ids.add(mid)

        logger.info(f"Number of unresolved questions in dfq: {len(unresolved_ids)}")

        unresolved_ids.difference_update(all_newly_fetched_ids)
        unresolved_ids = list(unresolved_ids)
        logger.info(f"Total (removing duplicates): {len(unresolved_ids)}")

        # --- Re-fetch existing unresolved markets ---
        all_existing_unresolved_markets: list[dict] = []
        invalid_ids: set[str] = set(self.get_nullified_ids())
        failed_condition_ids: list[str] = []
        for id_ in unresolved_ids:
            time.sleep(0.05)
            if id_ in invalid_ids:
                continue
            try:
                market = self._get_market(id_)
            except ConditionIdMarketNotFoundError:
                failed_condition_ids.append(id_)
                logger.warning(f"Skipping unresolved condition id {id_}.")
                continue
            if not self._is_market_binary(market):
                # Questions that were not Yes/No questions should be marked as resolved/closed so
                # they're not selected in question sets.
                invalid_ids.add(market["conditionId"])
                market["closed"] = True

            price_history = self._fetch_price_history(self._get_yes_token(market))
            if price_history is None:
                logger.error(f"PRICE HISTORY was NONE for {market['slug']}")
                # Add dummy entry with NaN value for last possible date so we don't accidentally
                # resolve these qusetions later or pull from them for the question set. Use today
                # because we remove one day from the converted_price_history in the next loop.
                price_history = [{"p": np.nan, "t": dates.convert_iso_date_to_epoch_time(today)}]

            market["price_history"] = price_history
            all_existing_unresolved_markets.append(market)

        if failed_condition_ids:
            raise FailedConditionIdsError(sorted(failed_condition_ids))

        logger.info("Finished fetching unresolved questions!")

        # Handle invalid questions.
        # Set all values in the price history to np.nan because it should never be resolved if they
        # were already included in a question set.
        if invalid_ids:
            logger.warning(f"Invalid questions found: {invalid_ids}")
            for market in all_existing_unresolved_markets:
                if market["conditionId"] in invalid_ids:
                    for item in market["price_history"]:
                        item["p"] = np.nan

        # --- Combine and transform ---
        all_markets = all_new_markets + all_existing_unresolved_markets
        all_markets = [m for m in all_markets if m["price_history"]]

        rows = []
        for market in all_markets:
            row = self._transform_question(market, fetch_datetime, invalid_ids)
            if row is not None:
                rows.append(row)

        logger.info(f"Fetched {len(rows)} questions.")
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[PolymarketFetchFrame],
        **kwargs: Any,
    ) -> UpdateResult:
        """Process fetched data into updated questions and resolution files.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[PolymarketFetchFrame]): Freshly fetched data.
        """
        resolution_files: dict[str, pd.DataFrame] = {}

        for question in dff.to_dict("records"):
            question_id = str(question["id"])

            # Build the resolution file from the embedded historical prices.
            resolution_files[question_id] = self._build_resolution_df(question)

            # Strip transient fields (not part of QuestionFrame).
            del question["fetch_datetime"]
            del question["probability"]
            del question["historical_prices"]

            # Upsert into dfq.
            if question["id"] in dfq["id"].values:
                dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
                for key, value in question.items():
                    dfq.at[dfq_index, key] = value
            else:
                dfq = pd.concat([dfq, pd.DataFrame([question])], ignore_index=True)

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: API calls
    # ------------------------------------------------------------------

    def _fetch_active_markets_from_api(self) -> list[dict]:
        """Fetch active binary markets from the Gamma API with price history attached.

        Paginates through all active, non-archived, non-closed markets ordered by liquidity,
        keeps binary markets with sufficient liquidity that aren't catch-all ("other") markets,
        and attaches each qualifying market's price history.
        """
        all_markets: list[dict] = []
        offset = 0
        limit = 500  # max page size: 500
        n_markets_fetched = 0

        params: dict[str, Any] = {
            "limit": limit,
            "archived": False,
            "active": True,
            "closed": False,
            "order": "liquidity",
            "ascending": False,
        }

        while True:
            params["offset"] = offset
            try:
                logger.info(f"Fetching markets with offset {offset}.")
                response = requests.get(_GAMMA_API_URL, params=params)
                response.raise_for_status()
                markets = response.json()
                if not markets:
                    logger.info(
                        f"Fetched total of {n_markets_fetched} markets, "
                        f"{len(all_markets)} satisfy criteria."
                    )
                    break

                n_markets_fetched += len(markets)
                for market in markets:
                    binary_market = self._is_market_binary(market)
                    # Avoids questions like the following, which don't make sense without the other
                    # questions in the event:
                    # * Will any other Republican Politician win the popular vote in the 2024
                    #   Presidential Election?
                    catch_all_market = "other" in market["slug"]  # no need to test "another" also
                    liquid_market = (
                        "liquidityNum" in market.keys()
                        and market["liquidityNum"] > _MIN_MARKET_LIQUIDITY
                    )
                    if binary_market and liquid_market and not catch_all_market:
                        price_history = self._fetch_price_history(self._get_yes_token(market))
                        if price_history is not None:
                            logger.info(
                                "Binary question satisfying criteria: "
                                f"https://polymarket.com/market/{market['slug']}"
                            )
                            market["price_history"] = price_history
                            all_markets.append(market)

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching markets: {e}")
                break

            time.sleep(1)
            offset += limit

        return all_markets

    def _get_market(self, condition_id: str) -> dict:
        """Fetch a single market by condition ID, trying open then closed markets.

        The Gamma API defaults to open markets only, so retry with ``closed=True`` when the open
        query returns no unique match.

        Args:
            condition_id (str): The market's condition ID.

        Raises:
            ConditionIdMarketNotFoundError: If no single market is found either way.
        """
        for params_market in [
            {"condition_ids": condition_id, "closed": False},
            {"condition_ids": condition_id, "closed": True},
        ]:
            response = requests.get(_GAMMA_API_URL, params=params_market)
            response.raise_for_status()
            markets = response.json()
            if len(markets) == 1:
                return markets[0]
        logger.error(f"Problem getting market for condition id {condition_id}.")
        raise ConditionIdMarketNotFoundError(condition_id)

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=20,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _fetch_price_history(self, market_id: str) -> list[dict] | None:
        """Retrieve the price history for a market token from the CLOB API.

        Note: the Polymarket API only provides history for roughly the last 6 months.

        Args:
            market_id (str): The CLOB token ID for the "Yes" outcome.
        """
        time.sleep(0.02)
        logger.info(f"Getting price history for {market_id}...")

        params = {
            "interval": "max",
            "market": market_id,
            "fidelity": 1440,
            "startTs": constants.BENCHMARK_START_DATE_EPOCHTIME,
        }

        try:
            response = requests.get(_CLOB_API_URL, params=params, verify=certifi.where())
            if not response.ok:
                logger.error(
                    f"Request to endpoint failed for {_CLOB_API_URL}: "
                    f"{response.status_code} {response.reason}. "
                    f"Headers: {response.headers}. "
                    f"Elapsed time: {response.elapsed}."
                )
                response.raise_for_status()

            return response.json().get("history", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch price history for market {market_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # Private: market helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_market_binary(market: dict) -> bool:
        """Return True if the market has exactly Yes/No outcomes."""
        return {s.lower() for s in json.loads(market["outcomes"])} == {"yes", "no"}

    @staticmethod
    def _get_yes_index(market: dict) -> int:
        """Return the index of the 'Yes' outcome."""
        return 0 if json.loads(market["outcomes"])[0].lower() == "yes" else 1

    @staticmethod
    def _get_yes_token(market: dict) -> str:
        """Return the CLOB token ID for the 'Yes' outcome."""
        yes_token_index = PolymarketSource._get_yes_index(market)
        return json.loads(market["clobTokenIds"])[yes_token_index]

    # ------------------------------------------------------------------
    # Private: price history helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_first_midnight_only(price_history: list[dict]) -> list[dict]:
        """Remove duplicate dates, keeping only the first value per day."""
        unique_dates: dict[str, dict] = {}
        for record in price_history:
            date_only = record["date"].split("T")[0]  # Extract the date part (YYYY-MM-DD)
            if date_only not in unique_dates:
                unique_dates[date_only] = record  # Keep the first occurrence of the date
        return list(unique_dates.values())

    @staticmethod
    def _subtract_one_day(price_history: list[dict]) -> list[dict]:
        """Subtract one day from all dates in the price history."""
        for record in price_history:
            record_datetime = datetime.fromisoformat(record["date"])
            record_datetime -= timedelta(days=1)
            record["date"] = record_datetime.isoformat()
        return price_history

    # ------------------------------------------------------------------
    # Private: question transformation
    # ------------------------------------------------------------------

    @staticmethod
    def _transform_question(
        market: dict,
        fetch_datetime: str,
        invalid_ids: set[str],
    ) -> dict | None:
        """Transform a raw Polymarket market (with price history) into a fetch-frame row.

        Builds the daily resolution series (epoch->ISO, dedupe, subtract one day, forward-fill),
        applies resolution logic (UMA oracle dates, resolved outcome prices), and returns the row.
        Returns None when the market is missing both ``endDate`` and ``events``.

        Args:
            market (dict): Raw Gamma API market with a ``price_history`` list attached.
            fetch_datetime (str): ISO timestamp recorded against each fetched row.
            invalid_ids (set[str]): Condition IDs (nullified + non-binary) whose resolution branch
                must be skipped so their values stay NaN.
        """
        price_history = market["price_history"]

        converted_price_history = [
            {"date": dates.convert_epoch_time_in_sec_to_iso(r["t"]), "value": r["p"]}
            for r in price_history
        ]
        converted_price_history = PolymarketSource._filter_first_midnight_only(
            converted_price_history
        )
        converted_price_history = PolymarketSource._subtract_one_day(converted_price_history)

        final_resolutions_df = pd.DataFrame(converted_price_history)
        final_resolutions_df["date"] = pd.to_datetime(final_resolutions_df["date"].str[:10])

        # Reindex to fill in missing dates including weekends.
        all_dates = pd.date_range(
            start=final_resolutions_df["date"].min(),
            end=final_resolutions_df["date"].max(),
            freq="D",
        )
        final_resolutions_df = (
            final_resolutions_df.set_index("date").reindex(all_dates, method="ffill").reset_index()
        )
        final_resolutions_df.rename(columns={"index": "date"}, inplace=True)
        final_resolutions_df = final_resolutions_df[["date", "value"]]
        final_resolutions_df["date"] = pd.to_datetime(final_resolutions_df["date"])

        current_prob = price_history[-1]["p"] if len(price_history) > 1 else np.nan
        resolved_datetime = resolved_datetime_str = "N/A"

        try:
            end_date = market["endDate"] if "endDate" in market else market["events"][0]["endDate"]
        except KeyError:
            # endDate unexpectedly missing from:
            # https://polymarket.com/event/will-trump-meet-with-khamenei-before-august
            return None
        market_closed_datetime_str = dates.convert_zulu_to_iso(end_date)
        market_closed_datetime = datetime.fromisoformat(market_closed_datetime_str).replace(
            tzinfo=None
        )

        use_uma_date = False
        if market.get("umaEndDate"):
            # UMA Oracle
            uma_datetime_str = dates.convert_zulu_to_iso(market["umaEndDate"])
            uma_datetime = datetime.fromisoformat(uma_datetime_str).replace(tzinfo=None)
            use_uma_date = uma_datetime < market_closed_datetime

        resolved_datetime_str = uma_datetime_str if use_uma_date else market_closed_datetime_str
        resolved_datetime = uma_datetime if use_uma_date else market_closed_datetime
        resolved_datetime = resolved_datetime.replace(hour=0, minute=0, second=0)

        # Get the resolution if the question is closed (but not if it's invalid so we maintain the
        # NaN values above)
        resolved = market.get("umaResolutionStatus", "") == "resolved"
        if resolved and market["conditionId"] not in invalid_ids:
            yes_index = PolymarketSource._get_yes_index(market)
            current_prob = float(json.loads(market["outcomePrices"])[yes_index])

            # Insert the resolution value on the resolved date. Truncate all data after that date.
            # Forward fill data until that date

            # Truncate any data after resolved_datetime
            final_resolutions_df = final_resolutions_df[
                final_resolutions_df["date"].dt.date <= resolved_datetime.date()
            ]

            # Insert resolved date and resolution value
            if resolved_datetime.date() in final_resolutions_df["date"].dt.date.values:
                final_resolutions_df.loc[
                    final_resolutions_df["date"].dt.date == resolved_datetime.date(), "value"
                ] = current_prob
            else:
                # If the date does not exist, add a new row
                final_resolutions_df.loc[len(final_resolutions_df)] = [
                    resolved_datetime,
                    current_prob,
                ]

            # Forward fill in case the resolution date is more than one day after the last day
            # for which data is available
            all_dates = pd.date_range(
                start=final_resolutions_df["date"].min(),
                end=final_resolutions_df["date"].max(),
                freq="D",
            )
            final_resolutions_df = (
                final_resolutions_df.set_index("date")
                .reindex(all_dates, method="ffill")
                .reset_index()
                .rename(columns={"index": "date"})
            )

        final_resolutions_df = final_resolutions_df[["date", "value"]]
        final_resolutions_df["date"] = final_resolutions_df["date"].astype(str)

        return {
            "id": market["conditionId"],
            "question": market["question"],
            "background": market["description"],
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": market["startDateIso"],
            "market_info_close_datetime": market_closed_datetime_str,
            "url": "https://polymarket.com/market/" + market["slug"],
            "resolved": resolved,
            "market_info_resolution_datetime": resolved_datetime_str,
            "fetch_datetime": fetch_datetime,
            "probability": "N/A" if np.isnan(current_prob) else current_prob,
            "forecast_horizons": "N/A",
            "freeze_datetime_value": "N/A" if np.isnan(current_prob) else current_prob,
            "freeze_datetime_value_explanation": "The market price.",
            "historical_prices": final_resolutions_df.to_dict(orient="records"),
        }

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    def _build_resolution_df(self, question: dict) -> DataFrame[ResolutionFrame]:
        """Build a resolution file from a fetched question's embedded historical prices.

        Args:
            question (dict): A PolymarketFetchFrame row with a ``historical_prices`` list.
        """
        df = pd.DataFrame(question["historical_prices"])
        df["id"] = question["id"]
        df = df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)
        return ResolutionFrame.validate(df)
