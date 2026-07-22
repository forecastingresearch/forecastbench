"""Kalshi question source."""

import logging
import time
from datetime import date, timedelta
from typing import Any, ClassVar

import backoff
import certifi
import numpy as np
import pandas as pd
import pandera.pandas as pa
import requests
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import KalshiFetchFrame, QuestionFrame, ResolutionFrame
from helpers import constants, data_utils, dates, question_curation

from ._market import MarketSource

logger = logging.getLogger(__name__)

_KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Liquidity floors. Any binary market that clears these and resolves within the window qualifies,
# regardless of its event category (Kalshi exposes ~16 categories). The thresholds are calibrated
# so the all-category pool approximates Polymarket's active question count.
_MIN_VOLUME = 10_000
_MIN_OPEN_INTEREST = 1000
_MAX_RESOLUTION_DATE_IN_DAYS = 365 * 2
_QUESTION_LIMIT = 5000

# Per-category cap applied in fetch(). Kalshi's liquid universe is dominated by a few high-volume
# categories (notably Sports, ~50% of the pool), so without a cap the pool would be flooded by one
# category. Capping each category keeps the discovered pool representative across all categories
# while smaller categories are taken in full.
_MAX_PER_CATEGORY = 1200

_CANDLESTICK_PERIOD_INTERVAL = 1440  # daily candlesticks
_RESOLVED_STATUSES = {"finalized"}


class MarketNotFoundError(Exception):
    """Raised when a Kalshi market ticker no longer exists (404).

    Deliberately not a ``requests.exceptions.RequestException`` so the ``@backoff`` retry on
    ``_get_market`` does not waste retries on a permanently-missing market, and so ``update()`` can
    skip the question instead of crashing the whole job.
    """

    def __init__(self, ticker: str):
        """Initialize the error with the ticker that could not be found."""
        self.ticker = ticker
        super().__init__(f"Kalshi market not found for ticker {ticker}.")


class KalshiSource(MarketSource):
    """Kalshi prediction market source."""

    name: ClassVar[str] = "kalshi"

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(
        self,
        *,
        today: date | None = None,
        **kwargs: Any,
    ) -> DataFrame[KalshiFetchFrame]:
        """Discover eligible Kalshi market tickers via the events endpoint.

        Paginates open events, keeping every liquid binary market that resolves within the window
        from the freeze window to ``_MAX_RESOLUTION_DATE_IN_DAYS`` out, in any category. The
        discovered pool is then balanced across categories (``_balance_categories``) so a few
        high-volume categories do not dominate. The upper date bound keeps the pool to markets
        that actually resolve, rather than the perpetual novelty markets (closing decades out) that
        otherwise clear the liquidity floors on cumulative volume.

        Args:
            today (date | None): Reference date for the min/max resolution dates. Defaults to
                today, computed once here and threaded through so every page shares the same
                reference instead of each recomputing "today".
        """
        if today is None:
            today = dates.get_date_today()
        min_resolution_date = today + timedelta(days=question_curation.FREEZE_WINDOW_IN_DAYS)
        max_resolution_date = today + timedelta(days=_MAX_RESOLUTION_DATE_IN_DAYS)
        ticker_categories = self._search_markets(
            min_resolution_date=min_resolution_date,
            max_resolution_date=max_resolution_date,
        )
        logger.info(
            f"Discovered {len(ticker_categories)} candidate market tickers across all categories."
        )
        ids = self._balance_categories(ticker_categories)
        logger.info(
            f"Kept {len(ids)} tickers after applying the per-category cap of {_MAX_PER_CATEGORY}."
        )
        return pd.DataFrame({"id": sorted(ids)})

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[KalshiFetchFrame],
        *,
        existing_resolution_files: dict[str, pd.DataFrame] | None = None,
        existing_resolution_ids: set[str] | None = None,
    ) -> UpdateResult:
        """Process fetched tickers into updated questions and resolution files.

        For each new ticker in dff, appends to dfq. Then for each unresolved question, fetches
        market details and builds/updates resolution files. Finally regenerates missing resolution
        files for resolved questions.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[KalshiFetchFrame]): Freshly fetched market tickers.
            existing_resolution_files (dict | None): Per-question existing resolution data.
            existing_resolution_ids (set[str] | None): Bare IDs that already have a resolution
                file in storage.
        """
        existing_resolution_files = existing_resolution_files or {}
        existing_resolution_ids = existing_resolution_ids or set()
        resolution_files: dict[str, pd.DataFrame] = {}
        not_found_ids: list[str] = []

        # --- Append new tickers from dff to dfq (capped to keep the pool bounded) ---
        newly_added_ids: set[str] = set()
        new_ids = dff[~dff["id"].isin(dfq["id"])]["id"]
        if not new_ids.empty:
            df_new = pd.DataFrame({"id": new_ids}).assign(
                **{col: None for col in dfq.columns if col != "id"}
            )
            df_new["resolved"] = False
            df_new["freeze_datetime_value_explanation"] = "The market price."
            df_new["market_info_resolution_datetime"] = "N/A"

            # Cap new additions so the unresolved pool stays under _QUESTION_LIMIT
            max_to_add = _QUESTION_LIMIT - len(dfq[dfq["resolved"] == False])  # noqa: E712
            if max_to_add > 0:
                # Random sample (not head()) when the cap binds, so the alphabetically-first
                # tickers aren't systematically favoured (fetch() sorts ids). fetch() already
                # balances categories, so a uniform sample here preserves that balance.
                if len(df_new) > max_to_add:
                    df_new = df_new.sample(n=max_to_add)
                # Track which tickers are brand-new this run: the append above seeds them with None
                # placeholders that the loop below fills in. Any that 404 before being populated must
                # be dropped rather than persisted (see the cleanup after the loops).
                newly_added_ids = set(df_new["id"].astype(str))
                dfq = pd.concat([dfq, df_new], ignore_index=True)

        # --- Update all unresolved questions ---
        dfq["resolved"] = dfq["resolved"].astype(bool)
        for index, row in dfq[~dfq["resolved"]].iterrows():
            try:
                market = self._get_market(row["id"])
            except MarketNotFoundError:
                # Market was delisted; leave the row untouched and move on rather than crash.
                not_found_ids.append(str(row["id"]))
                continue

            # Assign market details to dfq row
            dfq.at[index, "question"] = market["title"]
            dfq.at[index, "background"] = "N/A"
            dfq.at[index, "market_info_resolution_criteria"] = self._resolution_criteria(market)
            dfq.at[index, "market_info_open_datetime"] = dates.convert_zulu_to_iso(
                market["open_time"]
            )
            dfq.at[index, "market_info_close_datetime"] = dates.convert_zulu_to_iso(
                market["close_time"]
            )
            dfq.at[index, "url"] = f"https://kalshi.com/markets/{market['ticker']}"
            if self._is_resolved(market):
                dfq.at[index, "resolved"] = True
                dfq.at[index, "market_info_resolution_datetime"] = self._resolution_datetime(market)
            dfq.at[index, "forecast_horizons"] = "N/A"

            # Build resolution file
            existing_df = existing_resolution_files.get(row["id"])
            df_res = self._build_resolution_df(
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
            else:
                logger.warning(
                    f"No resolution file built for id={row['id']} "
                    "(no candlesticks / no usable price data)."
                )

        # --- Regenerate missing resolution files for resolved questions ---
        for _index, row in dfq[dfq["resolved"]].iterrows():
            if str(row["id"]) not in existing_resolution_ids and row["id"] not in resolution_files:
                try:
                    market = self._get_market(row["id"])
                except MarketNotFoundError:
                    not_found_ids.append(str(row["id"]))
                    continue
                df_res = self._build_resolution_df(
                    market=market,
                    market_info_resolution_datetime=row["market_info_resolution_datetime"],
                    existing_df=None,
                )
                if df_res is not None:
                    resolution_files[row["id"]] = df_res
                else:
                    logger.warning(
                        f"No resolution file built for resolved id={row['id']} "
                        "(no candlesticks / no usable price data)."
                    )

        # Drop brand-new tickers that 404'd before they were ever populated. Their row still holds
        # the None placeholders seeded in the append step above, which would fail QuestionFrame's
        # non-nullable columns on the next run's input validation. Existing questions that 404 keep
        # their previously-fetched data (left untouched in the loops above).
        orphan_ids = newly_added_ids.intersection(not_found_ids)
        if orphan_ids:
            dfq = dfq[~dfq["id"].isin(orphan_ids)].reset_index(drop=True)

        if not_found_ids:
            logger.warning(
                f"{len(not_found_ids)} question(s) had a market that 404'd (delisted/removed): "
                f"{sorted(not_found_ids)}. Of these, dropped {len(orphan_ids)} never-populated "
                "new ticker(s); kept the rest with their prior data."
            )

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: events (search) API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=500,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _call_search_endpoint(
        self,
        *,
        min_resolution_date: date,
        max_resolution_date: date | None = None,
        cursor: str | None = None,
    ) -> tuple[dict[str, str], str | None]:
        """Fetch one page of open events (with nested markets) and return qualifying tickers.

        Returns a mapping of qualifying market ticker to its parent event's category (the category
        lives on the event, not the market) so fetch() can balance the pool across categories.
        """
        endpoint = f"{_KALSHI_API_BASE}/events"
        params: dict[str, Any] = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(
                f"Request to endpoint failed for {endpoint}: {response.status_code} Error. "
                f"{response.text}"
            )
            response.raise_for_status()

        data = response.json()
        ticker_categories: dict[str, str] = {}
        for event in data.get("events", []):
            category = event.get("category")
            for market in event.get("markets", []):
                if self._market_qualifies(
                    market,
                    min_resolution_date=min_resolution_date,
                    max_resolution_date=max_resolution_date,
                ):
                    ticker = market.get("ticker")
                    if ticker:
                        ticker_categories[ticker] = category
        return ticker_categories, data.get("cursor")

    def _search_markets(
        self,
        *,
        min_resolution_date: date,
        max_resolution_date: date | None = None,
    ) -> dict[str, str]:
        """Discover market tickers (mapped to their category) by paginating all open events."""
        logger.info("Calling Kalshi events endpoint")
        ticker_categories: dict[str, str] = {}
        cursor: str | None = None
        while True:
            page, cursor = self._call_search_endpoint(
                min_resolution_date=min_resolution_date,
                max_resolution_date=max_resolution_date,
                cursor=cursor,
            )
            ticker_categories.update(page)
            if not cursor:
                break
        return ticker_categories

    @staticmethod
    def _balance_categories(ticker_categories: dict[str, str]) -> list[str]:
        """Cap each category to ``_MAX_PER_CATEGORY`` tickers, sampling randomly within a category.

        Kalshi's liquid universe is dominated by a few high-volume categories (notably Sports), so
        without a cap the pool would be flooded by one category. Capping keeps the pool
        representative across all categories while smaller categories are taken in full. Sampling is
        random within a category (not by ticker name) so the selection is not alphabetically biased
        and rotates across nightly runs.

        Args:
            ticker_categories (dict[str, str]): Discovered market tickers mapped to their event
                category.
        """
        if not ticker_categories:
            return []
        df = pd.DataFrame(
            {"id": list(ticker_categories), "category": list(ticker_categories.values())}
        )
        df["category"] = df["category"].fillna("Uncategorized")
        kept = [
            group.sample(n=_MAX_PER_CATEGORY) if len(group) > _MAX_PER_CATEGORY else group
            for _, group in df.groupby("category")
        ]
        return pd.concat(kept, ignore_index=True)["id"].tolist()

    @staticmethod
    def _market_qualifies(
        market: dict,
        *,
        min_resolution_date: date,
        max_resolution_date: date | None = None,
    ) -> bool:
        """Return True if a market is a liquid binary market resolving within the target window.

        A market qualifies when it is binary, sufficiently liquid (volume and open interest), and
        closes on or after ``min_resolution_date`` and (when set) no later than
        ``max_resolution_date``. Category is not a criterion -- every category is eligible and the
        pool is balanced across categories afterwards in fetch(). The upper date bound excludes
        perpetual novelty markets (closing decades out) that pass the liquidity floors on
        cumulative volume.
        """
        # Use defensive `.get()` access throughout: a single market missing a field would
        # otherwise raise a KeyError that backoff does not catch (it is not a RequestException),
        # crashing the entire nightly discovery. Missing liquidity -> 0 -> excluded.
        if market.get("market_type") != "binary":
            return False
        if float(market.get("volume_fp") or 0) < _MIN_VOLUME:
            return False
        if float(market.get("open_interest_fp") or 0) < _MIN_OPEN_INTEREST:
            return False
        close_time = market.get("close_time")
        if not close_time:
            return False
        close_date = dates.convert_zulu_to_datetime(close_time).date()
        if close_date < min_resolution_date:
            return False
        if max_resolution_date is not None and close_date > max_resolution_date:
            return False
        return True

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
    def _get_market(self, ticker: str) -> dict:
        """Fetch full market details from /markets/{ticker}."""
        logger.info(f"Calling market endpoint for {ticker}")
        endpoint = f"{_KALSHI_API_BASE}/markets/{ticker}"
        response = requests.get(endpoint, verify=certifi.where())
        if response.status_code == 404:
            # Market was delisted/removed. Raise a non-retryable error so backoff stops and the
            # caller can skip this question rather than aborting the whole update.
            logger.warning(f"Market not found (404) for ticker {ticker}; skipping.")
            raise MarketNotFoundError(ticker)
        if not response.ok:
            logger.error(f"Request to market endpoint failed for {ticker}.")
            response.raise_for_status()
        time.sleep(0.1)
        return response.json()["market"]

    # ------------------------------------------------------------------
    # Private: candlesticks API
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
    def _get_market_candlesticks(self, ticker: str) -> list[dict]:
        """Fetch daily candlesticks for a market from the candlesticks endpoint."""
        logger.info(f"Calling candlesticks endpoint for {ticker}")
        series_ticker = self._series_ticker(ticker)
        endpoint = f"{_KALSHI_API_BASE}/series/{series_ticker}/markets/{ticker}/candlesticks"
        params: dict[str, Any] = {
            "start_ts": constants.BENCHMARK_START_DATE_EPOCHTIME,
            "end_ts": int(dates.get_datetime_today().timestamp()),
            "period_interval": _CANDLESTICK_PERIOD_INTERVAL,
        }
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(f"Request to candlesticks endpoint failed for {ticker}.")
            response.raise_for_status()
        time.sleep(0.1)
        return response.json().get("candlesticks", [])

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    def _build_resolution_df(
        self,
        market: dict,
        market_info_resolution_datetime: str,
        existing_df: pd.DataFrame | None = None,
    ) -> DataFrame[ResolutionFrame] | None:
        """Build or update a resolution file for a single market."""
        yesterday = dates.get_date_today() - timedelta(days=1)
        ticker = market["ticker"]
        resolved = self._is_resolved(market)

        # --- Already up-to-date check ---
        # If resolved: must extend through the resolution date (so the resolution row is present).
        # If unresolved: must extend through yesterday.
        if existing_df is not None and not existing_df.empty:
            last_date = pd.to_datetime(existing_df["date"].max()).date()
            cutoff = pd.Timestamp(market_info_resolution_datetime).date() if resolved else yesterday
            if last_date >= cutoff:
                return existing_df

        # --- Fetch candlesticks and build daily series ---
        candles = self._get_market_candlesticks(ticker)
        df = pd.DataFrame(
            [
                {
                    "datetime": dates.convert_epoch_time_in_sec_to_iso(candle["end_period_ts"]),
                    "value": float(candle["price"]["close_dollars"]),
                }
                for candle in candles
                if candle.get("price", {}).get("close_dollars") is not None
            ]
        )
        if df.empty:
            return None

        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values(by="datetime")
        # Kalshi daily candles are ET-anchored: end_period_ts lands at 04:00/05:00 UTC (midnight
        # ET). Subtract a day so the value for date D is the price at the end of day D, matching
        # the convention used by polymarket (_subtract_one_day) and manifold (end-of-day bets).
        df["date"] = (df["datetime"] - pd.Timedelta(days=1)).dt.date
        df = df[df["date"] <= yesterday]
        if df.empty:
            return None

        df = df.groupby(by="date").last().reset_index()
        df = df[["date", "value"]]

        # --- Forward-fill missing dates ---
        date_range = pd.date_range(start=df["date"].min(), end=yesterday, freq="D")
        if resolved:
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

        if resolved:
            # Don't forward-fill last row (could be NaN for a void/ambiguous resolution)
            df.iloc[:-1] = df.iloc[:-1].ffill()
        else:
            df = df.ffill()

        df["id"] = ticker
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.date >= constants.BENCHMARK_START_DATE_DATETIME_DATE]
        return df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    # ------------------------------------------------------------------
    # Private: market helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_resolved_market_value(market: dict) -> float:
        """Map resolution outcome to numeric value.

        yes -> 1, no -> 0, anything else (scalar, void) -> NaN
        """
        return {"yes": 1, "no": 0}.get(market.get("result", ""), np.nan)

    @staticmethod
    def _is_resolved(market: dict) -> bool:
        """Return True if the market has reached a terminal (resolved) status."""
        return market.get("status") in _RESOLVED_STATUSES

    @staticmethod
    def _resolution_criteria(market: dict) -> str:
        """Join the market's primary and secondary rules into a resolution criteria string."""
        parts = [market.get("rules_primary"), market.get("rules_secondary")]
        parts = [part for part in parts if part]
        return " ".join(parts) if parts else "N/A"

    @staticmethod
    def _resolution_datetime(market: dict) -> str:
        """Return the resolution datetime as ISO, preferring settlement over expiration/close."""
        ts = (
            market.get("settlement_ts")
            or market.get("expected_expiration_time")
            or market["close_time"]
        )
        return dates.convert_zulu_to_iso(ts)

    @staticmethod
    def _series_ticker(ticker: str) -> str:
        """Derive the Kalshi series ticker (the prefix before the first dash)."""
        return ticker.split("-")[0]
