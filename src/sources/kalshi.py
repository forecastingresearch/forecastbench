"""Kalshi question source."""

import logging
import time
from collections import Counter
from datetime import date, timedelta
from typing import Any, ClassVar, TypedDict

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

# Kalshi's basic read tier currently permits 20 ordinary GET requests per second. Keep a
# conservative 10 requests/second ceiling so retries and small provider-side timing differences
# have headroom.
_MIN_REQUEST_INTERVAL = 0.1
_CANDLESTICK_PERIOD_INTERVAL = 60  # hourly candlesticks for UTC day-end values
_MAX_CANDLESTICKS_PER_REQUEST = 5000
# Kalshi counts both ends of the requested range. Keep each window one period below the limit,
# then advance by one second so consecutive windows cannot omit or duplicate a boundary.
_MAX_CANDLESTICK_RANGE_SECONDS = (
    (_MAX_CANDLESTICKS_PER_REQUEST - 1) * _CANDLESTICK_PERIOD_INTERVAL * 60
)
_POST_CLOSE_STATUSES = {"closed", "determined", "disputed", "amended", "finalized"}
_RESOLVED_STATUSES = {"finalized"}
_SETTLEMENT_SOURCE_PREFIX = "Outcome verified from "


class _SettlementSource(TypedDict):
    """Kalshi event-level source used to verify the outcome."""

    name: str | None
    url: str


class _DiscoveredMarket(TypedDict):
    """Kalshi fields retained from event discovery."""

    category: str
    event_ticker: str
    needs_yes_label: bool
    series_ticker: str
    settlement_sources: list[_SettlementSource]


class MarketNotFoundError(Exception):
    """Raised when a Kalshi market is absent from both live and historical APIs.

    Kalshi documents short-lived 404s while newly created markets propagate. API methods therefore
    retry this exception before ``update()`` treats the market as unavailable for the current run.
    """

    def __init__(self, ticker: str):
        """Initialize the error with the ticker that could not be found."""
        self.ticker = ticker
        super().__init__(f"Kalshi market not found for ticker {ticker}.")


class KalshiSource(MarketSource):
    """Kalshi prediction market source."""

    name: ClassVar[str] = "kalshi"

    def __init__(self) -> None:
        """Initialize request-throttling state."""
        super().__init__()
        self._last_request_time = 0.0

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
        discovered_markets = self._search_markets(
            min_resolution_date=min_resolution_date,
            max_resolution_date=max_resolution_date,
        )
        logger.info(
            f"Discovered {len(discovered_markets)} candidate market tickers across all categories."
        )
        ids = self._balance_categories(
            {ticker: metadata["category"] for ticker, metadata in discovered_markets.items()}
        )
        logger.info(
            f"Kept {len(ids)} tickers after applying the per-category cap of {_MAX_PER_CATEGORY}."
        )
        rows = [
            {
                "id": ticker,
                "event_ticker": discovered_markets[ticker]["event_ticker"],
                "needs_yes_label": discovered_markets[ticker]["needs_yes_label"],
                "series_ticker": discovered_markets[ticker]["series_ticker"],
                "settlement_sources": discovered_markets[ticker]["settlement_sources"],
            }
            for ticker in sorted(ids)
        ]
        return pd.DataFrame(
            rows,
            columns=[
                "id",
                "event_ticker",
                "needs_yes_label",
                "series_ticker",
                "settlement_sources",
            ],
        )

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
        persisted_dfq = dfq.copy(deep=True)
        persisted_ids = set(persisted_dfq["id"].astype(str))
        nullified_ids = self.get_nullified_ids()
        if nullified_ids:
            nullified_mask = dfq["id"].astype(str).isin(nullified_ids)
            dfq.loc[nullified_mask, "resolved"] = True
            dfq.loc[nullified_mask, "freeze_datetime_value"] = "N/A"
        resolution_files: dict[str, pd.DataFrame] = {}
        not_found_ids: set[str] = set()
        update_datetime = dates.get_datetime_today()
        candlesticks_end_ts = int(update_datetime.timestamp())
        yesterday = update_datetime.date() - timedelta(days=1)
        routing_by_id = {str(row["id"]): row for row in dff.to_dict(orient="records")}

        # --- Append new tickers from dff to dfq (capped to keep the pool bounded) ---
        newly_added_ids: set[str] = set()
        new_ids = dff[~dff["id"].isin(dfq["id"]) & ~dff["id"].astype(str).isin(nullified_ids)]["id"]
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

        # Fetch each unresolved market's current details once.
        dfq["resolved"] = dfq["resolved"].astype(bool)
        unresolved_rows = list(dfq[~dfq["resolved"]].iterrows())
        market_details: dict[str, dict] = {}
        for _index, row in unresolved_rows:
            question_id = str(row["id"])
            try:
                market_details[question_id] = self._get_market(question_id)
            except MarketNotFoundError:
                not_found_ids.add(question_id)

        # --- Update all unresolved questions ---
        for index, row in unresolved_rows:
            question_id = str(row["id"])
            market = market_details.get(question_id)
            if market is None:
                continue

            resolution_window = self._resolution_window(market)
            if resolution_window is None:
                if question_id in newly_added_ids:
                    dfq = dfq.drop(index=index)
                    action = "Dropped new"
                else:
                    dfq.at[index, "freeze_datetime_value"] = "N/A"
                    action = "Quarantined existing"
                logger.warning(
                    f"{action} Kalshi market {question_id} because its resolution window "
                    "is invalid."
                )
                continue
            earliest_resolution_time, _ = resolution_window

            # Assign market details to dfq row
            routing = routing_by_id.get(question_id)
            include_yes_label = (
                routing is not None and bool(routing["needs_yes_label"])
            ) or " [Yes: " in str(row["question"])
            dfq.at[index, "question"] = self._question_text(
                market, include_yes_label=include_yes_label
            )
            dfq.at[index, "background"] = "N/A"
            settlement_sources = routing["settlement_sources"] if routing is not None else None
            dfq.at[index, "market_info_resolution_criteria"] = self._resolution_criteria(
                market,
                settlement_sources=settlement_sources,
                existing_criteria=row["market_info_resolution_criteria"],
            )
            dfq.at[index, "market_info_open_datetime"] = dates.convert_zulu_to_iso(
                market["open_time"]
            )
            dfq.at[index, "market_info_close_datetime"] = dates.convert_zulu_to_iso(
                earliest_resolution_time
            )
            if routing is not None:
                dfq.at[index, "url"] = self._market_url(
                    routing["series_ticker"], routing["event_ticker"]
                )
            if self._is_resolved(market):
                dfq.at[index, "resolved"] = True
                dfq.at[index, "market_info_resolution_datetime"] = self._resolution_datetime(market)
            dfq.at[index, "forecast_horizons"] = "N/A"

            # Build resolution file
            existing_df = existing_resolution_files.get(row["id"])
            try:
                df_res = self._build_resolution_df(
                    market=market,
                    market_info_resolution_datetime=dfq.at[
                        index, "market_info_resolution_datetime"
                    ],
                    candlesticks_end_ts=candlesticks_end_ts,
                    yesterday=yesterday,
                    existing_df=existing_df,
                )
            except MarketNotFoundError:
                not_found_ids.add(str(row["id"]))
                continue
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
            question_id = str(row["id"])
            if question_id in nullified_ids:
                continue
            if question_id not in existing_resolution_ids and row["id"] not in resolution_files:
                try:
                    market = self._get_market(row["id"])
                except MarketNotFoundError:
                    not_found_ids.add(question_id)
                    continue
                try:
                    df_res = self._build_resolution_df(
                        market=market,
                        market_info_resolution_datetime=row["market_info_resolution_datetime"],
                        candlesticks_end_ts=candlesticks_end_ts,
                        yesterday=yesterday,
                        existing_df=None,
                    )
                except MarketNotFoundError:
                    not_found_ids.add(question_id)
                    continue
                if df_res is not None:
                    resolution_files[row["id"]] = df_res
                else:
                    logger.warning(
                        f"No resolution file built for resolved id={row['id']} "
                        "(no candlesticks / no usable price data)."
                    )

        # Drop brand-new tickers that were absent from both APIs before they were ever populated.
        # Persisted questions are restored after any partial refresh and quarantined from curation
        # with an N/A price; because they remain unresolved, a later nightly run can recover them.
        orphan_ids = newly_added_ids.intersection(not_found_ids)
        if orphan_ids:
            dfq = dfq[~dfq["id"].isin(orphan_ids)].reset_index(drop=True)

        persisted_not_found_ids = persisted_ids.intersection(not_found_ids)
        for question_id in persisted_not_found_ids:
            current_index = dfq.index[dfq["id"].astype(str) == question_id][0]
            persisted_index = persisted_dfq.index[persisted_dfq["id"].astype(str) == question_id][0]
            dfq.loc[current_index, :] = persisted_dfq.loc[persisted_index, :]
            dfq.at[current_index, "freeze_datetime_value"] = "N/A"

        if not_found_ids:
            logger.warning(
                f"{len(not_found_ids)} question(s) were absent from both live and historical APIs "
                f"after retries: {sorted(not_found_ids)}. Dropped {len(orphan_ids)} never-populated "
                f"new ticker(s) and quarantined {len(persisted_not_found_ids)} persisted "
                "question(s) with an N/A price."
            )

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: request throttling
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep if needed to keep consecutive Kalshi requests under the read limit."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

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
    ) -> tuple[dict[str, _DiscoveredMarket], str | None]:
        """Fetch one page of open events (with nested markets) and return qualifying tickers.

        Returns each qualifying market's parent category and routing identifiers so fetch() can
        balance the pool and retain enough information to build its public Kalshi URL.
        """
        endpoint = f"{_KALSHI_API_BASE}/events"
        params: dict[str, Any] = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        self._throttle()
        response = requests.get(endpoint, params=params, verify=certifi.where())
        if not response.ok:
            logger.error(
                f"Request to endpoint failed for {endpoint}: {response.status_code} Error. "
                f"{response.text}"
            )
            response.raise_for_status()

        data = response.json()
        try:
            events = data["events"]
            cursor = data["cursor"]
            if events == [None] and not cursor:
                return {}, None

            discovered_markets: dict[str, _DiscoveredMarket] = {}
            for event in events:
                if event is None:
                    raise ValueError("Kalshi events API response contains a null event.")
                category = event.get("category") or "Uncategorized"
                event_ticker = event["event_ticker"]
                series_ticker = event["series_ticker"]
                title_counts = Counter()
                for market in event["markets"]:
                    if market["event_ticker"] != event_ticker:
                        continue
                    if "title" not in market and not self._market_qualifies(
                        market,
                        min_resolution_date=min_resolution_date,
                        max_resolution_date=max_resolution_date,
                    ):
                        continue
                    title = market["title"]
                    if not isinstance(title, str) or not title.strip():
                        if self._market_qualifies(
                            market,
                            min_resolution_date=min_resolution_date,
                            max_resolution_date=max_resolution_date,
                        ):
                            raise ValueError(
                                "Kalshi events API response is missing required field 'title'."
                            )
                        continue
                    title_counts[title.strip().casefold()] += 1
                qualifying_markets = []
                for market in event["markets"]:
                    if market["event_ticker"] != event_ticker:
                        logger.warning(
                            f"Skipping Kalshi market {market['ticker']} because its event_ticker "
                            f"does not match parent event {event_ticker}."
                        )
                        continue
                    if self._market_qualifies(
                        market,
                        min_resolution_date=min_resolution_date,
                        max_resolution_date=max_resolution_date,
                    ):
                        qualifying_markets.append(market)
                if not qualifying_markets:
                    continue
                settlement_sources = [
                    {"name": source.get("name"), "url": source["url"]}
                    for source in event["settlement_sources"]
                ]
                for market in qualifying_markets:
                    discovered_markets[market["ticker"]] = {
                        "category": category,
                        "event_ticker": event_ticker,
                        "needs_yes_label": (title_counts[market["title"].strip().casefold()] > 1),
                        "series_ticker": series_ticker,
                        "settlement_sources": settlement_sources,
                    }
            return discovered_markets, cursor
        except KeyError as error:
            raise ValueError(
                f"Kalshi events API response is missing required field {error.args[0]!r}."
            ) from error

    def _search_markets(
        self,
        *,
        min_resolution_date: date,
        max_resolution_date: date | None = None,
    ) -> dict[str, _DiscoveredMarket]:
        """Discover market tickers and parent metadata by paginating all open events."""
        logger.info("Calling Kalshi events endpoint")
        discovered_markets: dict[str, _DiscoveredMarket] = {}
        cursor: str | None = None
        while True:
            page, cursor = self._call_search_endpoint(
                min_resolution_date=min_resolution_date,
                max_resolution_date=max_resolution_date,
                cursor=cursor,
            )
            discovered_markets.update(page)
            if not cursor:
                break
        return discovered_markets

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

        A market qualifies when it is active, binary, sufficiently liquid (volume and open
        interest), its earliest credible resolution is on or after ``min_resolution_date``, and
        its latest resolution bound is (when set) no later than ``max_resolution_date``. Category
        is not a criterion -- every category is eligible and the pool is balanced across
        categories afterwards in fetch().
        """
        if market["status"] != "active":
            return False
        if market["market_type"] != "binary":
            return False
        if float(market["volume_fp"]) < _MIN_VOLUME:
            return False
        if float(market["open_interest_fp"]) < _MIN_OPEN_INTEREST:
            return False
        resolution_window = KalshiSource._resolution_window(market)
        if resolution_window is None:
            return False
        earliest_resolution_time, latest_resolution_time = resolution_window
        earliest_resolution_date = dates.convert_zulu_to_datetime(earliest_resolution_time).date()
        latest_resolution_date = dates.convert_zulu_to_datetime(latest_resolution_time).date()
        if earliest_resolution_date < min_resolution_date:
            return False
        if max_resolution_date is not None and latest_resolution_date > max_resolution_date:
            return False
        return True

    # ------------------------------------------------------------------
    # Private: market detail API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        MarketNotFoundError,
        max_tries=3,
        on_backoff=data_utils.print_error_info_handler,
    )
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
        """Fetch market details from the live API, falling back to historical storage."""
        logger.info(f"Calling market endpoint for {ticker}")
        endpoint = f"{_KALSHI_API_BASE}/markets/{ticker}"
        self._throttle()
        response = requests.get(endpoint, verify=certifi.where())
        if response.status_code == 404:
            endpoint = f"{_KALSHI_API_BASE}/historical/markets/{ticker}"
            self._throttle()
            response = requests.get(endpoint, verify=certifi.where())
            if response.status_code == 404:
                logger.warning(f"Market {ticker} was absent from both live and historical APIs.")
                raise MarketNotFoundError(ticker)
        if not response.ok:
            logger.error(f"Request to market endpoint failed for {ticker}.")
            response.raise_for_status()
        return response.json()["market"]

    # ------------------------------------------------------------------
    # Private: candlesticks API
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        MarketNotFoundError,
        max_tries=3,
        on_backoff=data_utils.print_error_info_handler,
    )
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_time=200,
        max_tries=10,
        factor=2,
        base=2,
        on_backoff=data_utils.print_error_info_handler,
    )
    def _get_market_candlesticks(
        self,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
    ) -> list[dict]:
        """Fetch hourly candles in bounded windows, with historical fallback."""
        logger.info(f"Calling candlesticks endpoint for {ticker}")
        series_ticker = self._series_ticker(ticker)
        historical = False
        candles_by_end_ts: dict[int, dict] = {}
        window_start = start_ts

        while window_start <= end_ts:
            window_end = min(window_start + _MAX_CANDLESTICK_RANGE_SECONDS, end_ts)
            if historical:
                endpoint = f"{_KALSHI_API_BASE}/historical/markets/{ticker}/candlesticks"
            else:
                endpoint = (
                    f"{_KALSHI_API_BASE}/series/{series_ticker}/markets/{ticker}/candlesticks"
                )
            params: dict[str, Any] = {
                "start_ts": window_start,
                "end_ts": window_end,
                "period_interval": _CANDLESTICK_PERIOD_INTERVAL,
            }
            self._throttle()
            response = requests.get(endpoint, params=params, verify=certifi.where())
            if not historical and response.status_code == 404:
                historical = True
                endpoint = f"{_KALSHI_API_BASE}/historical/markets/{ticker}/candlesticks"
                self._throttle()
                response = requests.get(endpoint, params=params, verify=certifi.where())
            if response.status_code == 404:
                logger.warning(
                    f"Candlesticks for {ticker} were absent from both live and historical APIs."
                )
                raise MarketNotFoundError(ticker)
            if not response.ok:
                logger.error(f"Request to candlesticks endpoint failed for {ticker}.")
                response.raise_for_status()
            for candle in response.json().get("candlesticks", []):
                normalized_candle = candle.copy()
                normalized_price = candle.get("price", {}).copy()
                if "close_dollars" not in normalized_price and "close" in normalized_price:
                    normalized_price["close_dollars"] = normalized_price["close"]
                normalized_candle["price"] = normalized_price
                candles_by_end_ts[normalized_candle["end_period_ts"]] = normalized_candle

            if window_end == end_ts:
                break
            window_start = window_end + 1

        return [candles_by_end_ts[end_period_ts] for end_period_ts in sorted(candles_by_end_ts)]

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    def _build_resolution_df(
        self,
        market: dict,
        market_info_resolution_datetime: str,
        *,
        candlesticks_end_ts: int,
        yesterday: date,
        existing_df: pd.DataFrame | None = None,
    ) -> DataFrame[ResolutionFrame] | None:
        """Build or update a resolution file for a single market."""
        ticker = market["ticker"]
        resolved = self._is_resolved(market)
        existing_last_date: date | None = None

        # --- Already up-to-date check ---
        if existing_df is not None and not existing_df.empty:
            existing_last_date = pd.to_datetime(existing_df["date"].max()).date()
            if resolved:
                resolved_date = pd.Timestamp(market_info_resolution_datetime).date()
                if existing_last_date >= resolved_date:
                    # An unresolved run may already have stored a probability on the eventual
                    # settlement date. Replace it (and any later rows) with the terminal result.
                    df = existing_df.copy()
                    df["date"] = pd.to_datetime(df["date"])
                    df = df[df["date"].dt.date < resolved_date].reset_index(drop=True)
                    df.loc[len(df)] = {
                        "id": ticker,
                        "date": pd.Timestamp(resolved_date),
                        "value": self._get_resolved_market_value(market),
                    }
                    df = df[["id", "date", "value"]].astype(
                        dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE
                    )
                    if df.equals(existing_df):
                        return existing_df
                    return df
            elif existing_last_date >= yesterday:
                return existing_df

        # --- Fetch hourly candlesticks and build a UTC end-of-day series ---
        candlesticks_start_ts = max(
            constants.BENCHMARK_START_DATE_EPOCHTIME,
            int(dates.convert_zulu_to_datetime(market["open_time"]).timestamp()),
        )
        if existing_last_date is not None:
            candlesticks_start_ts = max(
                candlesticks_start_ts,
                dates.convert_iso_date_to_epoch_time(existing_last_date + timedelta(days=1)),
            )
        candles = self._get_market_candlesticks(
            ticker,
            start_ts=candlesticks_start_ts,
            end_ts=candlesticks_end_ts,
        )
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
        if not df.empty:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df = df.sort_values(by="datetime")

            # A stored date D represents the last market price at or before D+1 00:00 UTC. Build
            # those boundaries explicitly so a later rebuild cannot assign post-deadline trading
            # to D. An exact-boundary hourly candle is eligible because it closes the prior hour.
            first_boundary = df["datetime"].min().ceil("D")
            last_boundary = pd.Timestamp(yesterday + timedelta(days=1), tz="UTC")
            if first_boundary <= last_boundary:
                boundaries = pd.DataFrame(
                    {"boundary": pd.date_range(first_boundary, last_boundary, freq="D")}
                )
                df = pd.merge_asof(
                    boundaries,
                    df,
                    left_on="boundary",
                    right_on="datetime",
                    direction="backward",
                    allow_exact_matches=True,
                )
                df["date"] = df["boundary"].dt.date.apply(
                    lambda boundary_date: boundary_date - timedelta(days=1)
                )
                df = df[["date", "value"]]
            else:
                df = pd.DataFrame(columns=["date", "value"])
        else:
            df = pd.DataFrame(columns=["date", "value"])

        if existing_df is not None and not existing_df.empty:
            existing = existing_df[["date", "value"]].copy()
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            df = pd.concat([existing, df], ignore_index=True)
            df = (
                df.drop_duplicates(subset="date", keep="last")
                .sort_values(by="date")
                .reset_index(drop=True)
            )
        if df.empty:
            return None

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
    def _resolution_window(market: dict) -> tuple[str, str] | None:
        """Return the credible earliest and latest resolution timestamps.

        Kalshi's close time can be a late postponement bound rather than the time the outcome is
        expected to become known. Once a market has closed, its actual close and occurrence times
        supersede scheduling estimates that Kalshi may leave unchanged after an early settlement.
        Timestamp keys needed for the market's lifecycle state are accessed directly so absent API
        fields still fail loudly; present but unusable or inconsistent timestamps make the market
        ineligible.
        """
        post_close = market.get("status") in _POST_CLOSE_STATUSES
        timestamps = {"close": market["close_time"]}
        if not post_close:
            timestamps["expected"] = market["expected_expiration_time"]
            timestamps["latest"] = market["latest_expiration_time"]
        occurrence_datetime = market.get("occurrence_datetime")
        if not all(
            isinstance(timestamp, str) and timestamp.strip() for timestamp in timestamps.values()
        ):
            return None
        timestamps = {name: timestamp.strip() for name, timestamp in timestamps.items()}
        if occurrence_datetime is not None:
            if not isinstance(occurrence_datetime, str):
                return None
            if occurrence_datetime.strip():
                timestamps["occurrence"] = occurrence_datetime.strip()

        try:
            parsed_datetimes = {
                name: dates.convert_zulu_to_datetime(timestamp)
                for name, timestamp in timestamps.items()
            }
        except (TypeError, ValueError):
            return None
        if any(
            parsed_datetime.utcoffset() is None for parsed_datetime in parsed_datetimes.values()
        ):
            return None

        earliest_fields = ["close"]
        if "occurrence" in parsed_datetimes:
            earliest_fields.append("occurrence")

        if post_close:
            earliest_field = min(earliest_fields, key=parsed_datetimes.__getitem__)
            latest_field = max(earliest_fields, key=parsed_datetimes.__getitem__)
            return timestamps[earliest_field], timestamps[latest_field]

        earliest_fields.append("expected")
        earliest_field = min(earliest_fields, key=parsed_datetimes.__getitem__)
        latest_field = max(("close", "latest"), key=parsed_datetimes.__getitem__)
        latest_resolution_datetime = parsed_datetimes[latest_field]
        if parsed_datetimes["expected"] > latest_resolution_datetime:
            return None
        if (
            parsed_datetimes.get("occurrence", latest_resolution_datetime)
            > latest_resolution_datetime
        ):
            return None

        return timestamps[earliest_field], timestamps[latest_field]

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
    def _question_text(market: dict, *, include_yes_label: bool) -> str:
        """Add the child Yes label only when sibling market titles repeat."""
        if include_yes_label:
            return f'{market["title"]} [Yes: {market["yes_sub_title"]}]'
        return market["title"]

    @staticmethod
    def _market_url(series_ticker: str, event_ticker: str) -> str:
        """Build a public Kalshi URL from structured parent identifiers."""
        return f"https://kalshi.com/markets/{series_ticker.lower()}/x/{event_ticker.lower()}"

    @staticmethod
    def _resolution_criteria(
        market: dict,
        *,
        settlement_sources: list[_SettlementSource] | None = None,
        existing_criteria: object | None = None,
    ) -> str:
        """Join market rules and event-level outcome verification sources."""
        parts = [market.get("rules_primary"), market.get("rules_secondary")]
        parts = [part for part in parts if part]

        if settlement_sources is not None:
            formatted_sources = [
                f'{source["name"]} ({source["url"]})' if source.get("name") else source["url"]
                for source in settlement_sources
            ]
            if formatted_sources:
                parts.append(f"{_SETTLEMENT_SOURCE_PREFIX}{'; '.join(formatted_sources)}.")
        elif isinstance(existing_criteria, str):
            source_start = existing_criteria.rfind(_SETTLEMENT_SOURCE_PREFIX)
            if source_start >= 0:
                parts.append(existing_criteria[source_start:])

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
