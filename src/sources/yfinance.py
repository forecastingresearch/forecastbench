"""Yahoo Finance question source."""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import ClassVar

import pandas as pd
import pandera.pandas as pa
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import QuestionFrame, ResolutionFrame, YfinanceFetchFrame
from helpers import constants, dates

from ._dataset import DatasetSource

logger = logging.getLogger(__name__)


class YfinanceSource(DatasetSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"
    additional_required_metadata_keys: ClassVar[set[str]] = {"ticker_renames"}

    # Pinned at the start of fetch()/update() so every downstream helper (via self.get_date_today())
    # observes one consistent date for the whole run, even if it straddles midnight.
    _today: date | None = None

    def get_date_today(self) -> date:
        """Return the date pinned for this run, or the live date if none is pinned.

        fetch() and update() pin ``self._today`` once at the start; downstream helpers call this
        instead of ``dates.get_date_today()`` so they all see the same date.
        """
        return self._today if self._today is not None else dates.get_date_today()

    # ------------------------------------------------------------------
    # Public: fetch
    # ------------------------------------------------------------------

    @pa.check_types
    def fetch(
        self,
        *,
        dfq: DataFrame[QuestionFrame] | None = None,
    ) -> DataFrame[YfinanceFetchFrame]:
        """Fetch S&P 500 stock data from Yahoo Finance.

        The ticker universe is the union of the current S&P 500 constituents and any tickers
        already in the question bank, minus the tickers that are known to 404 on every run:
        curated nullified (known-delisted) tickers and renamed originals (whose data is served
        under their replacement symbol). Those are never fetched; the noise would only hide
        genuinely-new delistings. Any of them still in the pool are carried forward as resolved
        using their existing question row. Tickers that are still in the pool, have dropped out of
        the S&P 500, and can no longer be fetched (but are not yet curated) are likewise marked
        resolved.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
        """
        top_500 = self._get_sp500_tickers()
        set_top_500 = set(top_500)
        set_current = set(dfq["id"].unique()) if dfq is not None and "id" in dfq.columns else set()

        # Tickers we never fetch because they 404 on every run and only add log noise that masks
        # genuinely-new delistings: curated nullified (known-delisted) tickers, and renamed
        # originals (their price data is served under the replacement symbol; update() rebuilds
        # their resolution file from it). Drop both from the universe up front and carry their
        # existing question rows forward as resolved.
        nullified_ids = self.get_nullified_ids()
        renamed_original_ids = {entry["original_ticker"] for entry in self.ticker_renames}
        skip_fetch_ids = nullified_ids | renamed_original_ids
        all_tickers = list((set_top_500 | set_current) - skip_fetch_ids)

        nullified_in_pool = sorted(set_current & nullified_ids)
        renamed_in_pool = sorted(set_current & renamed_original_ids)
        carry_forward_ids = sorted(set_current & skip_fetch_ids)

        logger.info(
            "Stock tickers not in top 500 but in current stocks (excluding known-unfetchable): "
            f"{set_current - set_top_500 - skip_fetch_ids}"
        )
        if nullified_in_pool:
            logger.info(
                f"Skipping fetch for {len(nullified_in_pool)} known-delisted (nullified) tickers; "
                f"carrying them forward as resolved: {nullified_in_pool}"
            )
        if renamed_in_pool:
            logger.info(
                f"Skipping fetch for {len(renamed_in_pool)} renamed-original tickers (data comes "
                f"via their replacement); carrying them forward as resolved: {renamed_in_pool}"
            )

        # Pin 'today' once for this run so all downstream date logic is consistent.
        self._today = dates.get_date_today()
        current_time = dates.get_datetime_now()

        rows = []

        # Carry forward known-unfetchable tickers (nullified + renamed originals) without the API.
        for ticker_symbol in carry_forward_ids:
            rows.append(self._carry_forward_resolved(ticker_symbol, dfq, current_time))

        for ticker_symbol in all_tickers:
            time.sleep(1)  # Avoid YFRateLimitError
            company_name, hist = self._fetch_one_stock(ticker_symbol)

            if company_name and not hist.empty:
                current_price = round(hist["Close"].iloc[-1], 2)
                background = yf.Ticker(ticker_symbol).info.get("longBusinessSummary", "N/A")
                rows.append(
                    {
                        "id": ticker_symbol,
                        "question": (
                            f"Will {ticker_symbol}'s market close price on "
                            "{resolution_date} be higher than its market close price on "
                            "{forecast_due_date}?\n\n"
                            "Stock splits and reverse splits will be accounted for in resolving "
                            "this question. Forecasts on questions about companies that have been "
                            "delisted (through mergers or bankruptcy) will resolve to their final "
                            "close price."
                        ),
                        "background": background,
                        "market_info_resolution_criteria": "N/A",
                        "market_info_open_datetime": "N/A",
                        "market_info_close_datetime": "N/A",
                        "url": f"https://finance.yahoo.com/quote/{ticker_symbol}",
                        "resolved": False,
                        "market_info_resolution_datetime": "N/A",
                        "fetch_datetime": current_time,
                        "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                        "freeze_datetime_value": current_price,
                        "freeze_datetime_value_explanation": (
                            f"The latest market close price of {ticker_symbol}."
                        ),
                    }
                )
                logger.info(company_name)
            elif (
                company_name is None
                and ticker_symbol in set_current
                and ticker_symbol not in set_top_500
            ):
                # Newly delisted: still in the question pool but no longer fetchable and out of
                # the S&P 500, yet not in the curated nullified list. Carry the existing question
                # row forward as resolved and warn so it can be added to nullified_questions.
                rows.append(self._carry_forward_resolved(ticker_symbol, dfq, current_time))
                logger.warning(
                    f"{ticker_symbol} detected as delisted (not in S&P 500 and fetch failed); "
                    "consider adding it to nullified_questions"
                )

        return pd.DataFrame(rows)

    @staticmethod
    def _carry_forward_resolved(ticker_symbol: str, dfq: pd.DataFrame, current_time: str) -> dict:
        """Return a delisted ticker's existing question row, marked resolved.

        Shared by the curated-nullified skip (before fetch) and the runtime delisted heuristic
        (fetch returned nothing). freeze_datetime_value gets the delisted marker.

        Args:
            ticker_symbol (str): Ticker whose existing question row to carry forward.
            dfq (pd.DataFrame): Existing question bank (must contain ``ticker_symbol``).
            current_time (str): Fetch timestamp to stamp on the carried-forward row.
        """
        existing = dfq[dfq["id"] == ticker_symbol].iloc[0].to_dict()
        existing.update(
            {
                "resolved": True,
                "fetch_datetime": current_time,
                "freeze_datetime_value": "N/A",
            }
        )
        return existing

    # ------------------------------------------------------------------
    # Public: update
    # ------------------------------------------------------------------

    @pa.check_types
    def update(
        self,
        dfq: DataFrame[QuestionFrame],
        dff: DataFrame[YfinanceFetchFrame],
        *,
        existing_resolution_files: dict[str, DataFrame[ResolutionFrame]] | None = None,
        overwrite_price_history: bool = False,
    ) -> UpdateResult:
        """Process fetched stock data into updated questions and resolution files.

        Curated nullified (known-delisted) tickers are never sent to the API here either — they
        404 forever and their final close is fixed — so their resolution files are forward-filled
        from existing data instead of re-fetched.

        Args:
            dfq (DataFrame[QuestionFrame]): Existing questions.
            dff (DataFrame[YfinanceFetchFrame]): Freshly fetched data.
            existing_resolution_files (dict | None): Per-question existing resolution data. Must
                include any renamed-ticker originals so their files can be refreshed.
            overwrite_price_history (bool): If True, re-fetch all resolution data even if a file is
                already up-to-date.
        """
        existing_resolution_files = existing_resolution_files or {}
        resolution_files: dict[str, pd.DataFrame] = {}

        # Pin 'today' once for this run so all downstream date logic is consistent.
        self._today = dates.get_date_today()
        period = self._select_time_range(
            (self._today - constants.QUESTION_BANK_DATA_STORAGE_START_DATE).days
        )

        renamed_tickers = {entry["original_ticker"] for entry in self.ticker_renames}
        nullified_ids = self.get_nullified_ids()

        for question in dff.to_dict("records"):
            question_id = str(question["id"])

            if question_id in renamed_tickers:
                # Resolution file is rebuilt from the replacement ticker below.
                logger.info(f"Skipping {question_id} (renamed ticker, handled separately)")
            elif question_id in nullified_ids:
                # Known-delisted (nullified): never hit the API (it 404s). The final close is
                # fixed, so just forward-fill the existing resolution file to yesterday so that
                # newly-arriving resolution dates still find an exact-date row.
                df_res = self._forward_fill_existing(existing_resolution_files.get(question_id))
                if df_res is not None:
                    resolution_files[question_id] = df_res
            else:
                df_res = self._build_resolution_df(
                    question=question,
                    period=period,
                    existing_df=existing_resolution_files.get(question_id),
                    force=overwrite_price_history,
                )
                if df_res is not None:
                    resolution_files[question_id] = df_res

            # Strip transient fetch-only fields (not part of QuestionFrame)
            del question["fetch_datetime"]

            # Upsert into dfq
            if question["id"] in dfq["id"].values:
                dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
                for key, value in question.items():
                    dfq.at[dfq_index, key] = value
            else:
                new_q_row = pd.DataFrame([question])
                new_q_row = new_q_row.astype(constants.QUESTION_FILE_COLUMN_DTYPE)
                dfq = pd.concat([dfq, new_q_row], ignore_index=True)

        # Renamed tickers: write the original ticker's file as a copy of the replacement's
        # already-built (or existing) series so the two files are identical by construction.
        resolution_files.update(
            self._build_renamed_ticker_resolution_files(
                period, existing_resolution_files, resolution_files
            )
        )

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: S&P 500 tickers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_sp500_tickers() -> list[str]:
        """Scrape S&P 500 constituent tickers from Wikipedia."""
        try:
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            headers = {"User-Agent": constants.BENCHMARK_USER_AGENT}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", {"id": "constituents"})
            tickers = [row.find_all("td")[0].text.strip() for row in table.find_all("tr")[1:]]
            logger.info(f"Retrieved S&P 500 stock tickers: {len(tickers)} tickers")
            return tickers
        except Exception as e:
            logger.error(f"Failed to retrieve stock tickers due to: {e}")
            return []

    # ------------------------------------------------------------------
    # Private: single stock fetch
    # ------------------------------------------------------------------

    def _fetch_one_stock(self, ticker_symbol: str) -> tuple[str | None, pd.DataFrame | None]:
        """Fetch company name and the latest historical row for one ticker.

        Args:
            ticker_symbol (str): Stock ticker symbol.

        Returns:
            Tuple of (company_name, hist_df) or (None, None) on failure.
        """
        try:
            ticker = yf.Ticker(ticker_symbol)
            company_name = ticker.info["longName"]
            hist = ticker.history(period="5d", auto_adjust=False).reset_index()
            yesterday = self.get_date_today() - timedelta(days=1)
            hist["Date"] = pd.to_datetime(hist["Date"])
            hist = hist[hist["Date"].dt.date <= yesterday].tail(1)
            return company_name, hist
        except Exception:
            return None, None

    # ------------------------------------------------------------------
    # Private: resolution file building
    # ------------------------------------------------------------------

    @staticmethod
    def _select_time_range(days_difference: int) -> str:
        """Map days since data storage start to a yfinance period parameter.

        Possible time ranges in:
        ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']

        Args:
            days_difference (int): Days since QUESTION_BANK_DATA_STORAGE_START_DATE.
        """
        if days_difference <= 1:
            return "1d"
        elif days_difference <= 5:
            return "5d"
        elif days_difference <= 30:
            return "1mo"
        elif days_difference <= 90:
            return "3mo"
        elif days_difference <= 180:
            return "6mo"
        elif days_difference <= 365:
            return "1y"
        elif days_difference <= 365 * 2:
            return "2y"
        elif days_difference <= 365 * 5:
            return "5y"
        elif days_difference <= 365 * 10:
            return "10y"
        else:
            return "max"

    @staticmethod
    def _fetch_historical_prices(ticker_symbol: str, period: str) -> pd.DataFrame:
        """Fetch historical closing prices for a ticker.

        Args:
            ticker_symbol (str): Stock ticker symbol.
            period (str): yfinance period string.

        Returns:
            DataFrame with columns [date, value], or an empty DataFrame on failure.
        """
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period=period, auto_adjust=False)
            return hist[["Close"]].reset_index().rename(columns={"Date": "date", "Close": "value"})
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker_symbol}: {e}")
            return pd.DataFrame()

    def _get_historical_prices(
        self,
        existing_df: pd.DataFrame | None,
        ticker_symbol: str,
        period: str,
    ) -> pd.DataFrame | None:
        """Build a resolution DataFrame of daily prices for a ticker.

        Args:
            existing_df (pd.DataFrame | None): Existing resolution data, used as the fallback when
                the fetch returns nothing.
            ticker_symbol (str): Stock ticker symbol.
            period (str): yfinance period string.

        Returns:
            DataFrame with columns [id, date, value]; the existing data unchanged when the fetch
            returns nothing; or None when the fetch returns nothing and there is no existing data.
        """
        df = self._fetch_historical_prices(ticker_symbol, period)
        if df.empty:
            # Fetch returned nothing: keep the existing file unchanged; if there is none, there is
            # nothing to write.
            if existing_df is None or existing_df.empty:
                return None
            return existing_df

        yesterday = self.get_date_today() - timedelta(days=1)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[
            (df["date"] >= constants.QUESTION_BANK_DATA_STORAGE_START_DATE)
            & (df["date"] <= yesterday)
        ]

        # Forward fill for weekends/holidays
        full_date_range = pd.date_range(start=df["date"].min(), end=yesterday)
        df = df.set_index("date").reindex(full_date_range).ffill().rename_axis("date").reset_index()
        df["id"] = ticker_symbol
        return df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    def _finalize_resolution_file(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forward-fill a resolved ticker's resolution file to yesterday.

        Args:
            df (pd.DataFrame): Resolution data with columns [id, date, value].

        Returns:
            DataFrame forward-filled through yesterday.
        """
        if df.empty:
            return df

        end_date = self.get_date_today() - timedelta(days=1)

        df = df.copy()
        ticker_id = df["id"].iloc[0]
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        full_range = pd.date_range(start=df.index.min(), end=end_date)
        df = df.reindex(full_range).ffill().rename_axis("date").reset_index()
        df["id"] = ticker_id

        return df[["id", "date", "value"]].astype(dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE)

    def _forward_fill_existing(self, existing_df: pd.DataFrame | None) -> pd.DataFrame | None:
        """Forward-fill an existing resolution file to yesterday without fetching.

        For curated nullified (known-delisted) tickers, whose price is fixed and which 404 on the
        API. Mirrors the resolved path of ``_build_resolution_df`` minus the doomed request.

        Args:
            existing_df (pd.DataFrame | None): Existing resolution data, or None.

        Returns:
            The forward-filled DataFrame, or None when there is no existing file or nothing changed.
        """
        if existing_df is None or existing_df.empty:
            return None
        df_new = self._finalize_resolution_file(existing_df)
        if existing_df.equals(df_new):
            return None
        return df_new

    def _build_resolution_df(
        self,
        question: dict,
        period: str,
        existing_df: DataFrame[ResolutionFrame] | None = None,
        force: bool = False,
    ) -> DataFrame[ResolutionFrame] | None:
        """Build or update a resolution file for a single stock ticker.

        Args:
            question (dict): Must have 'id'; 'resolved' marks a delisted ticker.
            period (str): yfinance period string.
            existing_df (DataFrame[ResolutionFrame] | None): Existing resolution data.
            force (bool): If True, re-fetch even when the file is already up-to-date.

        Returns:
            The updated DataFrame, or None when no upload is needed (already up-to-date or
            unchanged).
        """
        is_resolved = question.get("resolved", False)
        yesterday = self.get_date_today() - timedelta(days=1)

        # Already up-to-date check — skip the API call entirely. Resolved (delisted) tickers are
        # always rebuilt so the final close price is forward-filled.
        if (
            not force
            and not is_resolved
            and existing_df is not None
            and not existing_df.empty
            and pd.to_datetime(existing_df["date"].iloc[-1]).date() >= yesterday
        ):
            logger.info(f"{question['id']} is skipped because it's already up-to-date!")
            return None

        df_new = self._get_historical_prices(existing_df, question["id"], period)
        if df_new is None:
            return None

        if is_resolved:
            df_new = self._finalize_resolution_file(df_new)

        # Only upload dataframes that changed.
        if existing_df is not None and not existing_df.empty and existing_df.equals(df_new):
            return None

        return df_new

    def _build_renamed_ticker_resolution_files(
        self,
        period: str,
        existing_resolution_files: dict[str, pd.DataFrame],
        built_resolution_files: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """Build resolution files for renamed tickers as a copy of their replacement's series.

        For each entry in ``self.ticker_renames``, the original ticker's resolution file is written
        as a relabelled copy of the **replacement's** series, so the two files are identical by
        construction (no second fetch that could diverge). The authoritative replacement series is
        "what ``<replacement>.jsonl`` will be after this run": the freshly-built frame if the
        replacement was built in this run's main loop, else the existing on-disk file. Only when the
        replacement isn't in this run's pool at all do we fetch it directly (there is then no
        ``<replacement>.jsonl`` to diverge from).

        Args:
            period (str): yfinance period string.
            existing_resolution_files (dict): Existing resolution data, keyed by question id; must
                include the original tickers (and the replacements, when known).
            built_resolution_files (dict): Resolution frames already built this run (the main
                loop's output), keyed by question id.

        Returns:
            Mapping of original ticker -> resolution DataFrame, only for files that changed.
        """
        resolution_files: dict[str, pd.DataFrame] = {}
        for entry in self.ticker_renames:
            original = entry["original_ticker"]
            replacement = entry["replacement_ticker"]

            existing_df = existing_resolution_files.get(original)

            # Authoritative replacement series: freshly built this run, else the on-disk file,
            # else fetch directly (replacement not in this run's pool, so nothing to diverge from).
            repl_series = built_resolution_files.get(replacement)
            if repl_series is None:
                repl_series = existing_resolution_files.get(replacement)
            if repl_series is None:
                repl_series = self._get_historical_prices(existing_df, replacement, period)

            if repl_series is None or repl_series.empty:
                logger.warning(
                    f"No data for replacement ticker {replacement} (original: {original})"
                )
                continue

            # Copy before relabelling so neither the built map nor the caller's file is mutated.
            df_new = repl_series.copy()
            df_new["id"] = original

            if existing_df is not None and not existing_df.empty and existing_df.equals(df_new):
                continue

            logger.info(f"Built resolution file for {original} as a copy of {replacement}")
            resolution_files[original] = df_new

        return resolution_files
