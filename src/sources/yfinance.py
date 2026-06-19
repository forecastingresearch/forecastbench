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
    required_metadata_keys: ClassVar[set[str]] = {"ticker_renames"}

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
        already in the question bank. Tickers that are still in the question pool but have dropped
        out of the S&P 500 and can no longer be fetched are marked resolved (delisted) using their
        existing question row.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
        """
        top_500 = self._get_sp500_tickers()
        set_top_500 = set(top_500)
        set_current = set(dfq["id"].unique()) if dfq is not None and "id" in dfq.columns else set()
        all_tickers = list(set_top_500 | set_current)

        logger.info(
            f"Stock tickers not in top 500 but in current stocks: {set_current - set_top_500}"
        )

        # Pin 'today' once for this run so all downstream date logic is consistent.
        self._today = dates.get_date_today()
        current_time = dates.get_datetime_now()

        rows = []
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
                        "probability": current_price,
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
                # Delisted: still in the question pool but no longer fetchable and out of the
                # S&P 500. Carry forward the existing question row, marked resolved.
                existing = dfq[dfq["id"] == ticker_symbol].iloc[0].to_dict()
                existing.update(
                    {
                        "resolved": True,
                        "fetch_datetime": current_time,
                        "probability": float("nan"),
                        "freeze_datetime_value": "N/A",
                    }
                )
                rows.append(existing)
                logger.warning(
                    f"{ticker_symbol} detected as delisted (not in S&P 500 and fetch failed)"
                )

        return pd.DataFrame(rows)

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

        for question in dff.to_dict("records"):
            question_id = str(question["id"])

            if question_id in renamed_tickers:
                # Resolution file is rebuilt from the replacement ticker below.
                logger.info(f"Skipping {question_id} (renamed ticker, handled separately)")
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
            del question["probability"]

            # Upsert into dfq
            if question["id"] in dfq["id"].values:
                dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
                for key, value in question.items():
                    dfq.at[dfq_index, key] = value
            else:
                new_q_row = pd.DataFrame([question])
                new_q_row = new_q_row.astype(constants.QUESTION_FILE_COLUMN_DTYPE)
                dfq = pd.concat([dfq, new_q_row], ignore_index=True)

        # Renamed tickers: fetch under the replacement, write under the original ticker.
        resolution_files.update(
            self._build_renamed_ticker_resolution_files(period, existing_resolution_files)
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
    ) -> dict[str, pd.DataFrame]:
        """Build resolution files for renamed tickers using their replacement symbols.

        For each entry in ``self.ticker_renames``, fetch price history under the replacement
        ticker and write it to a resolution file keyed by the original ticker.

        Args:
            period (str): yfinance period string.
            existing_resolution_files (dict): Existing resolution data, keyed by question id; must
                include the original tickers.

        Returns:
            Mapping of original ticker -> resolution DataFrame, only for files that changed.
        """
        resolution_files: dict[str, pd.DataFrame] = {}
        for entry in self.ticker_renames:
            original = entry["original_ticker"]
            replacement = entry["replacement_ticker"]

            existing_df = existing_resolution_files.get(original)

            df_new = self._get_historical_prices(existing_df, replacement, period)
            if df_new is None:
                logger.warning(
                    f"No data for replacement ticker {replacement} (original: {original})"
                )
                continue

            # df_new may be `existing_df` (fetch returned nothing); copy before relabelling so the
            # caller's resolution file is never mutated in place.
            df_new = df_new.copy()
            df_new["id"] = original

            if existing_df is not None and not existing_df.empty and existing_df.equals(df_new):
                continue

            logger.info(f"Built resolution file for {original} (via {replacement})")
            resolution_files[original] = df_new

        return resolution_files
