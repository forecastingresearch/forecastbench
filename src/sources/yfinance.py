"""Yahoo Finance question source."""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import ClassVar

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from _fb_types import UpdateResult
from _schemas import (
    QuestionFrame,
    ResolutionFrame,
    ResolveReadyFrame,
    YfinanceFetchFrame,
)
from helpers import constants, dates

from ._dataset import DatasetSource
from .yfinance_questions import YFINANCE_PAIRS

# The yfinance, requests and bs4 libraries are imported inside the three methods that use them.
# The curation and baseline jobs reach this module through helpers/yfinance.py and their images
# do not ship those libraries.

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Pair questions: row text and freeze value
# ----------------------------------------------------------------------

_PAIR_SEPARATOR = "_"  # pool tickers never contain one, so it also marks a row as a pair

_PAIR_FREEZE_PERCENT_CHANGE_DAYS = 30

_PAIR_FREEZE_VALUE_EXPLANATION = (
    "The latest market close price of each stock, in US dollars, and its percent change over "
    f"the preceding {_PAIR_FREEZE_PERCENT_CHANGE_DAYS} days."
)


def _pair_question_text(x: str, y: str) -> str:
    """Return the question text for the two stocks, each labelled like "Apple Inc. (AAPL)".

    The ``{forecast_due_date}`` and ``{resolution_date}`` placeholders stay in the published text,
    as in the single-ticker questions; the question set lists the resolution dates separately.
    """
    return (
        f"Will {x} have a higher rate of return than {y} between {{forecast_due_date}} and "
        "{resolution_date}?\n\n"
        "A stock's rate of return is its market close price on the resolution date divided by its "
        "market close price on the forecast due date, minus one. If the market is closed on either "
        "date, the most recent prior market close price is used. Prices are adjusted for stock "
        "splits and reverse splits. Dividends are not included. If either stock is delisted (e.g., "
        "through acquisition, merger, or bankruptcy), the most recent market close price before "
        "delisting is used for all subsequent resolution dates."
    )


def _pair_background(x: str, x_summary: str, y: str, y_summary: str) -> str:
    """Return the background: each stock's business summary, led by its ticker."""
    return f"{x}: {x_summary}\n\n{y}: {y_summary}"


def _pair_url(x: str, y: str) -> str:
    """Return both Yahoo quote pages. The criteria template formats this into ``{url}``."""
    return f"https://finance.yahoo.com/quote/{x} and https://finance.yahoo.com/quote/{y}"


def _clean_series(series: pd.DataFrame | None) -> pd.DataFrame | None:
    """Return a [date, value] frame sorted by date with real dates and numbers, or None."""
    if series is None or series.empty:
        return None
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(series["date"]).dt.date,
            "value": pd.to_numeric(series["value"], errors="coerce"),
        }
    ).dropna(subset=["value"])
    if out.empty:
        return None
    return out.sort_values("date", ignore_index=True)


def _ticker_freeze_text(ticker: str, series: pd.DataFrame) -> str:
    """Return "AAPL: 254.23 (+3.1% over the last 30 days)", or just the price for short history."""
    last = series.iloc[-1]
    text = f"{ticker}: {last['value']:.2f}"
    window_start = last["date"] - timedelta(days=_PAIR_FREEZE_PERCENT_CHANGE_DAYS)
    earlier = series.loc[series["date"] == window_start, "value"]
    if not earlier.empty and earlier.iloc[0] != 0:
        change = (last["value"] / earlier.iloc[0] - 1) * 100
        text += f" ({change:+.1f}% over the last {_PAIR_FREEZE_PERCENT_CHANGE_DAYS} days)"
    return text


def _pair_freeze_value(
    x: str, x_series: pd.DataFrame | None, y: str, y_series: pd.DataFrame | None
) -> str:
    """Return the freeze string for a pair, or "N/A" when it cannot describe one moment.

    "N/A" when either ticker has no usable series, or when the two series end on different
    dates: a stale file next to a current one would quote closes from two different sessions
    with nothing in the text to say so. Curation skips a pair whose freeze value is "N/A".

    Args:
        x (str): First ticker.
        x_series (pd.DataFrame | None): First ticker's resolution series [id, date, value].
        y (str): Second ticker.
        y_series (pd.DataFrame | None): Second ticker's resolution series.
    """
    x_clean = _clean_series(x_series)
    y_clean = _clean_series(y_series)
    if x_clean is None or y_clean is None:
        return "N/A"
    if x_clean["date"].iloc[-1] != y_clean["date"].iloc[-1]:
        return "N/A"
    return f"{_ticker_freeze_text(x, x_clean)}; {_ticker_freeze_text(y, y_clean)}"


class YfinanceSource(DatasetSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"
    additional_required_metadata_keys: ClassVar[set[str]] = {"ticker_renames"}
    # The committed pair list. An instance attribute of the same name overrides it (tests).
    pairs: ClassVar[list[tuple[str, str]]] = YFINANCE_PAIRS

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

        The ticker universe is the tickers already in the question bank, plus the replacement
        symbols from ``ticker_renames``, minus the ones that are known to 404 on every run:
        curated nullified (known-delisted) tickers and renamed originals (whose data is served
        under their replacement symbol). Those are never fetched;
        the noise would only hide genuinely-new delistings. Any of them still in the pool are
        carried forward as resolved using their existing question row. Tickers that are still in
        the pool, have dropped out of the S&P 500, and can no longer be fetched (but are not yet
        curated) are likewise marked resolved.

        The pool grows only by replacement symbols; a company joining the S&P 500 is not made
        into a question. The single-ticker questions are retired from sampling (the pair
        questions built by ``_upsert_pair_questions`` are sampled instead), and the constituent
        list is read only to tell a delisting apart from a Yahoo outage below.

        Args:
            dfq (DataFrame[QuestionFrame] | None): Existing question bank.
        """
        top_500 = self._get_sp500_tickers()
        set_top_500 = set(top_500)
        set_current = set(dfq["id"].unique()) if dfq is not None and "id" in dfq.columns else set()
        # Pair question ids (X_Y) are not tickers. update() builds those rows from the two
        # tickers' resolution files; do not send them to Yahoo.
        set_current = {i for i in set_current if not self._is_pair_id(i)}

        # Tickers we never fetch because they 404 on every run and only add log noise that masks
        # genuinely-new delistings: curated nullified (known-delisted) tickers, and renamed
        # originals (their price data is served under the replacement symbol; update() rebuilds
        # their resolution file from it). Drop both from the universe up front and carry their
        # existing question rows forward as resolved.
        nullified_ids = self.get_nullified_ids()
        renamed_original_ids = {entry["original_ticker"] for entry in self.ticker_renames}
        skip_fetch_ids = nullified_ids | renamed_original_ids
        all_tickers = list(set_current - skip_fetch_ids)
        # Replacement symbols are fetched like pool tickers so the main loop builds their files;
        # the rename step then copies the series to the original's file. The skip set applies to
        # them too: a replacement that is later delisted or renamed again must stay unfetched.
        all_tickers = sorted(
            (set(all_tickers) | {entry["replacement_ticker"] for entry in self.ticker_renames})
            - skip_fetch_ids
        )

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
        # A ticker whose profile Yahoo did not serve tonight keeps the summary its bank row has.
        bank_backgrounds = dict(zip(dfq["id"], dfq["background"])) if dfq is not None else {}
        # Pooled tickers that 404 this run but aren't curated (neither nullified nor renamed).
        # Recorded so the driver can surface them (e.g. via Slack) for triage into the right list.
        self.uncurated_delisted_tickers: list[str] = []

        # Carry forward known-unfetchable tickers (nullified + renamed originals) without the API.
        for ticker_symbol in carry_forward_ids:
            rows.append(self._carry_forward_resolved(ticker_symbol, dfq, current_time))

        for ticker_symbol in all_tickers:
            time.sleep(1)  # Avoid YFRateLimitError
            company_name, business_summary, hist = self._fetch_one_stock(ticker_symbol)

            if company_name and not hist.empty:
                current_price = round(hist["Close"].iloc[-1], 2)
                background = business_summary or bank_backgrounds.get(ticker_symbol, "N/A")
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
                        "latest_close_date": str(hist["Date"].iloc[-1].date()),
                        "company_name": company_name,
                        "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                        "freeze_datetime_value": current_price,
                        "freeze_datetime_value_explanation": (
                            f"The latest market close price of {ticker_symbol}."
                        ),
                    }
                )
                logger.info(company_name)
            elif ticker_symbol in set_current and ticker_symbol not in set_top_500:
                # In the question pool, no usable price data (no name or no price history), and
                # out of the S&P 500, but not in any curated list: either newly delisted or newly
                # renamed. Carry the existing row forward as resolved and record it so the driver
                # can flag it for triage.
                rows.append(self._carry_forward_resolved(ticker_symbol, dfq, current_time))
                self.uncurated_delisted_tickers.append(ticker_symbol)
                logger.warning(
                    f"{ticker_symbol} returned no usable price data (no name or no price "
                    "history) and is no longer in the S&P 500 (likely delisted or renamed). "
                    "If delisted, add it to nullified_questions; if renamed, add it to "
                    "ticker_renames (mapping it to its replacement symbol)."
                )

        return pd.DataFrame(rows)

    @staticmethod
    def _carry_forward_resolved(ticker_symbol: str, dfq: pd.DataFrame, current_time: str) -> dict:
        """Return a delisted ticker's existing question row, marked resolved.

        Shared by the curated-nullified skip (before fetch) and the runtime delisted heuristic
        (fetch returned nothing). freeze_datetime_value, latest_close_date and company_name get
        the delisted marker.

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
                "latest_close_date": "N/A",
                "company_name": "N/A",
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
            existing_resolution_files (dict | None): Per-question existing resolution data.
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
                # Resolution file is copied from the replacement ticker below.
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
            del question["latest_close_date"]
            del question["company_name"]

            # Upsert into dfq
            if question["id"] in dfq["id"].values:
                dfq_index = dfq.index[dfq["id"] == question["id"]].tolist()[0]
                for key, value in question.items():
                    dfq.at[dfq_index, key] = value
            else:
                new_q_row = pd.DataFrame([question])
                new_q_row = new_q_row.astype(constants.QUESTION_FILE_COLUMN_DTYPE)
                dfq = pd.concat([dfq, new_q_row], ignore_index=True)

        # A renamed original's file is a copy of its replacement's, written whenever the
        # replacement's file is.
        for entry in self.ticker_renames:
            if entry["replacement_ticker"] in resolution_files:
                replacement_file = resolution_files[entry["replacement_ticker"]]
                resolution_files[entry["original_ticker"]] = replacement_file.assign(
                    id=entry["original_ticker"]
                )

        dfq = self._upsert_pair_questions(dfq, dff, existing_resolution_files, resolution_files)

        return UpdateResult(
            dfq=dfq,
            resolution_files=resolution_files,
        )

    # ------------------------------------------------------------------
    # Private: pair questions
    # ------------------------------------------------------------------

    def _upsert_pair_questions(
        self,
        dfq: pd.DataFrame,
        dff: pd.DataFrame,
        existing_resolution_files: dict[str, pd.DataFrame],
        built_resolution_files: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Upsert one question row per pair in ``self.pairs`` and return the new dfq.

        A pair row is built from its tickers' resolution series: the one built this run, else the
        existing file. Pair rows have no resolution file; ``_resolve`` reads the tickers. A pair is
        resolved (out of sampling) once either ticker is resolved in the bank, nullified, or a
        renamed original.

        Args:
            dfq (pd.DataFrame): Question bank after this run's ticker upserts.
            dff (pd.DataFrame): This run's fetch frame (business summaries per ticker).
            existing_resolution_files (dict): Existing resolution series keyed by id.
            built_resolution_files (dict): Resolution series built this run keyed by id.
        """
        if not self.pairs:
            return dfq

        fetched = {str(row["id"]): row for row in dff.to_dict("records")}
        retired_tickers = (
            set(dfq.loc[dfq["resolved"].astype(bool), "id"])
            | self.get_nullified_ids()
            | {entry["original_ticker"] for entry in self.ticker_renames}
        )

        def series_for(ticker: str) -> pd.DataFrame | None:
            built = built_resolution_files.get(ticker)
            return built if built is not None else existing_resolution_files.get(ticker)

        bank_backgrounds = dict(zip(dfq["id"], dfq["background"]))

        def summary_for(ticker: str) -> str:
            row = fetched.get(ticker)
            if row is not None:
                return row["background"]
            # A ticker that failed to fetch tonight keeps the summary its bank row already has,
            # so one bad night does not rewrite every pair that uses it.
            return bank_backgrounds.get(ticker, "N/A")

        bank_questions = dict(zip(dfq["id"], dfq["question"]))

        def label_for(ticker: str) -> str | None:
            """Return "Apple Inc. (AAPL)", or None when tonight's fetch has no name for it."""
            row = fetched.get(ticker)
            if row is None or row["company_name"] == "N/A":
                return None
            return f"{row['company_name']} ({ticker})"

        def question_for(x: str, y: str) -> str:
            x_label, y_label = label_for(x), label_for(y)
            if x_label is not None and y_label is not None:
                return _pair_question_text(x_label, y_label)
            # A ticker with no name tonight keeps the text its bank row already has, so one bad
            # night does not reword the question; a pair new to the bank names the tickers alone.
            return bank_questions.get(self._make_pair_id(x, y)) or _pair_question_text(x, y)

        rows = []
        for x, y in self.pairs:
            rows.append(
                {
                    "id": self._make_pair_id(x, y),
                    "question": question_for(x, y),
                    "background": _pair_background(x, summary_for(x), y, summary_for(y)),
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "url": _pair_url(x, y),
                    "resolved": x in retired_tickers or y in retired_tickers,
                    "market_info_resolution_datetime": "N/A",
                    "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                    "freeze_datetime_value": _pair_freeze_value(x, series_for(x), y, series_for(y)),
                    "freeze_datetime_value_explanation": _PAIR_FREEZE_VALUE_EXPLANATION,
                }
            )

        # Drop then append: one vectorized upsert instead of a per-row index lookup.
        new_rows = pd.DataFrame(rows).astype(constants.QUESTION_FILE_COLUMN_DTYPE)
        dfq = dfq[~dfq["id"].isin(new_rows["id"])]
        return pd.concat([dfq, new_rows], ignore_index=True)

    # ------------------------------------------------------------------
    # Private: pair ids and the series a pair resolves on
    # ------------------------------------------------------------------

    @staticmethod
    def _is_pair_id(question_id: object) -> bool:
        """Tell whether a question id names a pair (a string with the separator)."""
        return isinstance(question_id, str) and _PAIR_SEPARATOR in question_id

    @staticmethod
    def _make_pair_id(x: str, y: str) -> str:
        """Return the id of the pair question for tickers X and Y."""
        return f"{x}{_PAIR_SEPARATOR}{y}"

    @staticmethod
    def _split_pair_id(pair_id: str) -> tuple[str, str]:
        """Return the two tickers of a pair id."""
        x, y = pair_id.split(_PAIR_SEPARATOR)
        return x, y

    def _id_is_nullified(self, id_val, nullified_ids: set[str]) -> bool:
        """Check whether a question id is nullified; a pair is, when either of its tickers is.

        Nullification is dated from the ticker's last trading session, so a pair asked after one
        stock stopped trading is nullified like the single-ticker question would be, while a
        pair asked while both traded resolves against the frozen final close.
        """
        if self._is_pair_id(id_val):
            return any(t in nullified_ids for t in self._split_pair_id(id_val))
        return super()._id_is_nullified(id_val, nullified_ids)

    def _pair_ratio_series(self, dfr: pd.DataFrame, pair_id: str) -> pd.DataFrame:
        """Return X's price divided by Y's price on every date both tickers have in ``dfr``.

        The ratio rises exactly when X's rate of return exceeds Y's, so the dataset rule "value
        on the resolution date is greater than value on the due date" resolves the pair. Dates
        where either value is missing, unparseable, or Y is zero are dropped.

        Args:
            dfr (pd.DataFrame): Resolution values with columns [id, date, value].
            pair_id (str): The pair id ``X_Y``.

        Returns:
            DataFrame with columns [id, date, value], sorted by date.
        """
        x, y = self._split_pair_id(pair_id)
        prices = pd.merge(
            dfr.loc[dfr["id"] == x, ["date", "value"]],
            dfr.loc[dfr["id"] == y, ["date", "value"]],
            on="date",
            suffixes=("_x", "_y"),
        )
        ratio = pd.to_numeric(prices["value_x"], errors="coerce") / pd.to_numeric(
            prices["value_y"], errors="coerce"
        )
        out = pd.DataFrame({"id": pair_id, "date": prices["date"], "value": ratio})
        out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
        return out.sort_values("date", ignore_index=True)[["id", "date", "value"]]

    def _resolve(
        self,
        df: DataFrame[ResolveReadyFrame],
        dfq: DataFrame[QuestionFrame],
        dfr: DataFrame[ResolutionFrame],
    ) -> tuple[DataFrame[ResolveReadyFrame], list[str]]:
        """Resolve pair questions as dataset questions on a synthesized price-ratio series.

        A pair id has no resolution file. Its series is X's price divided by Y's price per date,
        and that ratio rises exactly when X's rate of return beats Y's. Appending these rows to
        ``dfr`` lets ``DatasetSource._resolve`` handle pairs and single tickers with one rule. A
        pair whose ticker is missing from ``dfr`` gets an empty series and fails id validation like
        a missing ticker.
        """
        pair_ids = {question_id for question_id in df["id"] if self._is_pair_id(question_id)}
        if pair_ids:
            ratio_frames = [self._pair_ratio_series(dfr, pair_id) for pair_id in sorted(pair_ids)]
            dfr = pd.concat([dfr, *ratio_frames], ignore_index=True)
        return super()._resolve(df, dfq, dfr)

    @staticmethod
    def _get_sp500_tickers() -> list[str]:
        """Scrape S&P 500 constituent tickers from Wikipedia."""
        import requests
        from bs4 import BeautifulSoup

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

    def _fetch_one_stock(
        self, ticker_symbol: str
    ) -> tuple[str | None, str | None, pd.DataFrame | None]:
        """Fetch company name, business summary and the latest historical row for one ticker.

        Args:
            ticker_symbol (str): Stock ticker symbol.

        Returns:
            Tuple of (company_name, business_summary, hist_df). company_name is None when
            yfinance has no name for the ticker and business_summary is None when it has no
            profile for it (Yahoo's profile request can fail while the quote succeeds).
            (None, None, None) on failure.
        """
        import yfinance as yf

        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            company_name = info.get("longName") or info.get("shortName")
            business_summary = info.get("longBusinessSummary")
            hist = ticker.history(period="5d", auto_adjust=False).reset_index()
            yesterday = self.get_date_today() - timedelta(days=1)
            hist["Date"] = pd.to_datetime(hist["Date"])
            hist = hist[hist["Date"].dt.date <= yesterday].tail(1)
            return (
                company_name,
                business_summary,
                self._fill_missing_close(ticker_symbol, hist, info),
            )
        except Exception:
            return None, None, None

    @staticmethod
    def _fill_missing_close(ticker_symbol: str, hist: pd.DataFrame, info: dict) -> pd.DataFrame:
        """Fill a missing Close on the latest bar from the ticker's quote.

        Since September 2026 Yahoo's chart endpoint returns the most recent completed session with
        Open/High/Low/Volume filled but Close null (yfinance issue #2925). The close is still in
        the quote metadata, and which field holds it depends on when this runs relative to the
        bar's trading day: a quote from that day means ``regularMarketPrice`` is its close; a quote
        from a later session means the market has traded since, so that day's close is the quote's
        ``regularMarketPreviousClose``. A quote older than the bar is inconsistent and not used.

        Args:
            ticker_symbol (str): Stock ticker symbol, for logging.
            hist (pd.DataFrame): At most one price bar, with ``Date`` and ``Close`` columns.
            info (dict): The ticker's quote metadata (``yf.Ticker.info``).
        """
        if hist.empty or pd.notna(hist["Close"].iloc[-1]):
            return hist

        bar_date = hist["Date"].iloc[-1]
        market_time = info.get("regularMarketTime")
        if market_time is None:
            logger.warning(f"{ticker_symbol}: Close missing on {bar_date.date()} and no quote.")
            return hist

        quote_time = pd.Timestamp(market_time, unit="s", tz="UTC")
        if bar_date.tzinfo is not None:
            quote_time = quote_time.tz_convert(bar_date.tzinfo)
        if quote_time.date() == bar_date.date():
            price = info.get("regularMarketPrice")
        elif quote_time.date() > bar_date.date():
            price = info.get("regularMarketPreviousClose")
        else:
            logger.warning(
                f"{ticker_symbol}: Close missing on {bar_date.date()}; quote is from "
                f"{quote_time.date()}, not using it."
            )
            return hist
        if price is None:
            logger.warning(f"{ticker_symbol}: Close missing on {bar_date.date()} and no quote.")
            return hist

        logger.info(f"{ticker_symbol}: Close missing on {bar_date.date()}; using quote {price}.")
        hist = hist.copy()
        hist.loc[hist.index[-1], "Close"] = price
        return hist

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
        import yfinance as yf

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
        latest_close: float | None = None,
        latest_close_date: date | None = None,
    ) -> pd.DataFrame | None:
        """Build a resolution DataFrame of daily prices for a ticker.

        Args:
            existing_df (pd.DataFrame | None): Existing resolution data, used as the fallback when
                the fetch returns nothing.
            ticker_symbol (str): Stock ticker symbol.
            period (str): yfinance period string.
            latest_close (float | None): The close the fetch job got for ``latest_close_date``,
                used when the history's bar for that date has no close.
            latest_close_date (date | None): The session ``latest_close`` belongs to.

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

        # Yahoo serves closes as single-precision floats (329.4 arrives as 329.3999938965). The
        # file holds cents so every row, repaired or not, has the same representation as the
        # freeze value and a resolution never reads representation noise as a price move.
        df["value"] = df["value"].round(2)

        # Yahoo's chart endpoint can return the latest session with no close. The fetch job repaired
        # that close from the quote (see _fill_missing_close) and carries it as the row's freeze
        # value; use it here so the forward fill below does not copy the day before. The close is
        # used only for the session it was observed for: a fetch row from another night, or a
        # history whose newest bar is a different session, says nothing about this bar.
        if (
            latest_close is not None
            and not df.empty
            and df["date"].iloc[-1] == latest_close_date
            and pd.isna(df["value"].iloc[-1])
        ):
            df = df.copy()
            df.loc[df.index[-1], "value"] = latest_close

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
        # Rows are cents everywhere else; this file is never rebuilt from Yahoo, so round it here.
        df_new = existing_df.assign(value=existing_df["value"].round(2))
        df_new = self._finalize_resolution_file(df_new)
        if existing_df.equals(df_new):
            return None
        return df_new

    @staticmethod
    def _newest_close_matches(
        existing_df: pd.DataFrame, latest_close: float | None, latest_close_date: date | None
    ) -> bool:
        """Tell whether a file's newest close is the close the fetch job got for that session.

        Both hold cents, so they must be equal. With no fetch close for the newest row's date to
        compare against, the file counts as matching.

        Args:
            existing_df (pd.DataFrame): Existing resolution data with ``date`` and ``value``.
            latest_close (float | None): The fetch row's freeze value, or None when it has none.
            latest_close_date (date | None): The session that close belongs to.
        """
        if latest_close is None:
            return True
        newest = existing_df.iloc[-1]
        if pd.to_datetime(newest["date"]).date() != latest_close_date:
            return True
        value = pd.to_numeric(newest["value"], errors="coerce")
        return bool(pd.notna(value) and float(value) == latest_close)

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

        # The fetch row's freeze value is the latest close as repaired from the quote, dated by
        # latest_close_date; both are "N/A" on carried-forward rows.
        latest_close = pd.to_numeric(question["freeze_datetime_value"], errors="coerce")
        latest_close_date = pd.to_datetime(question["latest_close_date"], errors="coerce")
        if pd.isna(latest_close) or pd.isna(latest_close_date):
            latest_close, latest_close_date = None, None
        else:
            latest_close, latest_close_date = float(latest_close), latest_close_date.date()

        # Already up-to-date check — skip the API call entirely. Resolved (delisted) tickers are
        # always rebuilt so the final close price is forward-filled.
        if (
            not force
            and not is_resolved
            and existing_df is not None
            and not existing_df.empty
            and pd.to_datetime(existing_df["date"].iloc[-1]).date() >= yesterday
            # A file that reaches yesterday with a forward-filled close is not up to date; a rerun
            # must rebuild it so the fetch job's close lands in that row (idempotent update).
            and self._newest_close_matches(existing_df, latest_close, latest_close_date)
        ):
            logger.info(f"{question['id']} is skipped because it's already up-to-date!")
            return None

        df_new = self._get_historical_prices(
            existing_df,
            question["id"],
            period,
            latest_close=latest_close,
            latest_close_date=latest_close_date,
        )
        if df_new is None:
            return None

        if is_resolved:
            df_new = self._finalize_resolution_file(df_new)

        # Only upload dataframes that changed.
        if existing_df is not None and not existing_df.empty and existing_df.equals(df_new):
            return None

        return df_new
