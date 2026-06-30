"""Tests for yfinance source, fetch, and update logic."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from _schemas import YfinanceFetchFrame
from helpers import constants
from sources._metadata import SOURCE_METADATA
from sources.yfinance import YfinanceSource
from tests.conftest import (
    make_forecast_df,
    make_question_df,
    make_resolution_df,
    make_yfinance_fetch_df,
)

DELISTED_STOCKS = SOURCE_METADATA["yfinance"]["nullified_questions"]
TICKER_RENAMES = SOURCE_METADATA["yfinance"]["ticker_renames"]


class TestTickerRenamesDefinition:
    """Test the TICKER_RENAMES list is correctly defined."""

    def test_all_entries_have_required_keys(self):
        for entry in TICKER_RENAMES:
            assert "original_ticker" in entry, f"{entry} missing original_ticker"
            assert "replacement_ticker" in entry, f"{entry} missing replacement_ticker"
            assert isinstance(entry["original_ticker"], str)
            assert isinstance(entry["replacement_ticker"], str)

    def test_known_renames_present(self):
        renames = {e["original_ticker"]: e["replacement_ticker"] for e in TICKER_RENAMES}
        assert renames["FI"] == "FISV"
        assert renames["MMC"] == "MRSH"


class TestDelistedStocksDefinition:
    """Test the DELISTED_STOCKS list is correctly defined."""

    def test_all_entries_have_required_fields(self):
        for nq in DELISTED_STOCKS:
            assert isinstance(nq.id, str), f"{nq} missing string id"
            assert isinstance(nq.nullification_start_date, date), f"{nq} missing date"

    def test_known_delisted_tickers_present(self):
        ids = {nq.id for nq in DELISTED_STOCKS}
        expected = {"MRO", "CTLT", "DFS", "JNPR", "ANSS", "HES", "PARA", "WBA", "K", "DAY"}
        assert ids == expected

    def test_nullification_dates_are_day_after_last_trade(self):
        date_map = {nq.id: nq.nullification_start_date for nq in DELISTED_STOCKS}
        assert date_map["MRO"] == date(2024, 11, 22)
        assert date_map["CTLT"] == date(2024, 12, 18)
        assert date_map["DFS"] == date(2025, 5, 19)
        assert date_map["JNPR"] == date(2025, 7, 2)
        assert date_map["ANSS"] == date(2025, 7, 17)
        assert date_map["HES"] == date(2025, 7, 18)
        assert date_map["PARA"] == date(2025, 8, 7)
        assert date_map["WBA"] == date(2025, 8, 28)
        assert date_map["K"] == date(2025, 12, 11)
        assert date_map["DAY"] == date(2026, 2, 4)


class TestYfinanceSourceNullification:
    """Test that YfinanceSource nullifies delisted stocks correctly."""

    @pytest.fixture()
    def source(self):
        return YfinanceSource()

    def test_source_has_nullified_questions(self, source):
        assert len(source.nullified_questions) == 10

    def test_pre_delisting_question_not_nullified(self, source):
        """JNPR in the 2025-03-30 question set should NOT be nullified (delisted 2025-07-02)."""
        nullified = source.get_nullified_ids(as_of=date(2025, 3, 30))
        assert "JNPR" not in nullified

    def test_post_delisting_question_nullified(self, source):
        """JNPR in the 2025-08-17 question set should be nullified (delisted 2025-07-02)."""
        nullified = source.get_nullified_ids(as_of=date(2025, 8, 17))
        assert "JNPR" in nullified

    def test_early_delisting_nullifies_all_later_sets(self, source):
        """MRO was delisted 2024-11-22. Both question sets (2025-04-13, 2025-08-17) are after."""
        nullified_apr = source.get_nullified_ids(as_of=date(2025, 4, 13))
        nullified_aug = source.get_nullified_ids(as_of=date(2025, 8, 17))
        assert "MRO" in nullified_apr
        assert "MRO" in nullified_aug

    def test_exact_nullification_date_is_nullified(self, source):
        """On the nullification date itself, the question should be nullified."""
        nullified = source.get_nullified_ids(as_of=date(2025, 7, 2))
        assert "JNPR" in nullified

    def test_day_before_nullification_date_not_nullified(self, source):
        """The day before nullification, the question should be valid."""
        nullified = source.get_nullified_ids(as_of=date(2025, 7, 1))
        assert "JNPR" not in nullified

    def test_resolve_nullifies_post_delisting_row(self, source):
        """A delisted stock in a post-delisting question set resolves to NaN."""
        df = make_forecast_df(
            [
                {"id": "JNPR", "source": "yfinance", "forecast_due_date": "2025-08-17"},
                {"id": "AAPL", "source": "yfinance", "forecast_due_date": "2025-08-17"},
            ]
        )
        dfq = make_question_df([{"id": "JNPR"}, {"id": "AAPL"}])
        dfr = pd.DataFrame(
            {
                "id": ["AAPL", "AAPL"],
                "date": pd.to_datetime(["2025-12-31", "2025-08-17"]),
                "value": [150.0, 145.0],
            }
        )

        result, _ = source.resolve(df, dfq, dfr, forecast_due_date=date(2025, 8, 17))

        jnpr_row = result[result["id"] == "JNPR"].iloc[0]
        assert pd.isna(jnpr_row["resolved_to"])
        assert bool(jnpr_row["resolved"]) is True

    def test_resolve_pre_delisting_question_resolves_normally(self, source):
        """JNPR asked on 2025-03-30 (before delisting 2025-07-02) should resolve via final price."""
        forecast_due = "2025-03-30"
        resolution = "2025-09-30"
        df = make_forecast_df(
            [
                {
                    "id": "JNPR",
                    "source": "yfinance",
                    "forecast_due_date": forecast_due,
                    "resolution_date": resolution,
                },
            ]
        )
        dfq = make_question_df([{"id": "JNPR"}])
        dfr = pd.DataFrame(
            {
                "id": ["JNPR", "JNPR"],
                "date": pd.to_datetime([forecast_due, resolution]),
                "value": [35.0, 40.0],
            }
        )

        result, _ = source.resolve(df, dfq, dfr, forecast_due_date=date(2025, 3, 30))

        jnpr_row = result[result["id"] == "JNPR"].iloc[0]
        assert jnpr_row["resolved_to"] == 1.0
        assert bool(jnpr_row["resolved"]) is True


# ===========================================================================
# Refactored YfinanceSource (sources/yfinance.py) — fetch/update behaviour.
# The classes above test the metadata and base-class nullification; the classes
# below test the refactored source's own fetch/update logic.
# ===========================================================================


class TestSourceGetDateToday:
    """Tests for YfinanceSource.get_date_today (pinned-date accessor)."""

    def test_falls_back_to_live_date_when_unpinned(self, yfinance_source, freeze_today):
        """With no run pinned, returns the live date."""
        freeze_today(date(2026, 3, 18))
        assert yfinance_source.get_date_today() == date(2026, 3, 18)

    def test_returns_pinned_date(self, yfinance_source, freeze_today):
        """Once pinned, returns the pinned date regardless of the live clock."""
        freeze_today(date(2026, 3, 18))
        yfinance_source._today = date(2025, 1, 1)
        assert yfinance_source.get_date_today() == date(2025, 1, 1)


class TestSourceGetSp500Tickers:
    """Tests for YfinanceSource._get_sp500_tickers."""

    _HTML = """
    <html><body>
    <table id="constituents">
      <tr><th>Symbol</th><th>Security</th></tr>
      <tr><td>AAPL</td><td>Apple Inc.</td></tr>
      <tr><td>MSFT</td><td>Microsoft Corp.</td></tr>
      <tr><td>GOOGL</td><td>Alphabet Inc.</td></tr>
    </table>
    </body></html>
    """

    @patch("sources.yfinance.requests.get")
    def test_parses_wikipedia_table(self, mock_get):
        """Returns the ticker list from the Wikipedia constituents table."""
        mock_resp = Mock()
        mock_resp.content = self._HTML.encode()
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        assert YfinanceSource._get_sp500_tickers() == ["AAPL", "MSFT", "GOOGL"]

    @patch("sources.yfinance.requests.get")
    def test_sends_benchmark_user_agent(self, mock_get):
        """Sends the benchmark User-Agent header (regression for the dropped/Mozilla UA)."""
        mock_resp = Mock()
        mock_resp.content = self._HTML.encode()
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        YfinanceSource._get_sp500_tickers()

        _, kwargs = mock_get.call_args
        assert kwargs["headers"] == {"User-Agent": constants.BENCHMARK_USER_AGENT}

    @patch("sources.yfinance.requests.get")
    def test_returns_empty_on_error(self, mock_get):
        """Returns an empty list when the request fails (legacy-faithful swallow)."""
        mock_get.side_effect = Exception("Network error")
        assert YfinanceSource._get_sp500_tickers() == []


class TestSourceSelectTimeRange:
    """Tests for YfinanceSource._select_time_range."""

    @pytest.mark.parametrize(
        "days,expected",
        [
            (0, "1d"),
            (1, "1d"),
            (5, "5d"),
            (30, "1mo"),
            (90, "3mo"),
            (180, "6mo"),
            (365, "1y"),
            (730, "2y"),
            (1825, "5y"),
            (3650, "10y"),
            (4000, "max"),
        ],
    )
    def test_time_range_mapping(self, days, expected):
        """Correct yfinance period for each day range."""
        assert YfinanceSource._select_time_range(days) == expected


class TestSourceFetchOneStock:
    """Tests for YfinanceSource._fetch_one_stock."""

    @patch("sources.yfinance.yf.Ticker")
    def test_uses_unadjusted_close(self, mock_ticker_cls, yfinance_source, freeze_today):
        """History is requested with auto_adjust=False (raw close prices)."""
        freeze_today(date(2026, 3, 18))
        mock_ticker = MagicMock()
        mock_ticker.info = {"longName": "Apple Inc."}
        hist = pd.DataFrame(
            {"Date": pd.to_datetime(["2026-03-16", "2026-03-17"]), "Close": [253.0, 254.23]}
        ).set_index("Date")
        mock_ticker.history.return_value = hist
        mock_ticker_cls.return_value = mock_ticker

        name, out = yfinance_source._fetch_one_stock("AAPL")

        mock_ticker.history.assert_called_once_with(period="5d", auto_adjust=False)
        assert name == "Apple Inc."
        assert out["Close"].iloc[-1] == 254.23  # capped at yesterday, last row

    @patch("sources.yfinance.yf.Ticker")
    def test_returns_none_on_error(self, mock_ticker_cls, yfinance_source):
        """Returns (None, None) when the ticker lookup fails (legacy-faithful swallow)."""
        mock_ticker_cls.return_value.info.__getitem__.side_effect = KeyError("longName")
        assert yfinance_source._fetch_one_stock("INVALID") == (None, None)


class TestSourceFetch:
    """Tests for YfinanceSource.fetch."""

    @staticmethod
    def _ticker_with(close, name="Apple Inc.", summary="A company."):
        ticker = MagicMock()
        ticker.info = {"longName": name, "longBusinessSummary": summary}
        ticker.history.return_value = pd.DataFrame(
            {"Date": pd.to_datetime(["2026-03-17"]), "Close": [close]}
        ).set_index("Date")
        return ticker

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL"])
    def test_builds_valid_fetch_frame(
        self, _mock_tickers, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A fetched ticker produces a schema-valid YfinanceFetchFrame row."""
        freeze_today(date(2026, 3, 18))
        mock_ticker_cls.return_value = self._ticker_with(254.23)

        dff = yfinance_source.fetch(dfq=make_question_df([{"id": "AAPL"}]))

        YfinanceFetchFrame.validate(dff)
        row = dff[dff["id"] == "AAPL"].iloc[0]
        assert bool(row["resolved"]) is False
        assert row["url"] == "https://finance.yahoo.com/quote/AAPL"
        assert float(row["freeze_datetime_value"]) == 254.23

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=[])
    def test_delisted_ticker_marked_resolved(
        self, _mock_tickers, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A pool ticker that left the S&P 500 and fails to fetch is carried forward as resolved."""
        freeze_today(date(2026, 3, 18))
        mock_ticker_cls.return_value.info.__getitem__.side_effect = KeyError("longName")

        dfq = make_question_df([{"id": "OLDCO", "question": "legacy question"}])
        dff = yfinance_source.fetch(dfq=dfq)

        row = dff[dff["id"] == "OLDCO"].iloc[0]
        assert bool(row["resolved"]) is True
        assert row["freeze_datetime_value"] == "N/A"
        assert row["question"] == "legacy question"  # original question text preserved

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL", "FAILS"])
    def test_in_sp500_fetch_failure_is_dropped(
        self, _mock_tickers, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A ticker still in the S&P 500 that fails to fetch is dropped (not delisted)."""
        freeze_today(date(2026, 3, 18))
        hist = pd.DataFrame({"Close": [254.23], "Date": pd.to_datetime(["2026-03-17"])})
        mock_fetch_one.side_effect = lambda sym: (
            ("Apple Inc.", hist) if sym == "AAPL" else (None, None)
        )
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        dff = yfinance_source.fetch(dfq=make_question_df([{"id": "AAPL"}]))

        assert "FAILS" not in dff["id"].values
        assert "AAPL" in dff["id"].values

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL"])
    def test_not_in_sp500_but_fetch_succeeds_not_resolved(
        self, _mock_tickers, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A pool ticker no longer in the S&P 500 that still returns data is not marked resolved."""
        freeze_today(date(2026, 3, 18))
        hist = pd.DataFrame({"Close": [100.0], "Date": pd.to_datetime(["2026-03-17"])})
        mock_fetch_one.return_value = ("Some Co", hist)
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        dff = yfinance_source.fetch(dfq=make_question_df([{"id": "AAPL"}, {"id": "OUTCO"}]))

        outco = dff[dff["id"] == "OUTCO"].iloc[0]
        assert bool(outco["resolved"]) is False


class TestSourceFetchSkipsNullified:
    """Nullified (known-delisted) tickers are never fetched, only carried forward."""

    @staticmethod
    def _ok_hist():
        return pd.DataFrame({"Close": [254.23], "Date": pd.to_datetime(["2026-03-17"])})

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL"])
    def test_nullified_in_pool_skipped_and_carried_forward(
        self, _mock_tickers, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A nullified pool ticker is never sent to the API and is carried forward as resolved."""
        freeze_today(date(2026, 3, 18))
        mock_fetch_one.return_value = ("Apple Inc.", self._ok_hist())
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        dfq = make_question_df(
            [
                {"id": "AAPL", "question": "aapl question"},
                {"id": "ANSS", "question": "legacy ANSS question"},
            ]
        )
        dff = yfinance_source.fetch(dfq=dfq)

        fetched = [call.args[0] for call in mock_fetch_one.call_args_list]
        assert "ANSS" not in fetched, "nullified ticker must never be fetched"
        assert "AAPL" in fetched

        anss = dff[dff["id"] == "ANSS"].iloc[0]
        assert bool(anss["resolved"]) is True
        assert anss["freeze_datetime_value"] == "N/A"
        assert anss["question"] == "legacy ANSS question"  # original row preserved
        YfinanceFetchFrame.validate(dff)  # carry-forward row is schema-valid

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL", "ANSS"])
    def test_nullified_never_fetched_even_if_in_sp500_scrape(
        self, _mock_tickers, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """A nullified ticker is dropped from the universe even if the S&P 500 scrape lists it."""
        freeze_today(date(2026, 3, 18))
        mock_fetch_one.return_value = ("Apple Inc.", self._ok_hist())
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        dff = yfinance_source.fetch(dfq=make_question_df([{"id": "AAPL"}]))

        fetched = [call.args[0] for call in mock_fetch_one.call_args_list]
        assert "ANSS" not in fetched
        assert "ANSS" not in dff["id"].values  # not in pool -> not carried forward either

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL"])
    def test_carry_forward_does_not_404(
        self, _mock_tickers, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """Carrying a nullified ticker forward must not invoke yf.Ticker for it (no 404 noise)."""
        freeze_today(date(2026, 3, 18))
        mock_fetch_one.return_value = ("Apple Inc.", self._ok_hist())
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        yfinance_source.fetch(dfq=make_question_df([{"id": "AAPL"}, {"id": "WBA"}]))

        ticker_calls = [call.args[0] for call in mock_ticker_cls.call_args_list]
        assert "WBA" not in ticker_calls, "nullified ticker must not be passed to yf.Ticker"


class TestSourceFetchSkipsRenamed:
    """Renamed originals are never fetched; only their replacement is."""

    @patch("sources.yfinance.yf.Ticker")
    @patch.object(YfinanceSource, "_fetch_one_stock")
    def test_renamed_original_skipped_and_carried_forward(
        self, mock_fetch_one, mock_ticker_cls, yfinance_source, freeze_today
    ):
        """The renamed original (e.g. FI) is not fetched and is carried forward as resolved; its
        replacement (e.g. FISV) is still fetched normally."""
        freeze_today(date(2026, 3, 18))
        original = yfinance_source.ticker_renames[0]["original_ticker"]  # FI
        replacement = yfinance_source.ticker_renames[0]["replacement_ticker"]  # FISV
        hist = pd.DataFrame({"Close": [254.23], "Date": pd.to_datetime(["2026-03-17"])})
        mock_fetch_one.return_value = ("Some Co", hist)
        mock_ticker_cls.return_value.info.get.return_value = "N/A"

        with patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL", replacement]):
            dfq = make_question_df([{"id": "AAPL"}, {"id": original}, {"id": replacement}])
            dff = yfinance_source.fetch(dfq=dfq)

        fetched = [call.args[0] for call in mock_fetch_one.call_args_list]
        assert original not in fetched, "renamed original must never be fetched"
        assert replacement in fetched, "the live replacement must still be fetched"

        fi = dff[dff["id"] == original].iloc[0]
        assert bool(fi["resolved"]) is True
        assert fi["freeze_datetime_value"] == "N/A"


class TestSourceBuildResolutionDf:
    """Tests for YfinanceSource._build_resolution_df."""

    @staticmethod
    def _prices(dates_, values):
        return pd.DataFrame({"date": pd.to_datetime(dates_), "value": values})

    def test_skips_when_up_to_date(self, yfinance_source, freeze_today):
        """Returns None (no upload) when the existing file already reaches yesterday."""
        freeze_today(date(2026, 3, 18))
        existing = make_resolution_df([{"id": "AAPL", "date": "2026-03-17", "value": 250.0}])
        existing["date"] = existing["date"].astype(str)
        out = yfinance_source._build_resolution_df(
            {"id": "AAPL", "resolved": False}, period="1mo", existing_df=existing
        )
        assert out is None

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_force_rebuilds_when_up_to_date(self, mock_fetch, yfinance_source, freeze_today):
        """force=True re-fetches even when the existing file is current."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = self._prices(["2026-03-16", "2026-03-17"], [248.0, 251.0])
        existing = make_resolution_df([{"id": "AAPL", "date": "2026-03-17", "value": 250.0}])
        existing["date"] = existing["date"].astype(str)

        out = yfinance_source._build_resolution_df(
            {"id": "AAPL", "resolved": False}, period="1mo", existing_df=existing, force=True
        )
        assert out is not None
        mock_fetch.assert_called_once()

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_resolved_forward_fills_to_yesterday(self, mock_fetch, yfinance_source, freeze_today):
        """A resolved (delisted) ticker is forward-filled through yesterday."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = self._prices(["2026-03-13"], [99.5])

        out = yfinance_source._build_resolution_df(
            {"id": "GONE", "resolved": True}, period="1mo", existing_df=None
        )
        assert out is not None
        assert pd.to_datetime(out["date"]).max().date() == date(2026, 3, 17)  # yesterday
        assert float(out["value"].iloc[-1]) == 99.5  # final close carried forward
        assert (out["id"] == "GONE").all()

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_new_ticker_failed_fetch_returns_none(self, mock_fetch, yfinance_source, freeze_today):
        """A brand-new ticker whose fetch returns nothing writes no file (no empty upload)."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = pd.DataFrame()  # fetch failure / no data
        out = yfinance_source._build_resolution_df(
            {"id": "NEWCO", "resolved": False}, period="1mo", existing_df=None
        )
        assert out is None

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_failed_fetch_keeps_existing_file(self, mock_fetch, yfinance_source, freeze_today):
        """When fetch fails but a file exists, the existing data is kept (returned unchanged)."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = pd.DataFrame()  # fetch failure
        existing = make_resolution_df([{"id": "AAPL", "date": "2026-03-10", "value": 250.0}])
        existing["date"] = existing["date"].astype(str)
        # Not up-to-date (last date 03-10 < yesterday 03-17), so it doesn't early-skip; fetch fails.
        out = yfinance_source._build_resolution_df(
            {"id": "AAPL", "resolved": False}, period="1mo", existing_df=existing, force=True
        )
        # Existing equals the fallback -> no change -> None (existing file left as-is).
        assert out is None

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_unchanged_returns_none(self, mock_fetch, yfinance_source, freeze_today):
        """If the rebuilt file equals the existing one, returns None (no upload)."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = self._prices(["2026-03-16", "2026-03-17"], [248.0, 251.0])
        built = yfinance_source._build_resolution_df(
            {"id": "AAPL", "resolved": False}, period="1mo", existing_df=None
        )
        out = yfinance_source._build_resolution_df(
            {"id": "AAPL", "resolved": False}, period="1mo", existing_df=built, force=True
        )
        assert out is None


class TestSourceUpdate:
    """Tests for YfinanceSource.update."""

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_appends_new_question_and_strips_transient(
        self, mock_fetch, yfinance_source, freeze_today
    ):
        """A new fetched ticker is added to dfq without transient fetch columns."""
        freeze_today(date(2026, 3, 18))
        mock_fetch.return_value = pd.DataFrame(
            {"date": pd.to_datetime(["2026-03-16", "2026-03-17"]), "value": [10.0, 11.0]}
        )
        dfq = make_question_df([{"id": "OLD"}])
        dff = make_yfinance_fetch_df([{"id": "NEW"}])

        result = yfinance_source.update(dfq, dff)

        assert "NEW" in result.dfq["id"].values
        assert "fetch_datetime" not in result.dfq.columns
        assert "probability" not in result.dfq.columns
        assert "NEW" in result.resolution_files

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_renamed_ticker_resolution_built_from_replacement(
        self, mock_fetch, yfinance_source, freeze_today
    ):
        """Renamed tickers resolve under the original id using the replacement's price history.

        Regression for the delisted/renamed-ticker handling (prod fix 5042c68).
        """
        freeze_today(date(2026, 3, 18))
        renames = yfinance_source.ticker_renames
        assert renames, "yfinance metadata should declare ticker_renames"
        original = renames[0]["original_ticker"]
        replacement = renames[0]["replacement_ticker"]

        seen = []

        def fake_fetch(symbol, period):
            seen.append(symbol)
            return pd.DataFrame(
                {"date": pd.to_datetime(["2026-03-16", "2026-03-17"]), "value": [5.0, 6.0]}
            )

        mock_fetch.side_effect = fake_fetch

        dfq = make_question_df([{"id": original}])
        # The original ticker is NOT in dff (yfinance serves no data under it).
        dff = make_yfinance_fetch_df([{"id": "AAPL"}])

        result = yfinance_source.update(dfq, dff)

        assert original in result.resolution_files
        assert (result.resolution_files[original]["id"] == original).all()
        assert replacement in seen

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_renamed_original_skipped_in_main_loop(self, mock_fetch, yfinance_source, freeze_today):
        """A renamed original in dff is built via its replacement, never fetched directly."""
        freeze_today(date(2026, 3, 18))
        original = yfinance_source.ticker_renames[0]["original_ticker"]
        replacement = yfinance_source.ticker_renames[0]["replacement_ticker"]

        seen = []

        def fake_fetch(symbol, period):
            seen.append(symbol)
            return pd.DataFrame({"date": pd.to_datetime(["2026-03-17"]), "value": [6.0]})

        mock_fetch.side_effect = fake_fetch

        dfq = make_question_df([{"id": original}])
        dff = make_yfinance_fetch_df([{"id": original}])

        result = yfinance_source.update(dfq, dff)

        assert original in result.resolution_files
        assert original not in seen  # original symbol never fetched directly
        assert replacement in seen

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_renamed_original_is_exact_copy_of_replacement(
        self, mock_fetch, yfinance_source, freeze_today
    ):
        """When the replacement is in the pool, the original's resolution file is a relabelled copy
        of the replacement's series (identical date/value), and the replacement is fetched once."""
        freeze_today(date(2026, 3, 18))
        original = yfinance_source.ticker_renames[0]["original_ticker"]  # FI
        replacement = yfinance_source.ticker_renames[0]["replacement_ticker"]  # FISV

        seen = []

        def fake_fetch(symbol, period):
            seen.append(symbol)
            return pd.DataFrame(
                {"date": pd.to_datetime(["2026-03-16", "2026-03-17"]), "value": [10.0, 11.0]}
            )

        mock_fetch.side_effect = fake_fetch

        dfq = make_question_df([{"id": original}, {"id": replacement}])
        dff = make_yfinance_fetch_df(
            [{"id": replacement, "resolved": False}, {"id": original, "resolved": True}]
        )

        result = yfinance_source.update(dfq, dff)

        # Replacement fetched exactly once (its own build); original never fetched.
        assert seen.count(replacement) == 1
        assert original not in seen
        # Both files present and identical except for the id column.
        fi = result.resolution_files[original].reset_index(drop=True)
        fisv = result.resolution_files[replacement].reset_index(drop=True)
        assert (fi["id"] == original).all()
        assert (fisv["id"] == replacement).all()
        assert fi[["date", "value"]].equals(fisv[["date", "value"]])

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_nullified_stock_preserves_question_fields_and_is_not_fetched(
        self, mock_fetch, yfinance_source, freeze_today
    ):
        """A nullified (HES) ticker keeps its question fields, is never fetched, and is forward
        filled from its existing resolution file."""
        freeze_today(date(2026, 3, 18))
        seen = []

        def fake_fetch(symbol, period):
            seen.append(symbol)
            return pd.DataFrame({"date": pd.to_datetime(["2026-03-13"]), "value": [149.0]})

        mock_fetch.side_effect = fake_fetch
        dfq = make_question_df(
            [{"id": "HES", "question": "Will HES go up?", "background": "Hess Corporation."}]
        )
        dff = make_yfinance_fetch_df(
            [
                {
                    "id": "HES",
                    "question": "Will HES go up?",
                    "background": "Hess Corporation.",
                    "resolved": True,
                    "freeze_datetime_value": "N/A",
                }
            ]
        )
        existing = {
            "HES": make_resolution_df([{"id": "HES", "date": "2026-03-10", "value": 149.0}])
        }

        result = yfinance_source.update(dfq, dff, existing_resolution_files=existing)

        hes = result.dfq[result.dfq["id"] == "HES"].iloc[0]
        assert bool(hes["resolved"]) is True
        assert hes["question"] == "Will HES go up?"
        assert hes["background"] == "Hess Corporation."
        assert hes["freeze_datetime_value"] == "N/A"
        # Nullified: never fetched (no 404), resolution file forward-filled from existing data.
        assert "HES" not in seen
        assert "HES" in result.resolution_files
        out = result.resolution_files["HES"]
        assert pd.to_datetime(out["date"]).max().date() == date(2026, 3, 17)  # yesterday
        assert float(out["value"].iloc[-1]) == 149.0  # final close carried forward

    @patch.object(YfinanceSource, "_fetch_historical_prices")
    def test_nullified_stock_without_existing_file_writes_nothing(
        self, mock_fetch, yfinance_source, freeze_today
    ):
        """A nullified ticker with no existing resolution file is not fetched and writes no file."""
        freeze_today(date(2026, 3, 18))
        seen = []

        def fake_fetch(symbol, period):
            seen.append(symbol)
            return pd.DataFrame({"date": pd.to_datetime(["2026-03-17"]), "value": [1.0]})

        mock_fetch.side_effect = fake_fetch
        dfq = make_question_df([{"id": "HES"}])
        dff = make_yfinance_fetch_df([{"id": "HES", "resolved": True}])

        result = yfinance_source.update(dfq, dff)  # no existing_resolution_files

        assert "HES" not in seen  # never fetched
        assert "HES" not in result.resolution_files  # nothing to forward-fill


class TestSourceFinalizeResolutionFile:
    """Tests for YfinanceSource._finalize_resolution_file."""

    def test_empty_df_returns_empty(self, yfinance_source):
        """Empty input returns empty output."""
        df = pd.DataFrame(columns=["id", "date", "value"])
        assert yfinance_source._finalize_resolution_file(df).empty
