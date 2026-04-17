"""Tests for yfinance source, fetch, and update logic."""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from helpers import constants
from questions.yfinance.fetch.main import fetch_all_stock
from questions.yfinance.update_questions.main import (
    finalize_resolution_file,
    update_questions,
)
from sources.yfinance import DELISTED_STOCKS, TICKER_RENAMES, YfinanceSource
from tests.conftest import make_forecast_df, make_question_df


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

        result, _ = source.resolve(df, dfq, dfr, as_of=date(2025, 8, 17))

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

        result, _ = source.resolve(df, dfq, dfr, as_of=date(2025, 3, 30))

        jnpr_row = result[result["id"] == "JNPR"].iloc[0]
        assert jnpr_row["resolved_to"] == 1.0
        assert bool(jnpr_row["resolved"]) is True


class TestDetectDelistedStocks:
    """Test detection of delisted stocks during fetch."""

    @patch("questions.yfinance.fetch.main.time.sleep")
    @patch("questions.yfinance.fetch.main.get_sp500_tickers")
    @patch("questions.yfinance.fetch.main.fetch_one_stock")
    def test_stock_not_in_sp500_and_fetch_fails_is_resolved(
        self, mock_fetch, mock_sp500, mock_sleep
    ):
        """A stock not in S&P 500 that fails to fetch appears as resolved in the result."""
        mock_sp500.return_value = ["AAPL", "MSFT"]
        mock_fetch.return_value = (None, None)

        from tests.conftest import make_question_df

        dfq = make_question_df(
            [
                {"id": "AAPL"},
                {"id": "MSFT"},
                {"id": "HES", "question": "Will HES go up?", "background": "Hess Corp."},
            ]
        )
        result_df = fetch_all_stock(dfq)
        hes_rows = result_df[result_df["id"] == "HES"]
        assert len(hes_rows) == 1
        hes = hes_rows.iloc[0]
        assert bool(hes["resolved"]) is True
        assert hes["question"] == "Will HES go up?"
        assert hes["background"] == "Hess Corp."
        assert hes["freeze_datetime_value"] == "N/A"

    @patch("questions.yfinance.fetch.main.time.sleep")
    @patch("questions.yfinance.fetch.main.get_sp500_tickers")
    @patch("questions.yfinance.fetch.main.fetch_one_stock")
    def test_stock_in_sp500_and_fetch_fails_not_in_result(self, mock_fetch, mock_sp500, mock_sleep):
        """A stock in S&P 500 that fails to fetch doesn't appear in results."""
        mock_sp500.return_value = ["AAPL", "MSFT"]
        mock_fetch.return_value = (None, None)

        dfq = pd.DataFrame({"id": ["AAPL"]})
        result_df = fetch_all_stock(dfq)
        assert result_df.empty or "AAPL" not in result_df["id"].values

    @patch("questions.yfinance.fetch.main.time.sleep")
    @patch("questions.yfinance.fetch.main.dates")
    @patch("questions.yfinance.fetch.main.yf")
    @patch("questions.yfinance.fetch.main.get_sp500_tickers")
    @patch("questions.yfinance.fetch.main.fetch_one_stock")
    def test_stock_not_in_sp500_but_fetch_succeeds_not_resolved(
        self, mock_fetch, mock_sp500, mock_yf, mock_dates, mock_sleep
    ):
        """A stock not in S&P 500 that returns data has resolved=False."""
        mock_sp500.return_value = ["AAPL"]
        hist = pd.DataFrame({"Close": [100.0], "Date": [pd.Timestamp("2026-04-12")]})
        mock_fetch.return_value = ("Hess Corporation", hist)
        mock_yf.Ticker.return_value.info.get.return_value = "N/A"
        mock_dates.get_datetime_now.return_value = "2026-04-13T00:00:00Z"

        dfq = pd.DataFrame({"id": ["AAPL", "HES"]})
        result_df = fetch_all_stock(dfq)
        hes_rows = result_df[result_df["id"] == "HES"]
        assert len(hes_rows) == 1
        assert bool(hes_rows.iloc[0]["resolved"]) is False


class TestUpdateQuestionsDelistedStock:
    """Test that delisted stocks don't wipe out existing question data."""

    @patch("questions.yfinance.update_questions.main.create_renamed_ticker_resolution_files")
    @patch("questions.yfinance.update_questions.main.create_resolution_file")
    def test_delisted_stock_preserves_existing_fields(self, mock_create_res, mock_create_renamed):
        """When a delisted stock is updated, existing fields like question are preserved."""
        dfq = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "Will AAPL's market close price go up?",
                    "background": "Apple Inc.",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "200.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                },
                {
                    "id": "HES",
                    "question": "Will HES's market close price go up?",
                    "background": "Hess Corporation is an oil company.",
                    "url": "https://finance.yahoo.com/quote/HES",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "150.0",
                    "freeze_datetime_value_explanation": "The latest market close price of HES.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                },
            ]
        )

        # Simulate fetch output: both entries are complete records.
        dff = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "Will AAPL's market close price go up?",
                    "background": "Apple Inc.",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "200.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": "2026-04-16T00:00:00Z",
                    "probability": 200.0,
                },
                {
                    "id": "HES",
                    "question": "Will HES's market close price go up?",
                    "background": "Hess Corporation is an oil company.",
                    "url": "https://finance.yahoo.com/quote/HES",
                    "resolved": True,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "N/A",
                    "freeze_datetime_value_explanation": "The latest market close price of HES.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": "2026-04-16T00:00:00Z",
                    "probability": float("nan"),
                },
            ]
        )

        result = update_questions(dfq, dff)
        hes = result[result["id"] == "HES"].iloc[0]
        assert bool(hes["resolved"]) is True
        assert hes["question"] == "Will HES's market close price go up?"
        assert hes["background"] == "Hess Corporation is an oil company."
        assert hes["freeze_datetime_value"] == "N/A"

    @patch("questions.yfinance.update_questions.main.create_renamed_ticker_resolution_files")
    @patch("questions.yfinance.update_questions.main.create_resolution_file")
    def test_active_stock_updates_all_fields(self, mock_create_res, mock_create_renamed):
        """When an active stock is updated, all fields from fetch are applied."""
        dfq = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "Old question",
                    "background": "Old background",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": [7, 30],
                    "freeze_datetime_value": "100.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                }
            ]
        )

        dff = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "New question",
                    "background": "New background",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": constants.FORECAST_HORIZONS_IN_DAYS,
                    "freeze_datetime_value": "200.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": "2026-04-16T00:00:00Z",
                    "probability": 200.0,
                }
            ]
        )

        result = update_questions(dfq, dff)
        aapl = result[result["id"] == "AAPL"].iloc[0]
        assert aapl["question"] == "New question"
        assert aapl["background"] == "New background"


class TestTickerRenameResolution:
    """Test that renamed tickers are handled correctly in update_questions."""

    @patch("questions.yfinance.update_questions.main.create_renamed_ticker_resolution_files")
    @patch("questions.yfinance.update_questions.main.create_resolution_file")
    def test_renamed_ticker_skipped_in_main_loop(self, mock_create_res, mock_create_renamed):
        """create_resolution_file should not be called for original tickers in TICKER_RENAMES."""
        dfq = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "Will AAPL go up?",
                    "background": "Apple Inc.",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "200.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                },
                {
                    "id": "FI",
                    "question": "Will FI go up?",
                    "background": "Fiserv.",
                    "url": "https://finance.yahoo.com/quote/FI",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "N/A",
                    "freeze_datetime_value_explanation": "The latest market close price of FI.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                },
            ]
        )

        dff = pd.DataFrame(
            [
                {
                    "id": "AAPL",
                    "question": "Will AAPL go up?",
                    "background": "Apple Inc.",
                    "url": "https://finance.yahoo.com/quote/AAPL",
                    "resolved": False,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "200.0",
                    "freeze_datetime_value_explanation": "The latest market close price of AAPL.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": "2026-04-16T00:00:00Z",
                    "probability": 200.0,
                },
                {
                    "id": "FI",
                    "question": "Will FI go up?",
                    "background": "Fiserv.",
                    "url": "https://finance.yahoo.com/quote/FI",
                    "resolved": True,
                    "forecast_horizons": [7, 30, 90],
                    "freeze_datetime_value": "N/A",
                    "freeze_datetime_value_explanation": "The latest market close price of FI.",
                    "market_info_resolution_criteria": "N/A",
                    "market_info_open_datetime": "N/A",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                    "fetch_datetime": "2026-04-16T00:00:00Z",
                    "probability": float("nan"),
                },
            ]
        )

        result = update_questions(dfq, dff)

        # create_resolution_file should only be called for AAPL, not FI
        called_ids = [c.args[0]["id"] for c in mock_create_res.call_args_list]
        assert "AAPL" in called_ids
        assert "FI" not in called_ids

        # But FI should still be updated in dfq (resolved=True flows through)
        fi_row = result[result["id"] == "FI"].iloc[0]
        assert bool(fi_row["resolved"]) is True

    @patch("questions.yfinance.update_questions.main.gcp.storage.upload")
    @patch("questions.yfinance.update_questions.main.gcp.storage.download_no_error_message_on_404")
    @patch("questions.yfinance.update_questions.main.get_historical_prices")
    def test_renamed_ticker_fetches_replacement_data(
        self, mock_get_hist, mock_download, mock_upload
    ):
        """Data is fetched using replacement_ticker and written with original_ticker as ID."""
        from questions.yfinance.update_questions.main import (
            create_renamed_ticker_resolution_files,
        )

        mock_download.return_value = None  # no existing file

        mock_get_hist.return_value = pd.DataFrame(
            {
                "id": ["FISV", "FISV"],
                "date": ["2026-04-14", "2026-04-15"],
                "value": [220.0, 221.0],
            }
        )

        create_renamed_ticker_resolution_files("5y")

        # get_historical_prices should be called with replacement tickers
        call_tickers = [c.args[1] for c in mock_get_hist.call_args_list]
        assert "FISV" in call_tickers
        assert "MRSH" in call_tickers

        # Upload should write to yfinance/{original}.jsonl
        upload_filenames = [c[1]["filename"] for c in mock_upload.call_args_list]
        assert "yfinance/FI.jsonl" in upload_filenames
        assert "yfinance/MMC.jsonl" in upload_filenames

    @patch("questions.yfinance.update_questions.main.finalize_resolution_file")
    @patch("questions.yfinance.update_questions.main.gcp.storage.upload")
    @patch("questions.yfinance.update_questions.main.gcp.storage.download_no_error_message_on_404")
    @patch("questions.yfinance.update_questions.main.get_historical_prices")
    def test_renamed_ticker_not_finalized(
        self, mock_get_hist, mock_download, mock_upload, mock_finalize
    ):
        """finalize_resolution_file should not be called for renamed tickers."""
        from questions.yfinance.update_questions.main import (
            create_renamed_ticker_resolution_files,
        )

        mock_download.return_value = None

        mock_get_hist.return_value = pd.DataFrame(
            {
                "id": ["FISV", "FISV"],
                "date": ["2026-04-14", "2026-04-15"],
                "value": [220.0, 221.0],
            }
        )

        create_renamed_ticker_resolution_files("5y")

        mock_finalize.assert_not_called()


class TestFinalizeResolutionFile:
    """Test forward-filling a resolution file to cover resolution dates up to yesterday."""

    def test_forward_fills_to_yesterday(self, freeze_today):
        """The finalized resolution file extends to yesterday, not into the future."""
        freeze_today(date(2026, 4, 13))

        df = pd.DataFrame(
            {
                "id": ["HES", "HES", "HES"],
                "date": ["2025-05-01", "2025-05-02", "2025-05-03"],
                "value": [150.0, 151.0, 149.0],
            }
        )

        result = finalize_resolution_file(df)

        last_date = pd.to_datetime(result["date"]).max().date()
        assert last_date == date(2026, 4, 12)  # yesterday

        # All forward-filled values should be the last known price
        final_rows = result[pd.to_datetime(result["date"]).dt.date > date(2025, 5, 3)]
        assert (final_rows["value"].astype(float) == 149.0).all()

    def test_empty_df_returns_empty(self):
        """Empty input returns empty output."""
        df = pd.DataFrame(columns=["id", "date", "value"])
        result = finalize_resolution_file(df)
        assert result.empty
