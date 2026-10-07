"""Tests for yfinance relative-return pair questions."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from _fb_types import NullifiedQuestion
from helpers import constants
from helpers import yfinance as yfinance_helpers
from sources import yfinance
from sources.yfinance import YfinanceSource
from sources.yfinance_questions import YFINANCE_PAIRS
from tests.conftest import make_forecast_df, make_question_df, make_yfinance_fetch_df


def _series(ticker, start, values):
    """Daily [id, date, value] frame with string dates, like a resolution file on disk."""
    dates_ = pd.date_range(start=start, periods=len(values)).strftime("%Y-%m-%d")
    return pd.DataFrame({"id": ticker, "date": dates_, "value": values})


class TestPairIds:
    def test_is_pair_id(self):
        assert yfinance_helpers.is_pair_id("AAPL_MSFT")
        assert not yfinance_helpers.is_pair_id("AAPL")
        assert not yfinance_helpers.is_pair_id(("AAPL_MSFT", "A_B"))  # a tuple id is never a pair


class TestPairQuestionText:
    def test_keeps_date_placeholders_for_later_formatting(self):
        """The text mixes the stock labels with literal {date} placeholders; the f-string must
        keep them."""
        text = yfinance._pair_question_text("Apple Inc. (AAPL)", "AT&T Inc. (T)")
        assert text.startswith(
            "Will Apple Inc. (AAPL) have a higher rate of return than AT&T Inc. (T) between "
            "{forecast_due_date} and {resolution_date}?"
        )
        assert text.count("{resolution_date}") == 1
        assert text.count("{forecast_due_date}") == 2
        assert "market close price on {forecast_due_date}, minus one" in text
        # The shared yfinance resolution criteria already state the closed-market rule.
        assert "If the market is closed" not in text

    def test_background_leads_each_summary_with_its_ticker(self):
        text = yfinance._pair_background("AAPL", "Phones.", "MSFT", "PCs.")
        assert text == "AAPL: Phones.\n\nMSFT: PCs."

    def test_url_joins_both_quote_pages(self):
        assert yfinance._pair_url("AAPL", "MSFT") == (
            "https://finance.yahoo.com/quote/AAPL and https://finance.yahoo.com/quote/MSFT"
        )


class TestPairFreezeValue:
    def test_both_tickers_with_30_day_change(self):
        # 31 rows: 2026-02-15 .. 2026-03-17; the value 30 days before the last row is row 0.
        x = _series("AAPL", "2026-02-15", [100.0] * 30 + [110.0])
        y = _series("MSFT", "2026-02-15", [200.0] * 30 + [190.0])
        assert yfinance._pair_freeze_value("AAPL", x, "MSFT", y) == (
            "AAPL: 110.00 (+10.0% over the last 30 days); "
            "MSFT: 190.00 (-5.0% over the last 30 days)"
        )

    def test_short_history_omits_the_change(self):
        x = _series("AAPL", "2026-03-16", [100.0, 110.0])  # two sessions, ends 2026-03-17
        y = _series("MSFT", "2026-02-15", [200.0] * 31)  # 31 sessions, ends 2026-03-17
        assert yfinance._pair_freeze_value("AAPL", x, "MSFT", y) == (
            "AAPL: 110.00; MSFT: 200.00 (+0.0% over the last 30 days)"
        )

    @pytest.mark.parametrize("missing", [None, pd.DataFrame(columns=["id", "date", "value"])])
    def test_missing_ticker_is_not_available(self, missing):
        y = _series("MSFT", "2026-02-15", [200.0] * 31)
        assert yfinance._pair_freeze_value("AAPL", missing, "MSFT", y) == "N/A"
        assert yfinance._pair_freeze_value("AAPL", y, "MSFT", missing) == "N/A"

    def test_series_ending_on_different_dates_is_not_available(self):
        """A stale file next to a current one would quote closes from two different sessions."""
        x = _series("AAPL", "2026-03-10", [100.0] * 7)  # ends 2026-03-16
        y = _series("MSFT", "2026-03-10", [200.0] * 8)  # ends 2026-03-17
        assert yfinance._pair_freeze_value("AAPL", x, "MSFT", y) == "N/A"

    def test_string_values_are_parsed(self):
        """Resolution files on disk hold values as object dtype; strings must still compute."""
        x = _series("AAPL", "2026-03-16", ["100.0", "110.0"])
        y = _series("MSFT", "2026-03-16", ["200.0", "200.0"])
        assert yfinance._pair_freeze_value("AAPL", x, "MSFT", y) == "AAPL: 110.00; MSFT: 200.00"


class TestPairRatioSeries:
    def test_ratio_on_shared_dates_only(self):
        dfr = pd.DataFrame(
            {
                "id": ["AAPL", "AAPL", "MSFT", "MSFT", "MSFT"],
                "date": pd.to_datetime(
                    ["2026-01-01", "2026-01-02", "2026-01-01", "2026-01-02", "2026-01-03"]
                ),
                "value": [100.0, 110.0, 200.0, 200.0, 210.0],
            }
        )
        out = yfinance_helpers.pair_ratio_series(dfr, "AAPL_MSFT")
        assert out.columns.tolist() == ["id", "date", "value"]
        assert out["id"].tolist() == ["AAPL_MSFT", "AAPL_MSFT"]
        assert out["date"].tolist() == list(pd.to_datetime(["2026-01-01", "2026-01-02"]))
        assert out["value"].tolist() == [0.5, 0.55]

    def test_unparseable_and_zero_denominators_are_dropped(self):
        dfr = pd.DataFrame(
            {
                "id": ["AAPL", "AAPL", "MSFT", "MSFT"],
                "date": pd.to_datetime(["2026-01-01", "2026-01-02"] * 2),
                "value": ["100", "N/A", "0", "200"],
            }
        )
        out = yfinance_helpers.pair_ratio_series(dfr, "AAPL_MSFT")
        assert out.empty

    def test_missing_ticker_gives_empty_frame_with_columns(self):
        dfr = pd.DataFrame(
            {"id": ["AAPL"], "date": pd.to_datetime(["2026-01-01"]), "value": [100.0]}
        )
        out = yfinance_helpers.pair_ratio_series(dfr, "AAPL_MSFT")
        assert out.empty
        assert out.columns.tolist() == ["id", "date", "value"]


class TestPairList:
    """Structural checks over the whole committed list. No test names a pair."""

    def test_each_pair_is_two_distinct_tickers_without_underscores(self):
        for pair in YFINANCE_PAIRS:
            assert len(pair) == 2
            x, y = pair
            assert isinstance(x, str) and isinstance(y, str)
            assert x != y
            assert "_" not in x and "_" not in y

    def test_no_duplicate_pairs_in_either_order(self):
        assert len({tuple(sorted(p)) for p in YFINANCE_PAIRS}) == len(YFINANCE_PAIRS)

    def test_both_orientations_occur(self):
        assert any(x < y for x, y in YFINANCE_PAIRS)
        assert any(x > y for x, y in YFINANCE_PAIRS)


def _ticker_with(close, name="Apple Inc.", summary="A company."):
    ticker = MagicMock()
    ticker.info = {"longName": name, "longBusinessSummary": summary}
    ticker.history.return_value = pd.DataFrame(
        {"Date": pd.to_datetime(["2026-03-17"]), "Close": [close]}
    ).set_index("Date")
    return ticker


class TestFetchWithPairs:
    @patch("yfinance.Ticker")
    @patch.object(YfinanceSource, "_get_sp500_tickers", return_value=["AAPL"])
    def test_pair_ids_are_never_fetched(
        self, _mock_tickers, mock_ticker_cls, yfinance_source, freeze_today
    ):
        freeze_today(date(2026, 3, 18))
        mock_ticker_cls.return_value = _ticker_with(254.23)
        dfq = make_question_df([{"id": "AAPL"}, {"id": "AAPL_MSFT"}])

        dff = yfinance_source.fetch(dfq=dfq)

        fetched = [call.args[0] for call in mock_ticker_cls.call_args_list]
        assert "AAPL_MSFT" not in fetched
        assert "AAPL_MSFT" not in dff["id"].values


class TestUpdatePairPass:
    """update() upserts one row per pair from the tickers' resolution series."""

    @pytest.fixture()
    def source(self, yfinance_source):
        yfinance_source.pairs = [("AAPL", "MSFT")]
        return yfinance_source

    @staticmethod
    def _dff():
        return make_yfinance_fetch_df(
            [
                {"id": "AAPL", "background": "Phones.", "company_name": "Apple Inc."},
                {"id": "MSFT", "background": "PCs.", "company_name": "Microsoft Corporation"},
            ]
        )

    @staticmethod
    def _existing():
        return {
            "AAPL": _series("AAPL", "2026-02-15", [100.0] * 30 + [110.0]),
            "MSFT": _series("MSFT", "2026-02-15", [200.0] * 30 + [190.0]),
        }

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_builds_the_pair_row(self, _mock_build, source, freeze_today):
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df([{"id": "AAPL"}, {"id": "MSFT"}])

        result = source.update(dfq, self._dff(), existing_resolution_files=self._existing())

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["question"].startswith(
            "Will Apple Inc. (AAPL) have a higher rate of return than Microsoft Corporation "
            "(MSFT) between {forecast_due_date} and {resolution_date}?"
        )
        assert row["background"] == "AAPL: Phones.\n\nMSFT: PCs."
        assert row["url"] == (
            "https://finance.yahoo.com/quote/AAPL and https://finance.yahoo.com/quote/MSFT"
        )
        assert bool(row["resolved"]) is False
        assert row["forecast_horizons"] == constants.FORECAST_HORIZONS_IN_DAYS
        assert row["freeze_datetime_value"] == (
            "AAPL: 110.00 (+10.0% over the last 30 days); "
            "MSFT: 190.00 (-5.0% over the last 30 days)"
        )
        assert row["freeze_datetime_value_explanation"] == yfinance._PAIR_FREEZE_VALUE_EXPLANATION
        assert row["market_info_resolution_criteria"] == "N/A"
        assert "AAPL_MSFT" not in result.resolution_files

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_uses_series_built_this_run_over_existing(self, mock_build, source, freeze_today):
        freeze_today(date(2026, 3, 18))
        fresh = _series("AAPL", "2026-03-17", [999.0])
        mock_build.side_effect = lambda question, **kwargs: (
            fresh if question["id"] == "AAPL" else None
        )
        dfq = make_question_df([{"id": "AAPL"}, {"id": "MSFT"}])

        result = source.update(dfq, self._dff(), existing_resolution_files=self._existing())

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["freeze_datetime_value"].startswith("AAPL: 999.00;")

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_missing_ticker_series_gives_na_freeze_value(self, _mock_build, source, freeze_today):
        freeze_today(date(2026, 3, 18))
        existing = self._existing()
        del existing["MSFT"]
        dfq = make_question_df([{"id": "AAPL"}, {"id": "MSFT"}])

        result = source.update(dfq, self._dff(), existing_resolution_files=existing)

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["freeze_datetime_value"] == "N/A"

    @pytest.mark.parametrize(
        "retired_ticker_setup",
        ["resolved_in_bank", "nullified", "renamed_original"],
    )
    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    @patch.object(YfinanceSource, "_forward_fill_existing", return_value=None)
    def test_pair_is_resolved_when_a_ticker_is_retired(
        self, _ffill, _mock_build, retired_ticker_setup, yfinance_source, freeze_today
    ):
        freeze_today(date(2026, 3, 18))
        if retired_ticker_setup == "resolved_in_bank":
            retired = "MSFT"
            dfq = make_question_df([{"id": "AAPL"}, {"id": retired, "resolved": True}])
            dff = make_yfinance_fetch_df([{"id": "AAPL"}, {"id": retired, "resolved": True}])
        elif retired_ticker_setup == "nullified":
            retired = yfinance_source.nullified_questions[0].id
            dfq = make_question_df([{"id": "AAPL"}, {"id": retired}])
            dff = make_yfinance_fetch_df([{"id": "AAPL"}, {"id": retired}])
        else:
            retired = yfinance_source.ticker_renames[0]["original_ticker"]
            dfq = make_question_df([{"id": "AAPL"}, {"id": retired}])
            dff = make_yfinance_fetch_df([{"id": "AAPL"}])
        yfinance_source.pairs = [tuple(sorted(["AAPL", retired]))]

        with patch.object(YfinanceSource, "_get_historical_prices", return_value=None):
            result = yfinance_source.update(dfq, dff)

        pair_id = "_".join(sorted(["AAPL", retired]))
        row = result.dfq[result.dfq["id"] == pair_id].iloc[0]
        assert bool(row["resolved"]) is True

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_ticker_missing_from_fetch_frame_keeps_its_bank_summary(
        self, _mock_build, source, freeze_today
    ):
        """A ticker that failed to fetch tonight must not wipe its summary from every pair."""
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df(
            [{"id": "AAPL", "background": "Phones."}, {"id": "MSFT", "background": "PCs."}]
        )
        dff = make_yfinance_fetch_df([{"id": "AAPL", "background": "Phones."}])

        result = source.update(dfq, dff, existing_resolution_files=self._existing())

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["background"] == "AAPL: Phones.\n\nMSFT: PCs."

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_ticker_missing_from_fetch_keeps_the_pair_question_text(
        self, _mock_build, source, freeze_today
    ):
        """The question names both companies, so a ticker with no fetch row tonight would lose
        its name; the pair keeps the text its bank row already has instead."""
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df(
            [
                {"id": "AAPL", "question": "Will AAPL go up?"},
                {"id": "MSFT", "question": "Will MSFT go up?"},
                {"id": "AAPL_MSFT", "question": "Will Apple..."},
            ]
        )
        dff = make_yfinance_fetch_df([{"id": "AAPL", "company_name": "Apple Inc."}])

        result = source.update(dfq, dff, existing_resolution_files=self._existing())

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["question"] == "Will Apple..."

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_new_pair_with_a_nameless_ticker_names_the_tickers_alone(
        self, _mock_build, source, freeze_today
    ):
        """No fetch row for MSFT and no bank row for the pair yet: the text falls back to the
        tickers until a night when both names are known."""
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df([{"id": "AAPL"}, {"id": "MSFT"}])
        dff = make_yfinance_fetch_df([{"id": "AAPL", "company_name": "Apple Inc."}])

        result = source.update(dfq, dff, existing_resolution_files=self._existing())

        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["question"].startswith("Will AAPL have a higher rate of return than MSFT ")

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_ticker_not_in_fetch_frame_gets_na_background(
        self, _mock_build, yfinance_source, freeze_today
    ):
        """A ticker with neither a fetch row nor a bank row has no summary to fall back to."""
        freeze_today(date(2026, 3, 18))
        yfinance_source.pairs = [("AAPL", "FISV")]
        dfq = make_question_df([{"id": "AAPL"}])
        dff = make_yfinance_fetch_df([{"id": "AAPL", "background": "Phones."}])

        with patch.object(YfinanceSource, "_get_historical_prices", return_value=None):
            result = yfinance_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "AAPL_FISV"].iloc[0]
        assert row["background"] == "AAPL: Phones.\n\nFISV: N/A"

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_rerun_updates_the_existing_pair_row_in_place(self, _mock_build, source, freeze_today):
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df(
            [{"id": "AAPL"}, {"id": "MSFT"}, {"id": "AAPL_MSFT", "freeze_datetime_value": "old"}]
        )

        result = source.update(dfq, self._dff(), existing_resolution_files=self._existing())

        assert (result.dfq["id"] == "AAPL_MSFT").sum() == 1
        row = result.dfq[result.dfq["id"] == "AAPL_MSFT"].iloc[0]
        assert row["freeze_datetime_value"] != "old"
        assert result.dfq["id"].tolist()[:2] == ["AAPL", "MSFT"]

    @patch.object(YfinanceSource, "_build_resolution_df", return_value=None)
    def test_no_pairs_means_no_new_rows(self, _mock_build, yfinance_source, freeze_today):
        freeze_today(date(2026, 3, 18))
        dfq = make_question_df([{"id": "AAPL"}])

        result = yfinance_source.update(dfq, make_yfinance_fetch_df([{"id": "AAPL"}]))

        assert result.dfq["id"].tolist() == ["AAPL"]


def _dfr(**prices):
    """Build dfr from {ticker: {date: value}}."""
    rows = [
        {"id": ticker, "date": pd.Timestamp(day), "value": value}
        for ticker, by_date in prices.items()
        for day, value in by_date.items()
    ]
    return pd.DataFrame(rows)


class TestResolvePairs:
    DUE = "2026-01-01"
    RES = "2026-01-08"

    def _resolve(self, yfinance_source, dfr, rows):
        df = make_forecast_df(
            [
                {"source": "yfinance", "forecast_due_date": self.DUE, "resolution_date": self.RES}
                | row
                for row in rows
            ]
        )
        dfq = make_question_df([{"id": i} for i in dfr["id"].unique()])
        result, _ = yfinance_source.resolve(df, dfq, dfr, forecast_due_date=date(2026, 1, 1))
        return result

    def test_x_outperforms_y_resolves_yes(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 120.0}, MSFT={self.DUE: 200.0, self.RES: 210.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 1.0
        assert bool(result.iloc[0]["resolved"]) is True

    def test_y_outperforms_x_resolves_no(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 105.0}, MSFT={self.DUE: 200.0, self.RES: 240.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 0.0

    def test_equal_returns_resolve_no(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 110.0}, MSFT={self.DUE: 200.0, self.RES: 220.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 0.0

    def test_frozen_delisted_ticker_keeps_its_last_price(self, yfinance_source):
        """A delisted ticker's file is forward-filled with its final close; its return freezes."""
        dfr = _dfr(
            AAPL={self.DUE: 100.0, self.RES: 100.0},  # frozen at the last close
            MSFT={self.DUE: 200.0, self.RES: 190.0},
        )
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 1.0

    def test_both_tickers_frozen_is_a_tie(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 100.0}, MSFT={self.DUE: 200.0, self.RES: 200.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 0.0

    def test_pair_is_nullified_when_a_ticker_was_delisted_by_the_due_date(self, yfinance_source):
        """Asked after one stock had already stopped trading, the pair is nullified like the
        single-ticker question would be, not resolved against a dead price."""
        yfinance_source.nullified_questions = [
            NullifiedQuestion(id="AAPL", nullification_start_date=date(2026, 1, 1))
        ]
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 100.0}, MSFT={self.DUE: 200.0, self.RES: 190.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert pd.isna(result.iloc[0]["resolved_to"])
        assert bool(result.iloc[0]["resolved"]) is True

    def test_pair_resolves_when_the_delisting_came_after_the_due_date(self, yfinance_source):
        """Asked while both stocks traded, the pair resolves against the frozen final close."""
        yfinance_source.nullified_questions = [
            NullifiedQuestion(id="AAPL", nullification_start_date=date(2026, 1, 2))
        ]
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 100.0}, MSFT={self.DUE: 200.0, self.RES: 190.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert result.iloc[0]["resolved_to"] == 1.0

    def test_missing_value_on_a_date_leaves_pair_unresolved(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 120.0}, MSFT={self.DUE: 200.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])
        assert pd.isna(result.iloc[0]["resolved_to"])
        assert bool(result.iloc[0]["resolved"]) is False

    def test_pair_ticker_absent_from_dfr_raises(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 120.0})
        with pytest.raises(ValueError, match="Missing resolution values"):
            self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}])

    def test_single_tickers_still_resolve_alongside_pairs(self, yfinance_source):
        dfr = _dfr(AAPL={self.DUE: 100.0, self.RES: 120.0}, MSFT={self.DUE: 200.0, self.RES: 190.0})
        result = self._resolve(yfinance_source, dfr, [{"id": "AAPL_MSFT"}, {"id": "MSFT"}])
        by_id = result.set_index("id")["resolved_to"]
        assert by_id["AAPL_MSFT"] == 1.0
        assert by_id["MSFT"] == 0.0
