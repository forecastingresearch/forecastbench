"""Tests for KalshiSource fetch/update logic."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from _fb_types import NullifiedQuestion, SourceQuestionBank
from _schemas import KalshiFetchFrame, QuestionFrame, ResolutionFrame
from curate_questions.create_question_set import main as create_question_set
from resolve._impute import impute_missing_forecasts
from resolve._prepare import check_and_prepare_forecast_file, set_resolution_dates
from resolve.explode_question_set import explode_question_set
from resolve.resolve_all import resolve_all
from sources.kalshi import KalshiSource, MarketNotFoundError
from sources.registry import SOURCES

from .conftest import (
    make_kalshi_api_market,
    make_kalshi_candlestick,
    make_kalshi_event,
    make_kalshi_fetch_df,
    make_question_df,
    make_question_set_df,
    make_resolution_df,
)


def _ts(year, month, day, hour=0):
    """Return the unix timestamp (seconds) for a UTC datetime."""
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp())


def _update_boundaries(today: date) -> dict[str, int | date]:
    """Return the timestamp and date boundaries pinned at the start of an update."""
    update_datetime = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    return {
        "candlesticks_end_ts": int(update_datetime.timestamp()),
        "yesterday": today - timedelta(days=1),
    }


def _discovered_market(
    category: str,
    event_ticker: str = "KXTEST",
    series_ticker: str = "KXTEST",
    needs_yes_label: bool = False,
    settlement_sources: list[dict[str, str | None]] | None = None,
) -> dict[str, object]:
    """Build retained Kalshi event metadata for fetch tests."""
    return {
        "category": category,
        "event_ticker": event_ticker,
        "needs_yes_label": needs_yes_label,
        "series_ticker": series_ticker,
        "settlement_sources": settlement_sources or [],
    }


# ---------------------------------------------------------------------------
# _get_resolved_market_value (pure, no mocking)
# ---------------------------------------------------------------------------


class TestGetResolvedMarketValue:
    """Tests for KalshiSource._get_resolved_market_value static method."""

    def test_yes_resolution(self):
        """'yes' result returns 1."""
        assert KalshiSource._get_resolved_market_value(make_kalshi_api_market(result="yes")) == 1

    def test_no_resolution(self):
        """'no' result returns 0."""
        assert KalshiSource._get_resolved_market_value(make_kalshi_api_market(result="no")) == 0

    def test_empty_result_is_nan(self):
        """Empty result returns NaN."""
        assert np.isnan(KalshiSource._get_resolved_market_value(make_kalshi_api_market(result="")))

    def test_scalar_result_is_nan(self):
        """A non yes/no result returns NaN."""
        result = KalshiSource._get_resolved_market_value(make_kalshi_api_market(result="scalar"))
        assert np.isnan(result)


# ---------------------------------------------------------------------------
# _is_resolved / _series_ticker (pure)
# ---------------------------------------------------------------------------


class TestMarketHelpers:
    """Tests for small Kalshi static helpers."""

    def test_is_resolved_finalized(self):
        """A finalized market is treated as resolved."""
        market = make_kalshi_api_market(status="finalized")
        assert KalshiSource._is_resolved(market) is True

    def test_is_resolved_non_terminal(self):
        """Markets that can still change are not yet resolved."""
        for status in [
            "initialized",
            "active",
            "inactive",
            "closed",
            "determined",
            "disputed",
            "amended",
        ]:
            assert KalshiSource._is_resolved(make_kalshi_api_market(status=status)) is False

    def test_series_ticker(self):
        """Series ticker is the prefix before the first dash."""
        assert KalshiSource._series_ticker("KXWCSPREAD-26JUN18CANQAT-CAN6") == "KXWCSPREAD"

    def test_resolution_criteria_joins_rules(self):
        """Primary and secondary rules are joined, empties dropped."""
        market = make_kalshi_api_market(rules_primary="Primary.", rules_secondary="Secondary.")
        assert KalshiSource._resolution_criteria(market) == "Primary. Secondary."

    def test_resolution_criteria_includes_settlement_sources(self):
        """Event-level outcome sources are appended with their provider URLs."""
        market = make_kalshi_api_market(rules_primary="Primary.", rules_secondary="Secondary.")
        settlement_sources = [
            {"name": "Library of Congress", "url": "https://www.congress.gov/"},
            {"name": "House Clerk", "url": "https://clerk.house.gov/"},
        ]

        criteria = KalshiSource._resolution_criteria(market, settlement_sources=settlement_sources)

        assert criteria == (
            "Primary. Secondary. Outcome verified from Library of Congress "
            "(https://www.congress.gov/); House Clerk (https://clerk.house.gov/)."
        )

    def test_resolution_criteria_preserves_stored_sources(self):
        """A capped fetch cannot remove a previously stored settlement source."""
        market = make_kalshi_api_market(rules_primary="Updated primary.", rules_secondary="")
        existing_criteria = (
            "Old primary. Outcome verified from Library of Congress " "(https://www.congress.gov/)."
        )

        criteria = KalshiSource._resolution_criteria(market, existing_criteria=existing_criteria)

        assert criteria == (
            "Updated primary. Outcome verified from Library of Congress "
            "(https://www.congress.gov/)."
        )

    def test_resolution_criteria_uses_url_when_source_name_is_missing(self):
        """A URL-only provider source remains usable resolution information."""
        market = make_kalshi_api_market(rules_primary="Primary.", rules_secondary="")

        criteria = KalshiSource._resolution_criteria(
            market,
            settlement_sources=[{"name": None, "url": "https://example.com/outcome"}],
        )

        assert criteria == "Primary. Outcome verified from https://example.com/outcome."

    def test_resolution_criteria_na_when_empty(self):
        """No rules yields 'N/A'."""
        market = make_kalshi_api_market(rules_primary="", rules_secondary="")
        assert KalshiSource._resolution_criteria(market) == "N/A"

    def test_resolution_datetime_prefers_settlement(self):
        """settlement_ts is preferred over expiration/close."""
        market = make_kalshi_api_market(settlement_ts="2026-01-13T05:00:00Z")
        assert KalshiSource._resolution_datetime(market).startswith("2026-01-13")


# ---------------------------------------------------------------------------
# _build_resolution_df (mock _get_market_candlesticks)
# ---------------------------------------------------------------------------


class TestBuildResolutionDf:
    """Tests for KalshiSource._build_resolution_df."""

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_already_up_to_date(self, mock_candles, kalshi_source, freeze_today):
        """Skips API call if existing data covers through yesterday."""
        freeze_today(date(2026, 1, 15))
        existing = make_resolution_df(
            [
                {"id": "KXTEST-001", "date": "2024-06-01", "value": 0.5},
                {"id": "KXTEST-001", "date": "2026-01-14", "value": 0.6},
            ]
        )
        market = make_kalshi_api_market()
        result = kalshi_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="N/A",
            existing_df=existing,
            **_update_boundaries(date(2026, 1, 15)),
        )

        assert result.equals(existing)
        mock_candles.assert_not_called()

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_basic_unresolved_market(self, mock_candles, kalshi_source, freeze_today):
        """Builds a valid time series from candlesticks for an unresolved market."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 10), close_dollars="0.40"),
            make_kalshi_candlestick(_ts(2026, 1, 12), close_dollars="0.60"),
        ]
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        assert result is not None
        assert (result["id"] == "KXTEST-001").all()
        ResolutionFrame.validate(result)
        # Midnight candles close the preceding UTC dates; missing dates are forward-filled.
        assert len(result) >= 5

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_empty_candles_returns_none(self, mock_candles, kalshi_source, freeze_today):
        """No candlesticks returns None."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = []
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )
        assert result is None

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_no_trade_candles_returns_none(self, mock_candles, kalshi_source, freeze_today):
        """Candlesticks with no trades (empty price) return None."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 10)),
            make_kalshi_candlestick(_ts(2026, 1, 12)),
        ]
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )
        assert result is None

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_forward_fills_gaps(self, mock_candles, kalshi_source, freeze_today):
        """Missing dates between candlesticks are forward-filled."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 10), close_dollars="0.30"),
            make_kalshi_candlestick(_ts(2026, 1, 14), close_dollars="0.80"),
        ]
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        dates_in_df = pd.to_datetime(result["date"]).dt.date.tolist()
        assert date(2026, 1, 11) in dates_in_df
        assert date(2026, 1, 12) in dates_in_df
        assert date(2026, 1, 13) in dates_in_df

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_resolved_truncates_at_resolution(self, mock_candles, kalshi_source, freeze_today):
        """Resolved market: data truncated at resolution date, final row has resolved value."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 10), close_dollars="0.40"),
            make_kalshi_candlestick(_ts(2026, 1, 12), close_dollars="0.60"),
            make_kalshi_candlestick(_ts(2026, 1, 14), close_dollars="0.90"),
        ]
        market = make_kalshi_api_market(status="finalized", result="yes")
        result = kalshi_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="2026-01-13T12:00:00+00:00",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        assert result is not None
        last_date = pd.to_datetime(result["date"].iloc[-1]).date()
        assert last_date == date(2026, 1, 13)
        assert float(result["value"].iloc[-1]) == 1.0
        all_dates = pd.to_datetime(result["date"]).dt.date
        assert all(d <= date(2026, 1, 13) for d in all_dates)

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_resolved_void_nan_last_row(self, mock_candles, kalshi_source, freeze_today):
        """Void resolution (empty result) on a terminal market: last row is NaN."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 10), close_dollars="0.40"),
            make_kalshi_candlestick(_ts(2026, 1, 12), close_dollars="0.60"),
        ]
        market = make_kalshi_api_market(status="finalized", result="")
        result = kalshi_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="2026-01-13T12:00:00+00:00",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        assert result is not None
        assert np.isnan(float(result["value"].iloc[-1]))

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_filters_future_candles(self, mock_candles, kalshi_source, freeze_today):
        """Candlesticks after yesterday's UTC boundary cannot enter the daily series."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 15), close_dollars="0.50"),
            make_kalshi_candlestick(_ts(2026, 1, 16), close_dollars="0.90"),
        ]
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        assert result is not None
        all_dates = pd.to_datetime(result["date"]).dt.date
        assert all(d <= date(2026, 1, 14) for d in all_dates)
        assert date(2026, 1, 15) not in all_dates.tolist()

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_exact_utc_boundary_closes_previous_day(
        self, mock_candles, kalshi_source, freeze_today
    ):
        """A candle ending exactly at UTC midnight supplies the preceding day's value."""
        freeze_today(date(2026, 1, 15))
        mock_candles.return_value = [
            make_kalshi_candlestick(_ts(2026, 1, 12), close_dollars="0.55"),
        ]
        result = kalshi_source._build_resolution_df(
            market=make_kalshi_api_market(),
            market_info_resolution_datetime="N/A",
            existing_df=None,
            **_update_boundaries(date(2026, 1, 15)),
        )

        result_dates = pd.to_datetime(result["date"]).dt.date
        assert result_dates.min() == date(2026, 1, 11)
        first_val = result.loc[result_dates == date(2026, 1, 11), "value"].iloc[0]
        assert float(first_val) == 0.55

    @patch.object(KalshiSource, "_get_market_candlesticks")
    def test_utc_cutoff_is_invariant_to_later_rebuild(
        self, mock_candles, kalshi_source, freeze_today
    ):
        """Post-midnight trading cannot retroactively change the prior UTC date."""
        freeze_today(date(2026, 7, 24))
        candles = [
            make_kalshi_candlestick(_ts(2026, 7, 23, hour=4), close_dollars="0.51"),
            make_kalshi_candlestick(_ts(2026, 7, 24, hour=0), close_dollars="0.50"),
            make_kalshi_candlestick(_ts(2026, 7, 24, hour=4), close_dollars="0.49"),
        ]
        mock_candles.side_effect = lambda _ticker, **kwargs: [
            candle for candle in candles if candle["end_period_ts"] <= kwargs["end_ts"]
        ]
        market = make_kalshi_api_market(open_time="2026-07-22T00:00:00Z")

        at_cutoff = kalshi_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="N/A",
            candlesticks_end_ts=_ts(2026, 7, 24, hour=0),
            yesterday=date(2026, 7, 23),
        )
        later_rebuild = kalshi_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="N/A",
            candlesticks_end_ts=_ts(2026, 7, 24, hour=6),
            yesterday=date(2026, 7, 23),
        )

        at_cutoff_value = at_cutoff.loc[
            pd.to_datetime(at_cutoff["date"]).dt.date == date(2026, 7, 23), "value"
        ].iloc[0]
        later_value = later_rebuild.loc[
            pd.to_datetime(later_rebuild["date"]).dt.date == date(2026, 7, 23), "value"
        ].iloc[0]
        assert float(at_cutoff_value) == 0.50
        assert float(later_value) == 0.50


# ---------------------------------------------------------------------------
# _call_search_endpoint (mock requests.get)
# ---------------------------------------------------------------------------


class TestCallSearchEndpoint:
    """Tests for KalshiSource._call_search_endpoint."""

    def _mock_response(self, events, cursor=None):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"events": events, "cursor": cursor}
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.kalshi.requests.get")
    def test_basic_returns_qualifying_tickers(self, mock_get, kalshi_source):
        """Returns qualifying tickers with category and parent routing metadata."""
        settlement_sources = [{"name": "Library of Congress", "url": "https://www.congress.gov/"}]
        events = [
            make_kalshi_event(
                category="Economics",
                settlement_sources=settlement_sources,
                markets=[
                    make_kalshi_api_market(ticker="A"),
                    make_kalshi_api_market(ticker="B"),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        tickers, cursor = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert tickers == {
            "A": _discovered_market(
                "Economics",
                needs_yes_label=True,
                settlement_sources=settlement_sources,
            ),
            "B": _discovered_market(
                "Economics",
                needs_yes_label=True,
                settlement_sources=settlement_sources,
            ),
        }
        assert cursor is None

    @patch("sources.kalshi.requests.get")
    def test_url_only_settlement_source_is_retained(self, mock_get, kalshi_source):
        """A qualifying event can use a settlement source that has only a URL."""
        events = [
            make_kalshi_event(
                settlement_sources=[{"url": "https://example.com/outcome"}],
                markets=[make_kalshi_api_market(ticker="A")],
            )
        ]
        mock_get.return_value = self._mock_response(events)

        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {
            "A": _discovered_market(
                "Economics",
                settlement_sources=[{"name": None, "url": "https://example.com/outcome"}],
            )
        }

    @patch("sources.kalshi.requests.get")
    def test_irrelevant_event_does_not_parse_settlement_sources(self, mock_get, kalshi_source):
        """Malformed sources cannot fail discovery when no child market qualifies."""
        events = [
            make_kalshi_event(
                settlement_sources=[{}],
                markets=[make_kalshi_api_market(ticker="low", volume_fp="0")],
            )
        ]
        mock_get.return_value = self._mock_response(events)

        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {}

    @patch("sources.kalshi.requests.get")
    def test_terminal_null_event_page_is_empty(self, mock_get, kalshi_source):
        """Kalshi's terminal [null] pagination sentinel ends discovery cleanly."""
        mock_get.return_value = self._mock_response([None], cursor=None)

        tickers, cursor = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {}
        assert cursor is None

    @patch("sources.kalshi.requests.get")
    def test_nonterminal_null_event_fails_loudly(self, mock_get, kalshi_source):
        """A null event is accepted only as the final page's sole sentinel."""
        mock_get.return_value = self._mock_response([None], cursor="next")

        with pytest.raises(ValueError, match="contains a null event"):
            kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

    @patch("sources.kalshi.requests.get")
    def test_skips_market_with_mismatched_parent_event(self, mock_get, kalshi_source):
        """A market cannot inherit routing metadata from a different parent event."""
        events = [
            make_kalshi_event(
                event_ticker="EVENT-A",
                markets=[make_kalshi_api_market(ticker="A", event_ticker="EVENT-B")],
            )
        ]
        mock_get.return_value = self._mock_response(events)

        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {}

    @patch("sources.kalshi.requests.get")
    def test_filters_non_binary(self, mock_get, kalshi_source):
        """Scalar markets are excluded."""
        events = [
            make_kalshi_event(
                category="Economics",
                markets=[
                    make_kalshi_api_market(ticker="bin", market_type="binary"),
                    make_kalshi_api_market(ticker="scal", market_type="scalar"),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert set(ids) == {"bin"}

    @patch("sources.kalshi.requests.get")
    def test_filters_non_active_child_market(self, mock_get, kalshi_source):
        """An open event contributes only child markets that are themselves active."""
        events = [
            make_kalshi_event(
                markets=[
                    make_kalshi_api_market(ticker="active", status="active"),
                    make_kalshi_api_market(ticker="closed", status="closed"),
                ]
            )
        ]
        mock_get.return_value = self._mock_response(events)

        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert set(ids) == {"active"}

    @patch("sources.kalshi.requests.get")
    def test_filtered_sibling_still_marks_shared_title(self, mock_get, kalshi_source):
        """A filtered sibling still makes the qualifying child's shared title need a label."""
        events = [
            make_kalshi_event(
                category="Economics",
                markets=[
                    make_kalshi_api_market(ticker="low", volume_fp="100.00"),
                    make_kalshi_api_market(ticker="ok", volume_fp="10000.00"),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert ids == {"ok": _discovered_market("Economics", needs_yes_label=True)}

    @patch("sources.kalshi.requests.get")
    def test_filters_low_open_interest(self, mock_get, kalshi_source):
        """Markets with open interest below the floor are excluded."""
        events = [
            make_kalshi_event(
                category="Economics",
                markets=[
                    make_kalshi_api_market(ticker="low", open_interest_fp="10.00"),
                    make_kalshi_api_market(ticker="ok", open_interest_fp="2000.00"),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert set(ids) == {"ok"}

    @patch("sources.kalshi.requests.get")
    def test_filters_close_before_min_resolution(self, mock_get, kalshi_source):
        """An early trading close remains an earliest plausible resolution bound."""
        events = [
            make_kalshi_event(
                category="Economics",
                markets=[
                    make_kalshi_api_market(ticker="soon", close_time="2026-01-20T00:00:00Z"),
                    make_kalshi_api_market(ticker="ok", close_time="2026-03-01T00:00:00Z"),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert set(ids) == {"ok"}

    @patch("sources.kalshi.requests.get")
    def test_filters_sports_event_before_min_despite_later_close(self, mock_get, kalshi_source):
        """A postponement close cannot hide a sports outcome expected before the minimum."""
        events = [
            make_kalshi_event(
                category="Sports",
                markets=[
                    make_kalshi_api_market(
                        ticker="postponement-bound",
                        occurrence_datetime="2026-07-25T05:00:00Z",
                        expected_expiration_time="2026-07-25T05:00:00Z",
                        close_time="2026-08-08T02:00:00Z",
                        latest_expiration_time="2026-08-08T02:00:00Z",
                    ),
                    make_kalshi_api_market(
                        ticker="aligned",
                        occurrence_datetime=None,
                        expected_expiration_time="2026-08-05T05:00:00Z",
                        close_time="2026-08-05T05:00:00Z",
                        latest_expiration_time="2026-08-08T02:00:00Z",
                    ),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)

        ids, _ = kalshi_source._call_search_endpoint(
            min_resolution_date=date(2026, 8, 2),
            max_resolution_date=date(2026, 8, 31),
        )

        assert set(ids) == {"aligned"}

    @patch("sources.kalshi.requests.get")
    def test_filters_stale_expected_expiration_after_latest_bound(self, mock_get, kalshi_source):
        """A stale child-contract expiration beyond the latest bound is rejected."""
        market = make_kalshi_api_market(
            ticker="stale-ladder",
            occurrence_datetime="2026-08-01T00:00:00Z",
            close_time="2026-08-01T00:00:00Z",
            latest_expiration_time="2026-08-08T00:00:00Z",
            expected_expiration_time="2027-01-01T00:00:00Z",
        )
        mock_get.return_value = self._mock_response([make_kalshi_event(markets=[market])])

        ids, _ = kalshi_source._call_search_endpoint(
            min_resolution_date=date(2026, 7, 1),
            max_resolution_date=date(2027, 12, 31),
        )

        assert ids == {}

    @patch("sources.kalshi.requests.get")
    def test_filters_close_after_max_resolution(self, mock_get, kalshi_source):
        """Either provider latest bound can exclude a market beyond the maximum date."""
        events = [
            make_kalshi_event(
                category="Economics",
                markets=[
                    make_kalshi_api_market(ticker="ok", close_time="2026-06-01T00:00:00Z"),
                    make_kalshi_api_market(ticker="far-close", close_time="2099-01-01T00:00:00Z"),
                    make_kalshi_api_market(
                        ticker="far-latest",
                        occurrence_datetime="2026-06-01T00:00:00Z",
                        expected_expiration_time="2026-06-01T00:00:00Z",
                        close_time="2026-06-01T00:00:00Z",
                        latest_expiration_time="2099-01-01T00:00:00Z",
                    ),
                ],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        ids, _ = kalshi_source._call_search_endpoint(
            min_resolution_date=date(2026, 1, 25),
            max_resolution_date=date(2028, 1, 1),
        )
        assert set(ids) == {"ok"}

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("expected_expiration_time", ""),
            ("latest_expiration_time", "not-a-date"),
            ("occurrence_datetime", "2026-12-01T00:00:00"),
        ],
    )
    @patch("sources.kalshi.requests.get")
    def test_filters_unusable_timing(self, mock_get, kalshi_source, field, value):
        """Empty, malformed, or timezone-naive timestamps are ineligible."""
        market = make_kalshi_api_market(**{field: value})
        mock_get.return_value = self._mock_response([make_kalshi_event(markets=[market])])

        ids, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert ids == {}

    @patch("sources.kalshi.requests.get")
    def test_any_category_included(self, mock_get, kalshi_source):
        """A liquid market in any category is included (no category whitelist)."""
        events = [
            make_kalshi_event(
                category="Sports",
                markets=[make_kalshi_api_market(ticker="sporty", volume_24h_fp="10.00")],
            )
        ]
        mock_get.return_value = self._mock_response(events)
        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert tickers == {"sporty": _discovered_market("Sports")}

    @patch("sources.kalshi.requests.get")
    def test_returns_ticker_to_category_mapping(self, mock_get, kalshi_source):
        """Each qualifying ticker retains its category and parent routing metadata."""
        events = [
            make_kalshi_event(
                category="Crypto",
                markets=[make_kalshi_api_market(ticker="btc")],
            ),
            make_kalshi_event(
                category="Politics",
                markets=[make_kalshi_api_market(ticker="election")],
            ),
        ]
        mock_get.return_value = self._mock_response(events)
        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))
        assert tickers == {
            "btc": _discovered_market("Crypto"),
            "election": _discovered_market("Politics"),
        }

    @patch("sources.kalshi.requests.get")
    def test_cursor_passed_through(self, mock_get, kalshi_source):
        """The next-page cursor is returned and an incoming cursor is sent in params."""
        mock_get.return_value = self._mock_response([], cursor="next_page")
        _, cursor = kalshi_source._call_search_endpoint(
            min_resolution_date=date(2026, 1, 25), cursor="cur1"
        )
        assert cursor == "next_page"
        assert mock_get.call_args.kwargs["params"]["cursor"] == "cur1"

    @patch("sources.kalshi.requests.get")
    def test_missing_category_uses_uncategorized(self, mock_get, kalshi_source):
        """A market remains eligible when its event omits the deprecated category field."""
        event = make_kalshi_event(markets=[make_kalshi_api_market(ticker="uncategorized")])
        del event["category"]
        mock_get.return_value = self._mock_response([event])

        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {"uncategorized": _discovered_market("Uncategorized")}

    @pytest.mark.parametrize(
        ("location", "missing_field"),
        [
            ("response", "events"),
            ("response", "cursor"),
            ("event", "markets"),
            ("event", "event_ticker"),
            ("event", "series_ticker"),
            ("event", "settlement_sources"),
        ],
    )
    @patch("sources.kalshi.requests.get")
    def test_missing_required_container_field_fails_loudly(
        self, mock_get, kalshi_source, location, missing_field
    ):
        """A malformed response or event raises because discovery completeness is unknown."""
        market = make_kalshi_api_market()
        event = make_kalshi_event(markets=[market])
        data = {"events": [event], "cursor": None}
        target = {"response": data, "event": event}[location]
        del target[missing_field]

        response = Mock()
        response.ok = True
        response.json.return_value = data
        mock_get.return_value = response

        with pytest.raises(
            ValueError,
            match=rf"Kalshi events API response is missing required field '{missing_field}'",
        ):
            kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

    @pytest.mark.parametrize(
        "missing_field",
        [
            "ticker",
            "event_ticker",
            "title",
            "status",
            "market_type",
            "volume_fp",
            "open_interest_fp",
            "close_time",
            "expected_expiration_time",
            "latest_expiration_time",
        ],
    )
    @patch("sources.kalshi.requests.get")
    def test_missing_required_market_field_fails_loudly(
        self, mock_get, kalshi_source, missing_field
    ):
        """A qualifying market missing a required field raises a clear error."""
        market = make_kalshi_api_market()
        del market[missing_field]
        mock_get.return_value = self._mock_response([make_kalshi_event(markets=[market])])

        with pytest.raises(
            ValueError,
            match=rf"Kalshi events API response is missing required field '{missing_field}'",
        ):
            kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

    @patch("sources.kalshi.requests.get")
    def test_ineligible_market_without_title_is_skipped(self, mock_get, kalshi_source):
        """An irrelevant title omission cannot abort discovery for eligible markets."""
        titleless = make_kalshi_api_market(ticker="titleless", volume_fp="100.00")
        del titleless["title"]
        eligible = make_kalshi_api_market(ticker="eligible", title="Eligible question")
        mock_get.return_value = self._mock_response(
            [make_kalshi_event(markets=[titleless, eligible])]
        )

        tickers, _ = kalshi_source._call_search_endpoint(min_resolution_date=date(2026, 1, 25))

        assert tickers == {"eligible": _discovered_market("Economics")}


# ---------------------------------------------------------------------------
# _get_market / _get_market_candlesticks (mock requests.get)
# ---------------------------------------------------------------------------


class TestGetMarket:
    """Tests for KalshiSource._get_market."""

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_returns_market_object(self, mock_get, mock_sleep, kalshi_source):
        """Unwraps and returns the 'market' object."""
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"market": make_kalshi_api_market(ticker="KXTEST-001")}
        mock_get.return_value = resp

        result = kalshi_source._get_market("KXTEST-001")
        assert result["ticker"] == "KXTEST-001"

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_live_404_falls_back_to_historical_market(self, mock_get, mock_sleep, kalshi_source):
        """An archived market is loaded from the historical detail endpoint."""
        live = Mock(status_code=404, ok=False)
        historical = Mock(status_code=200, ok=True)
        historical.json.return_value = {"market": make_kalshi_api_market(ticker="KXARCHIVED-001")}
        mock_get.side_effect = [live, historical]

        result = kalshi_source._get_market("KXARCHIVED-001")

        assert result["ticker"] == "KXARCHIVED-001"
        assert "/historical/markets/KXARCHIVED-001" in mock_get.call_args.args[0]

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_transient_dual_404_is_retried(self, mock_get, mock_sleep, kalshi_source):
        """A newly created market can become available during bounded 404 retries."""
        missing = Mock(status_code=404, ok=False)
        available = Mock(status_code=200, ok=True)
        available.json.return_value = {"market": make_kalshi_api_market(ticker="KXNEW-001")}
        mock_get.side_effect = [missing, missing, available]

        result = kalshi_source._get_market("KXNEW-001")

        assert result["ticker"] == "KXNEW-001"

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_dual_404_raises_market_not_found(self, mock_get, mock_sleep, kalshi_source):
        """Repeated absence from both API partitions is reported to update()."""
        missing = Mock(status_code=404, ok=False)
        mock_get.return_value = missing

        with pytest.raises(MarketNotFoundError):
            kalshi_source._get_market("KXGONE-001")


class TestGetMarketCandlesticks:
    """Tests for KalshiSource._get_market_candlesticks."""

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_builds_series_url_and_returns_candles(
        self, mock_get, mock_sleep, kalshi_source, freeze_today
    ):
        """Uses the series-derived URL and returns the candlesticks list."""
        freeze_today(date(2026, 1, 15))
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {
            "candlesticks": [make_kalshi_candlestick(_ts(2026, 1, 10), close_dollars="0.5")]
        }
        mock_get.return_value = resp

        result = kalshi_source._get_market_candlesticks(
            "KXWCSPREAD-26JUN18CANQAT-CAN6",
            start_ts=_ts(2026, 1, 9),
            end_ts=_ts(2026, 1, 15),
        )
        assert len(result) == 1
        url = mock_get.call_args[0][0]
        assert "/series/KXWCSPREAD/markets/KXWCSPREAD-26JUN18CANQAT-CAN6/candlesticks" in url
        assert mock_get.call_args.kwargs["params"]["period_interval"] == 60

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_historical_candles_are_normalized(self, mock_get, mock_sleep, kalshi_source):
        """Archived price.close values use the same shape as live close_dollars values."""
        live = Mock(status_code=404, ok=False)
        historical = Mock(status_code=200, ok=True)
        historical.json.return_value = {
            "candlesticks": [
                {
                    "end_period_ts": _ts(2026, 1, 10),
                    "price": {"close": "0.50"},
                }
            ]
        }
        mock_get.side_effect = [live, historical]

        candles = kalshi_source._get_market_candlesticks(
            "KXARCHIVED-001",
            start_ts=_ts(2026, 1, 9),
            end_ts=_ts(2026, 1, 15),
        )

        assert candles[0]["price"]["close_dollars"] == "0.50"
        assert "/historical/markets/KXARCHIVED-001/candlesticks" in mock_get.call_args.args[0]

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_long_hourly_history_is_fetched_in_bounded_windows(
        self, mock_get, mock_sleep, kalshi_source
    ):
        """Histories over Kalshi's 5,000-candle limit are merged without duplicates."""
        first = Mock()
        first.ok = True
        first.json.return_value = {
            "candlesticks": [
                make_kalshi_candlestick(1000, close_dollars="0.30"),
                make_kalshi_candlestick(2000, close_dollars="0.40"),
            ]
        }
        second = Mock()
        second.ok = True
        second.json.return_value = {
            "candlesticks": [
                make_kalshi_candlestick(2000, close_dollars="0.40"),
                make_kalshi_candlestick(3000, close_dollars="0.50"),
            ]
        }
        mock_get.side_effect = [first, second]
        end_ts = 100 + 5001 * 60 * 60

        candles = kalshi_source._get_market_candlesticks(
            "KXTEST-001",
            start_ts=100,
            end_ts=end_ts,
        )

        assert [candle["end_period_ts"] for candle in candles] == [1000, 2000, 3000]
        assert mock_get.call_count == 2
        first_params = mock_get.call_args_list[0].kwargs["params"]
        second_params = mock_get.call_args_list[1].kwargs["params"]
        assert first_params["end_ts"] - first_params["start_ts"] < 5000 * 60 * 60
        assert second_params["start_ts"] == first_params["end_ts"] + 1
        assert second_params["end_ts"] == end_ts

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.requests.get")
    def test_empty_candles(self, mock_get, mock_sleep, kalshi_source, freeze_today):
        """Missing candlesticks key returns empty list."""
        freeze_today(date(2026, 1, 15))
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"ticker": "KXTEST-001"}
        mock_get.return_value = resp
        assert (
            kalshi_source._get_market_candlesticks(
                "KXTEST-001",
                start_ts=_ts(2026, 1, 14),
                end_ts=_ts(2026, 1, 15),
            )
            == []
        )


# ---------------------------------------------------------------------------
# Request throttling
# ---------------------------------------------------------------------------


class TestRequestThrottling:
    """Tests that consecutive Kalshi API requests are paced across endpoints."""

    @patch("sources.kalshi.time.sleep")
    @patch("sources.kalshi.time.monotonic", side_effect=[100.0, 100.0, 100.04, 100.1])
    @patch("sources.kalshi.requests.get")
    def test_consecutive_requests_share_rate_limit(
        self, mock_get, _mock_monotonic, mock_sleep, kalshi_source
    ):
        """A market request followed by a candle request waits for the remaining interval."""
        market_response = Mock()
        market_response.ok = True
        market_response.status_code = 200
        market_response.json.return_value = {"market": make_kalshi_api_market(ticker="KXTEST-001")}
        candle_response = Mock()
        candle_response.ok = True
        candle_response.json.return_value = {"candlesticks": []}
        mock_get.side_effect = [market_response, candle_response]

        kalshi_source._get_market("KXTEST-001")
        kalshi_source._get_market_candlesticks(
            "KXTEST-001",
            start_ts=_ts(2026, 1, 14),
            end_ts=_ts(2026, 1, 15),
        )

        mock_sleep.assert_called_once_with(pytest.approx(0.06))
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# fetch() (mock _search_markets)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for KalshiSource.fetch."""

    @patch.object(KalshiSource, "_search_markets")
    def test_basic_fetch(self, mock_search, kalshi_source):
        """Returns sorted IDs with their structured parent identifiers."""
        mock_search.return_value = {
            "id_b": _discovered_market("Sports", "event_b", "series_b", needs_yes_label=True),
            "id_a": _discovered_market("Economics", "event_a", "series_a"),
            "id_c": _discovered_market("Crypto", "event_c", "series_c"),
        }
        dff = kalshi_source.fetch()

        assert dff["id"].tolist() == ["id_a", "id_b", "id_c"]
        assert dff["needs_yes_label"].tolist() == [False, True, False]
        assert dff[["event_ticker", "series_ticker"]].to_dict("records") == [
            {"event_ticker": "event_a", "series_ticker": "series_a"},
            {"event_ticker": "event_b", "series_ticker": "series_b"},
            {"event_ticker": "event_c", "series_ticker": "series_c"},
        ]
        assert dff["settlement_sources"].tolist() == [[], [], []]
        KalshiFetchFrame.validate(dff)

    @patch.object(KalshiSource, "_search_markets")
    def test_empty_results(self, mock_search, kalshi_source):
        """Empty search returns empty valid frame."""
        mock_search.return_value = {}
        dff = kalshi_source.fetch()

        assert len(dff) == 0
        KalshiFetchFrame.validate(dff)

    @patch.object(KalshiSource, "_search_markets")
    def test_fetch_caps_dominant_category(self, mock_search, kalshi_source, monkeypatch):
        """fetch() balances the pool so a dominant category cannot flood it."""
        monkeypatch.setattr("sources.kalshi._MAX_PER_CATEGORY", 2)
        # 5 Sports (over the cap of 2) and 1 each of two other categories (kept in full).
        mock_search.return_value = {
            **{
                f"sport_{i}": _discovered_market("Sports", f"sport_event_{i}", "sport_series")
                for i in range(5)
            },
            "econ_0": _discovered_market("Economics", "econ_event", "econ_series"),
            "crypto_0": _discovered_market("Crypto", "crypto_event", "crypto_series"),
        }
        dff = kalshi_source.fetch()

        kept = set(dff["id"])
        assert len([i for i in kept if i.startswith("sport_")]) == 2  # Sports capped
        assert "econ_0" in kept and "crypto_0" in kept  # small categories kept in full
        assert len(dff) == 4
        KalshiFetchFrame.validate(dff)


class TestBalanceCategories:
    """Tests for KalshiSource._balance_categories."""

    def test_empty_returns_empty(self):
        """No discovered tickers returns an empty list."""
        assert KalshiSource._balance_categories({}) == []

    def test_small_categories_kept_in_full(self, monkeypatch):
        """Every category under the cap is kept entirely."""
        monkeypatch.setattr("sources.kalshi._MAX_PER_CATEGORY", 10)
        mapping = {"a": "Sports", "b": "Economics", "c": "Crypto"}
        assert set(KalshiSource._balance_categories(mapping)) == {"a", "b", "c"}

    def test_over_cap_category_is_downsampled(self, monkeypatch):
        """A category above the cap is reduced to exactly the cap; others untouched."""
        monkeypatch.setattr("sources.kalshi._MAX_PER_CATEGORY", 3)
        mapping = {**{f"s{i}": "Sports" for i in range(10)}, "e0": "Economics"}
        kept = KalshiSource._balance_categories(mapping)
        assert len([i for i in kept if i.startswith("s")]) == 3
        assert "e0" in kept


# ---------------------------------------------------------------------------
# update() (mock _get_market + _build_resolution_df)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for KalshiSource.update."""

    def test_uses_one_time_cutoff_for_all_markets(self, kalshi_source):
        """Every candlestick request in one update uses the same time cutoff."""
        response = Mock()
        response.ok = True
        response.json.return_value = {
            "candlesticks": [make_kalshi_candlestick(_ts(2026, 1, 12), close_dollars="0.50")]
        }
        update_datetimes = [
            datetime(2026, 1, 15, 23, 59, 58, tzinfo=timezone.utc),
            datetime(2026, 1, 15, 23, 59, 59, tzinfo=timezone.utc),
            datetime(2026, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 1, 16, 0, 0, 1, tzinfo=timezone.utc),
        ]
        ids = ["KXTEST-001", "KXTEST-002"]
        dfq = make_question_df([{"id": ticker, "resolved": False} for ticker in ids])
        dff = make_kalshi_fetch_df([{"id": ticker} for ticker in ids])

        with (
            patch.object(
                KalshiSource,
                "_get_market",
                side_effect=lambda ticker: make_kalshi_api_market(
                    ticker=ticker,
                    open_time="2026-01-10T00:00:00Z",
                ),
            ),
            patch("sources.kalshi.requests.get", return_value=response) as mock_get,
            patch("sources.kalshi.time.sleep"),
            patch(
                "sources.kalshi.dates.get_datetime_today",
                side_effect=update_datetimes,
            ),
        ):
            kalshi_source.update(dfq, dff)

        request_end_timestamps = [
            call.kwargs["params"]["end_ts"] for call in mock_get.call_args_list
        ]
        expected_end_ts = int(update_datetimes[0].timestamp())
        assert request_end_timestamps == [expected_end_ts, expected_end_ts]

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_new_id_appended(self, mock_market, mock_build, kalshi_source):
        """Tickers in dff not in dfq get appended with defaults."""
        mock_market.return_value = make_kalshi_api_market(ticker="new_001")
        mock_build.return_value = make_resolution_df(
            [{"id": "new_001", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "existing_001"}])
        dff = make_kalshi_fetch_df([{"id": "new_001"}])

        result = kalshi_source.update(dfq, dff)

        assert "new_001" in result.dfq["id"].values
        assert len(result.dfq) == 2
        new_row = result.dfq[result.dfq["id"] == "new_001"].iloc[0]
        assert new_row["freeze_datetime_value_explanation"] == "The market price."

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_existing_unresolved_updated(self, mock_market, mock_build, kalshi_source):
        """Unresolved question fields are updated from market details."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXTEST-001",
            title="Updated question text",
            yes_sub_title="Specific Yes outcome",
            rules_primary="New rules",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "KXTEST-001", "date": "2024-06-01", "value": 0.65}]
        )
        dfq = make_question_df([{"id": "KXTEST-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])
        dff.at[0, "settlement_sources"] = [
            {"name": "Library of Congress", "url": "https://www.congress.gov/"}
        ]

        result = kalshi_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "KXTEST-001"].iloc[0]
        assert row["question"] == "Updated question text"
        assert row["market_info_resolution_criteria"] == (
            "New rules Outcome verified from Library of Congress " "(https://www.congress.gov/)."
        )
        assert row["url"] == "https://kalshi.com/markets/kxtest/x/kxtest"

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_new_question_url_uses_structured_parent_identifiers(
        self, mock_market, mock_build, kalshi_source
    ):
        """A first update builds the public URL without parsing the child market ticker."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="APPLEFOLD-26DEC31",
            event_ticker="APPLEFOLD",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "APPLEFOLD-26DEC31", "date": "2026-08-01", "value": 0.8}]
        )
        dff = make_kalshi_fetch_df(
            [
                {
                    "id": "APPLEFOLD-26DEC31",
                    "event_ticker": "APPLEFOLD",
                    "series_ticker": "KXAPPLEFOLD",
                }
            ]
        )

        result = kalshi_source.update(
            make_question_df([{"id": "existing", "resolved": True}]),
            dff,
            existing_resolution_ids={"existing"},
        )

        row = result.dfq[result.dfq["id"] == "APPLEFOLD-26DEC31"].iloc[0]
        assert row["url"] == "https://kalshi.com/markets/kxapplefold/x/applefold"

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_existing_question_absent_from_fetch_keeps_fetch_metadata(
        self, mock_market, mock_build, kalshi_source
    ):
        """A later capped fetch preserves an existing URL and Yes-label decision."""
        mock_market.return_value = make_kalshi_api_market(ticker="KXTEST-001")
        mock_build.return_value = make_resolution_df(
            [{"id": "KXTEST-001", "date": "2026-08-01", "value": 0.5}]
        )
        expected_url = "https://kalshi.com/markets/kxtest/x/kxtest"
        expected_question = "Will X happen by 2026? [Yes: X happens]"
        expected_criteria = (
            "Resolves Yes if X happens. Outcome verified from Library of Congress "
            "(https://www.congress.gov/)."
        )
        dfq = make_question_df(
            [
                {
                    "id": "KXTEST-001",
                    "question": expected_question,
                    "url": expected_url,
                    "market_info_resolution_criteria": expected_criteria,
                }
            ]
        )

        result = kalshi_source.update(dfq, make_kalshi_fetch_df([]))

        row = result.dfq.iloc[0]
        assert row["question"] == expected_question
        assert row["url"] == expected_url
        assert row["market_info_resolution_criteria"] == expected_criteria

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_persists_earliest_resolution_for_shared_curation_filter(
        self, mock_market, mock_build, kalshi_source, monkeypatch
    ):
        """The existing close-field filter sees Kalshi's earliest plausible resolution."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXSPORTS-001",
            occurrence_datetime="2026-07-25T05:00:00Z",
            expected_expiration_time="2026-07-25T05:00:00Z",
            close_time="2026-08-08T02:00:00Z",
            latest_expiration_time="2026-08-08T02:00:00Z",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "KXSPORTS-001", "date": "2026-07-23", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "KXSPORTS-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXSPORTS-001"}])

        result = kalshi_source.update(dfq, dff)
        row = result.dfq.loc[result.dfq["id"] == "KXSPORTS-001"].iloc[0]

        assert row["market_info_close_datetime"] == "2026-07-25T05:00:00+00:00"
        monkeypatch.setattr(
            create_question_set.question_curation,
            "FREEZE_DATETIME",
            datetime(2026, 7, 23, tzinfo=timezone.utc),
        )
        monkeypatch.setattr(create_question_set.question_curation, "FREEZE_WINDOW_IN_DAYS", 10)
        curated = create_question_set.drop_questions_that_resolve_too_soon(
            source="kalshi", dfq=result.dfq
        )
        assert curated.empty

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_closed_market_remains_unresolved_but_is_ineligible_for_curation(
        self, mock_market, mock_build, kalshi_source, monkeypatch
    ):
        """A closed market stays in the bank while its refreshed close time excludes it."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXCLOSED-001",
            status="closed",
            close_time="2026-07-24T05:00:00Z",
            occurrence_datetime="2026-08-25T05:00:00Z",
            expected_expiration_time="2027-01-01T00:00:00Z",
            latest_expiration_time="2026-07-24T05:00:00Z",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "KXCLOSED-001", "date": "2026-07-23", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "KXCLOSED-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXCLOSED-001"}])

        result = kalshi_source.update(dfq, dff)
        row = result.dfq.loc[result.dfq["id"] == "KXCLOSED-001"].iloc[0]

        assert bool(row["resolved"]) is False
        assert row["market_info_close_datetime"] == "2026-07-24T05:00:00+00:00"
        monkeypatch.setattr(
            create_question_set.question_curation,
            "FREEZE_DATETIME",
            datetime(2026, 7, 23, tzinfo=timezone.utc),
        )
        monkeypatch.setattr(create_question_set.question_curation, "FREEZE_WINDOW_IN_DAYS", 10)
        curated = create_question_set.drop_questions_that_resolve_too_soon(
            source="kalshi", dfq=result.dfq
        )
        assert curated.empty

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_distinct_sibling_titles_omit_participant_labels(
        self, mock_market, mock_build, kalshi_source
    ):
        """Complete sibling titles are not narrowed by non-binding participant labels."""
        markets = {
            "GOVPARTYAZ-26-D": make_kalshi_api_market(
                ticker="GOVPARTYAZ-26-D",
                event_ticker="GOVPARTYAZ-26",
                title="Will the Democratic party win the governorship in Arizona",
                yes_sub_title="Katie Hobbs",
                rules_primary=(
                    "If a representative of the Democratic party is inaugurated as the governor "
                    "of Arizona pursuant to the 2026 election, then the market resolves to Yes."
                ),
            ),
            "GOVPARTYAZ-26-R": make_kalshi_api_market(
                ticker="GOVPARTYAZ-26-R",
                event_ticker="GOVPARTYAZ-26",
                title="Will the Republican party win the governorship in Arizona",
                yes_sub_title="Andy Biggs",
                rules_primary=(
                    "If a representative of the Republican party is inaugurated as the governor "
                    "of Arizona pursuant to the 2026 election, then the market resolves to Yes."
                ),
            ),
        }
        mock_market.side_effect = lambda ticker: markets[ticker]
        mock_build.side_effect = lambda market, *args, **kwargs: make_resolution_df(
            [{"id": market["ticker"], "date": "2026-08-01", "value": 0.5}]
        )
        ids = list(markets)
        dfq = make_question_df([{"id": ticker, "resolved": False} for ticker in ids])
        dff = make_kalshi_fetch_df(
            [{"id": ticker, "event_ticker": "GOVPARTYAZ-26"} for ticker in ids]
        )

        result = kalshi_source.update(dfq, dff)

        rows = result.dfq.set_index("id")
        assert rows.at["GOVPARTYAZ-26-D", "question"] == (
            "Will the Democratic party win the governorship in Arizona"
        )
        assert rows.at["GOVPARTYAZ-26-R", "question"] == (
            "Will the Republican party win the governorship in Arizona"
        )

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_child_outcomes_produce_distinct_questions(
        self, mock_market, mock_build, kalshi_source
    ):
        """Sibling contracts with a shared title retain distinct public questions."""
        markets = {
            "KXSPACEX-120": make_kalshi_api_market(
                ticker="KXSPACEX-120",
                event_ticker="KXSPACEX",
                title="How many launches will SpaceX have in 2026?",
                yes_sub_title="Above 120",
            ),
            "KXSPACEX-140": make_kalshi_api_market(
                ticker="KXSPACEX-140",
                event_ticker="KXSPACEX",
                title="How many launches will SpaceX have in 2026?",
                yes_sub_title="Above 140",
            ),
        }
        mock_market.side_effect = lambda ticker: markets[ticker]
        mock_build.side_effect = lambda market, *args, **kwargs: make_resolution_df(
            [{"id": market["ticker"], "date": "2024-06-01", "value": 0.5}]
        )
        ids = list(markets)
        dfq = make_question_df([{"id": ticker, "resolved": False} for ticker in ids])
        dff = make_kalshi_fetch_df([{"id": ticker, "needs_yes_label": True} for ticker in ids])

        result = kalshi_source.update(dfq, dff)

        questions = result.dfq.set_index("id")["question"]
        assert questions["KXSPACEX-120"] == (
            "How many launches will SpaceX have in 2026? [Yes: Above 120]"
        )
        assert questions["KXSPACEX-140"] == (
            "How many launches will SpaceX have in 2026? [Yes: Above 140]"
        )
        assert questions.nunique() == 2

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_early_settlement_becomes_resolved(self, mock_market, mock_build, kalshi_source):
        """A finalized early close ignores the stale scheduled expiration."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXGOVOKNOMR-26-MMAZ",
            status="finalized",
            result="yes",
            occurrence_datetime="2026-08-26T02:51:02.814Z",
            close_time="2026-08-26T03:03:07Z",
            latest_expiration_time="2026-08-26T03:03:07Z",
            expected_expiration_time="2027-01-01T15:00:21Z",
            settlement_ts="2026-08-26T03:08:17.058528Z",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "KXGOVOKNOMR-26-MMAZ", "date": "2026-08-26", "value": 1.0}]
        )
        dfq = make_question_df([{"id": "KXGOVOKNOMR-26-MMAZ", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXGOVOKNOMR-26-MMAZ"}])

        result = kalshi_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "KXGOVOKNOMR-26-MMAZ"].iloc[0]
        assert bool(row["resolved"]) is True
        assert row["market_info_close_datetime"] == "2026-08-26T02:51:02+00:00"
        assert row["market_info_resolution_datetime"] == "2026-08-26T03:08:17+00:00"

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_invalid_windows_are_isolated_without_aborting(
        self, mock_market, mock_build, kalshi_source
    ):
        """Unusable markets cannot prevent valid stored questions from updating."""
        markets = {
            "invalid": make_kalshi_api_market(ticker="invalid", occurrence_datetime="not-a-date"),
            "valid": make_kalshi_api_market(ticker="valid", title="Updated valid question"),
            "new-invalid": make_kalshi_api_market(
                ticker="new-invalid", occurrence_datetime="not-a-date"
            ),
        }
        mock_market.side_effect = lambda ticker: markets[ticker]
        mock_build.side_effect = lambda market, **kwargs: make_resolution_df(
            [{"id": market["ticker"], "date": "2026-08-01", "value": 0.6}]
        )
        dfq = make_question_df(
            [
                {
                    "id": "invalid",
                    "question": "Persisted invalid question",
                    "freeze_datetime_value": "0.4",
                },
                {
                    "id": "valid",
                    "question": "Persisted valid question",
                    "freeze_datetime_value": "0.5",
                },
            ]
        )

        result = kalshi_source.update(dfq, make_kalshi_fetch_df([{"id": "new-invalid"}]))

        questions = result.dfq.set_index("id")
        assert questions.at["invalid", "question"] == "Persisted invalid question"
        assert questions.at["invalid", "freeze_datetime_value"] == "N/A"
        assert questions.at["valid", "question"] == "Updated valid question"
        assert float(questions.at["valid", "freeze_datetime_value"]) == 0.6
        assert "new-invalid" not in questions.index
        assert set(result.resolution_files) == {"valid"}

    @pytest.mark.parametrize(("outcome", "expected"), [("yes", 1.0), ("no", 0.0)])
    def test_finalization_replaces_stale_settlement_probability(
        self, outcome, expected, kalshi_source
    ):
        """Finalization emits a file replacing a same-date probability with the outcome."""
        ticker = "KXTEST-001"
        resolved_date = date(2026, 1, 13)
        existing = make_resolution_df(
            [
                {"id": ticker, "date": "2026-01-12", "value": 0.55},
                {"id": ticker, "date": "2026-01-13", "value": 0.63},
            ]
        )
        market = make_kalshi_api_market(
            ticker=ticker,
            status="finalized",
            result=outcome,
            settlement_ts="2026-01-13T05:00:00Z",
        )
        dfq = make_question_df([{"id": ticker, "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": ticker}])

        with (
            patch.object(KalshiSource, "_get_market", return_value=market),
            patch.object(KalshiSource, "_get_market_candlesticks", return_value=[]),
        ):
            result = kalshi_source.update(
                dfq,
                dff,
                existing_resolution_files={ticker: existing},
                existing_resolution_ids={ticker},
            )

        corrected = result.resolution_files[ticker]
        corrected_dates = pd.to_datetime(corrected["date"]).dt.date
        row = result.dfq.loc[result.dfq["id"] == ticker].iloc[0]
        assert bool(row["resolved"]) is True
        assert float(row["freeze_datetime_value"]) == expected
        assert corrected_dates.max() == resolved_date
        assert corrected_dates.tolist().count(resolved_date) == 1
        assert float(corrected["value"].iloc[-1]) == expected

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_resolution_file_stored(self, mock_market, mock_build, kalshi_source):
        """Resolution file from _build_resolution_df is in result."""
        mock_market.return_value = make_kalshi_api_market(ticker="KXTEST-001")
        mock_build.return_value = make_resolution_df(
            [{"id": "KXTEST-001", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "KXTEST-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff)
        assert "KXTEST-001" in result.resolution_files

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_freeze_datetime_value_set(self, mock_market, mock_build, kalshi_source):
        """freeze_datetime_value is set to last value of resolution df."""
        mock_market.return_value = make_kalshi_api_market(ticker="KXTEST-001")
        mock_build.return_value = make_resolution_df(
            [
                {"id": "KXTEST-001", "date": "2024-06-01", "value": 0.3},
                {"id": "KXTEST-001", "date": "2024-06-02", "value": 0.75},
            ]
        )
        dfq = make_question_df([{"id": "KXTEST-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff)
        row = result.dfq[result.dfq["id"] == "KXTEST-001"].iloc[0]
        assert str(row["freeze_datetime_value"]) == "0.75"

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_build_resolution_returns_none(self, mock_market, mock_build, kalshi_source):
        """_build_resolution_df returning None: no resolution file stored."""
        mock_market.return_value = make_kalshi_api_market(ticker="KXTEST-001")
        mock_build.return_value = None
        dfq = make_question_df([{"id": "KXTEST-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff)
        assert "KXTEST-001" not in (result.resolution_files or {})

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_regenerates_missing_resolved_files(self, mock_market, mock_build, kalshi_source):
        """Resolved questions missing from storage get resolution files regenerated."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXTEST-001", status="finalized", result="yes"
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "KXTEST-001", "date": "2024-06-01", "value": 1.0}]
        )
        dfq = make_question_df(
            [
                {
                    "id": "KXTEST-001",
                    "resolved": True,
                    "market_info_resolution_datetime": "2024-07-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff, existing_resolution_ids=set())
        assert "KXTEST-001" in result.resolution_files

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_skips_resolved_already_in_storage(self, mock_market, mock_build, kalshi_source):
        """Resolved questions with files in storage are not re-fetched."""
        dfq = make_question_df(
            [
                {
                    "id": "KXTEST-001",
                    "resolved": True,
                    "market_info_resolution_datetime": "2024-07-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff, existing_resolution_ids={"KXTEST-001"})
        mock_market.assert_not_called()
        assert "KXTEST-001" not in (result.resolution_files or {})

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_caps_new_questions(self, mock_market, mock_build, kalshi_source, monkeypatch):
        """New tickers exceeding the unresolved-pool cap are not all added."""
        question_limit = 2
        monkeypatch.setattr("sources.kalshi._QUESTION_LIMIT", question_limit)
        mock_market.return_value = make_kalshi_api_market()
        mock_build.return_value = make_resolution_df(
            [{"id": "x", "date": "2024-06-01", "value": 0.5}]
        )
        new_ids = [f"new_{i}" for i in range(5)]
        dfq = make_question_df([{"id": "existing"}])
        dff = make_kalshi_fetch_df([{"id": ticker} for ticker in new_ids])

        result = kalshi_source.update(dfq, dff)

        result_ids = set(result.dfq["id"])
        assert len(result.dfq) == question_limit
        assert "existing" in result_ids
        assert len(result_ids.intersection(new_ids)) == 1

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_output_schema_valid(self, mock_market, mock_build, kalshi_source):
        """Output dfq passes QuestionFrame validation."""
        mock_market.return_value = make_kalshi_api_market(ticker="new_001")
        mock_build.return_value = make_resolution_df(
            [{"id": "new_001", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "existing_001"}])
        dff = make_kalshi_fetch_df([{"id": "new_001"}])

        result = kalshi_source.update(dfq, dff)
        QuestionFrame.validate(result.dfq)

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_quarantines_persisted_market_not_found(self, mock_market, mock_build, kalshi_source):
        """A persisted dual-404 market is retained but made ineligible for curation."""
        mock_market.side_effect = MarketNotFoundError("KXTEST-001")
        dfq = make_question_df([{"id": "KXTEST-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff)

        row = result.dfq.loc[result.dfq["id"] == "KXTEST-001"].iloc[0]
        assert bool(row["resolved"]) is False
        assert row["freeze_datetime_value"] == "N/A"
        assert "KXTEST-001" not in (result.resolution_files or {})
        mock_build.assert_not_called()

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_candle_miss_restores_persisted_question(self, mock_market, mock_build, kalshi_source):
        """A dual-404 candle miss cannot leave a partially refreshed question behind."""
        mock_market.return_value = make_kalshi_api_market(
            ticker="KXTEST-001",
            title="New title",
            status="finalized",
            settlement_ts="2026-01-13T05:00:00Z",
        )
        mock_build.side_effect = MarketNotFoundError("KXTEST-001")
        dfq = make_question_df(
            [{"id": "KXTEST-001", "question": "Persisted title", "resolved": False}]
        )
        dff = make_kalshi_fetch_df([{"id": "KXTEST-001"}])

        result = kalshi_source.update(dfq, dff)

        row = result.dfq.loc[result.dfq["id"] == "KXTEST-001"].iloc[0]
        assert row["question"] == "Persisted title"
        assert bool(row["resolved"]) is False
        assert row["freeze_datetime_value"] == "N/A"

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_nullified_market_is_skipped_and_does_not_consume_cap(
        self, mock_market, mock_build, kalshi_source, monkeypatch
    ):
        """A confirmed permanent orphan is retained without blocking a new candidate."""
        monkeypatch.setattr("sources.kalshi._QUESTION_LIMIT", 1)
        monkeypatch.setattr(
            kalshi_source,
            "nullified_questions",
            [NullifiedQuestion(id="KXNULL-001", nullification_start_date=date(2026, 1, 1))],
        )
        mock_market.return_value = make_kalshi_api_market(ticker="KXNEW-001")
        mock_build.return_value = make_resolution_df(
            [{"id": "KXNEW-001", "date": "2026-01-10", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "KXNULL-001", "resolved": False}])
        dff = make_kalshi_fetch_df([{"id": "KXNEW-001"}])

        result = kalshi_source.update(dfq, dff)

        nullified = result.dfq.loc[result.dfq["id"] == "KXNULL-001"].iloc[0]
        assert bool(nullified["resolved"]) is True
        assert nullified["freeze_datetime_value"] == "N/A"
        assert "KXNEW-001" in result.dfq["id"].values
        assert all(call.args[0] != "KXNULL-001" for call in mock_market.call_args_list)

    @patch.object(KalshiSource, "_build_resolution_df")
    @patch.object(KalshiSource, "_get_market")
    def test_new_ticker_not_found_is_dropped(self, mock_market, mock_build, kalshi_source):
        """A brand-new ticker that 404s is dropped, not persisted as a null-filled row.

        The append step seeds new rows with None placeholders; if a brand-new ticker 404s before it
        is populated, keeping that row would fail QuestionFrame's non-nullable columns on the next
        run's input validation. Existing questions that 404 keep their prior data (see
        test_quarantines_persisted_market_not_found).
        """
        mock_market.side_effect = MarketNotFoundError("new_404")
        # existing_001 is resolved and already in storage, so it is never re-fetched; only the
        # brand-new ticker reaches _get_market (and 404s).
        dfq = make_question_df([{"id": "existing_001", "resolved": True}])
        dff = make_kalshi_fetch_df([{"id": "new_404"}])

        result = kalshi_source.update(dfq, dff, existing_resolution_ids={"existing_001"})

        assert "new_404" not in result.dfq["id"].values
        assert "existing_001" in result.dfq["id"].values
        # The persisted frame must still satisfy the (non-nullable) QuestionFrame contract.
        QuestionFrame.validate(result.dfq)
        mock_build.assert_not_called()


def test_update_driver_downloads_only_unresolved_resolution_histories():
    """The nightly update downloads history contents only for unresolved questions."""
    from orchestration.func_kalshi_update import main as update_main

    dfq = make_question_df(
        [
            {"id": "active", "resolved": False},
            {"id": "nullified", "resolved": False},
            {"id": "finalized", "resolved": True},
        ]
    )
    dff = make_kalshi_fetch_df([{"id": "active"}])

    with (
        patch.object(
            update_main.data_utils,
            "get_data_from_cloud_storage",
            return_value=(dfq, dff),
        ),
        patch.object(update_main.data_utils, "upload_questions"),
        patch.object(
            update_main._source_io.gcp.storage,
            "download_no_error_message_on_404",
        ) as mock_download,
        patch.object(update_main._source_io.os.path, "exists", return_value=False),
        patch.object(
            update_main._source_io,
            "list_existing_resolution_ids",
            return_value={"active", "finalized"},
        ),
        patch.object(update_main, "KalshiSource") as mock_source_class,
    ):
        mock_source_class.return_value.get_nullified_ids.return_value = {"nullified"}
        mock_source_class.return_value.update.return_value = Mock(
            dfq=dfq,
            resolution_files={},
        )

        update_main.driver(None)

    downloaded_paths = [call.kwargs["filename"] for call in mock_download.call_args_list]
    assert downloaded_paths == ["kalshi/active.jsonl"]


# ---------------------------------------------------------------------------
# End-to-end resolution (constraint 4)
# ---------------------------------------------------------------------------


class TestKalshiEndToEnd:
    """Question set -> explode -> resolve_all -> dummy forecasts -> impute."""

    def test_resolution_passes(self, freeze_today):
        """A Kalshi question set resolves and dummy forecasts flow through imputation."""
        freeze_today(date(2025, 2, 1))

        # Build a question set: 2 standard + 1 combo Kalshi question, plus a data
        # question to seed the shared resolution date.
        question_set_df = make_question_set_df(
            [
                {"id": "m1", "source": "kalshi", "resolution_dates": "N/A"},
                {"id": "m2", "source": "kalshi", "resolution_dates": "N/A"},
                {"id": ("m1", "m2"), "source": "kalshi", "resolution_dates": "N/A"},
                {"id": "d1", "source": "fred", "resolution_dates": ["2025-01-08"]},
            ]
        )

        exploded = explode_question_set(question_set_df, "2025-01-01")
        exploded = exploded[exploded["source"] == "kalshi"].copy()
        assert len(exploded) > 0

        # Question bank: market resolves to yesterday's (Jan 31) value.
        dfq = make_question_df([{"id": "m1", "resolved": False}, {"id": "m2", "resolved": False}])
        dfr = make_resolution_df(
            [
                {"id": "m1", "date": "2025-01-01", "value": 0.3},
                {"id": "m1", "date": "2025-01-08", "value": 0.5},
                {"id": "m1", "date": "2025-01-31", "value": 0.7},
                {"id": "m2", "date": "2025-01-01", "value": 0.4},
                {"id": "m2", "date": "2025-01-08", "value": 0.6},
                {"id": "m2", "date": "2025-01-31", "value": 0.8},
            ]
        )
        question_bank = {"kalshi": SourceQuestionBank(dfq=dfq, dfr=dfr)}

        resolved, _ = resolve_all(
            exploded,
            question_bank=question_bank,
            sources={"kalshi": SOURCES["kalshi"]},
            forecast_due_date=date(2025, 1, 1),
        )
        assert len(resolved) > 0
        assert resolved["resolved_to"].notna().all()

        # Dummy forecasts: m1 provided, m2 missing (to exercise imputation).
        forecast_df = pd.DataFrame(
            {
                "id": ["m1", "m2"],
                "source": ["kalshi", "kalshi"],
                "direction": [(), ()],
                "forecast": [0.65, np.nan],
                "resolution_date": ["2025-01-08", "2025-01-08"],
            }
        )
        prepared = check_and_prepare_forecast_file(forecast_df, "2025-01-01", "test_org")
        merged = set_resolution_dates(prepared, resolved)
        result = impute_missing_forecasts(merged, "test_org", "test_model_org", "test_model")

        m1_rows = result[result["id"] == "m1"]
        assert len(m1_rows) > 0
        assert m1_rows.iloc[0]["forecast"] == 0.65
        assert bool(m1_rows.iloc[0]["imputed"]) is False

        m2_rows = result[result["id"] == "m2"]
        assert len(m2_rows) > 0
        assert m2_rows.iloc[0]["forecast"] == 0.5
        assert bool(m2_rows.iloc[0]["imputed"]) is True
