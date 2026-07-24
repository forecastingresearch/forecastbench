"""Tests for PolymarketSource fetch/update logic."""

import os
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import requests

from _schemas import PolymarketFetchFrame, QuestionFrame, ResolutionFrame
from helpers import question_curation
from sources.polymarket import (
    _MIN_MARKET_LIQUIDITY,
    ConditionIdMarketNotFoundError,
    FailedConditionIdsError,
    PolymarketSource,
)

from .conftest import (
    make_polymarket_api_market,
    make_polymarket_fetch_df,
    make_polymarket_price_history,
    make_question_df,
    make_resolution_df,
)

# ---------------------------------------------------------------------------
# _is_market_binary (pure, no mocking)
# ---------------------------------------------------------------------------


class TestIsMarketBinary:
    """Tests for PolymarketSource._is_market_binary static method."""

    def test_yes_no_binary(self):
        """Standard Yes/No market is binary."""
        market = make_polymarket_api_market(outcomes='["Yes", "No"]')
        assert PolymarketSource._is_market_binary(market) is True

    def test_no_yes_binary(self):
        """Reversed No/Yes order is still binary (case-insensitive)."""
        market = make_polymarket_api_market(outcomes='["No", "Yes"]')
        assert PolymarketSource._is_market_binary(market) is True

    def test_non_binary(self):
        """Non Yes/No outcomes are not binary."""
        market = make_polymarket_api_market(outcomes='["Over", "Under"]')
        assert PolymarketSource._is_market_binary(market) is False

    def test_three_outcomes(self):
        """More than two outcomes is not binary."""
        market = make_polymarket_api_market(outcomes='["Yes", "No", "Maybe"]')
        assert PolymarketSource._is_market_binary(market) is False


# ---------------------------------------------------------------------------
# _get_yes_index / _get_yes_token (pure, no mocking)
# ---------------------------------------------------------------------------


class TestGetYesIndex:
    """Tests for PolymarketSource._get_yes_index static method."""

    def test_yes_first(self):
        """Yes at index 0."""
        market = make_polymarket_api_market(outcomes='["Yes", "No"]')
        assert PolymarketSource._get_yes_index(market) == 0

    def test_no_first(self):
        """No at index 0 means Yes is at index 1."""
        market = make_polymarket_api_market(outcomes='["No", "Yes"]')
        assert PolymarketSource._get_yes_index(market) == 1


class TestGetYesToken:
    """Tests for PolymarketSource._get_yes_token static method."""

    def test_yes_first(self):
        """Extracts first token when Yes is at index 0."""
        market = make_polymarket_api_market(
            outcomes='["Yes", "No"]',
            clobTokenIds='["tok_yes", "tok_no"]',
        )
        assert PolymarketSource._get_yes_token(market) == "tok_yes"

    def test_no_first(self):
        """Extracts second token when No is at index 0."""
        market = make_polymarket_api_market(
            outcomes='["No", "Yes"]',
            clobTokenIds='["tok_no", "tok_yes"]',
        )
        assert PolymarketSource._get_yes_token(market) == "tok_yes"


# ---------------------------------------------------------------------------
# _filter_first_midnight_only (pure, no mocking)
# ---------------------------------------------------------------------------


class TestFilterFirstMidnightOnly:
    """Tests for PolymarketSource._filter_first_midnight_only static method."""

    def test_no_duplicates(self):
        """All unique dates pass through."""
        history = [
            {"date": "2026-01-10T00:00:00+00:00", "value": 0.5},
            {"date": "2026-01-11T00:00:00+00:00", "value": 0.6},
        ]
        result = PolymarketSource._filter_first_midnight_only(history)
        assert len(result) == 2

    def test_duplicates_keeps_first(self):
        """Two entries on the same date: first one kept."""
        history = [
            {"date": "2026-01-10T00:00:00+00:00", "value": 0.5},
            {"date": "2026-01-10T12:00:00+00:00", "value": 0.9},
            {"date": "2026-01-11T00:00:00+00:00", "value": 0.6},
        ]
        result = PolymarketSource._filter_first_midnight_only(history)
        assert len(result) == 2
        assert result[0]["value"] == 0.5

    def test_empty_list(self):
        """Empty input returns empty output."""
        assert PolymarketSource._filter_first_midnight_only([]) == []


# ---------------------------------------------------------------------------
# _subtract_one_day (pure, no mocking)
# ---------------------------------------------------------------------------


class TestSubtractOneDay:
    """Tests for PolymarketSource._subtract_one_day static method."""

    def test_basic(self):
        """Each date is shifted back by one day."""
        history = [
            {"date": "2026-01-10T00:00:00+00:00", "value": 0.5},
            {"date": "2026-01-11T00:00:00+00:00", "value": 0.6},
        ]
        result = PolymarketSource._subtract_one_day(history)
        assert "2026-01-09" in result[0]["date"]
        assert "2026-01-10" in result[1]["date"]

    def test_preserves_time_component(self):
        """Time of day is preserved after subtraction."""
        history = [{"date": "2026-01-10T14:30:00+00:00", "value": 0.5}]
        result = PolymarketSource._subtract_one_day(history)
        dt = datetime.fromisoformat(result[0]["date"])
        assert dt.hour == 14
        assert dt.minute == 30


# ---------------------------------------------------------------------------
# _build_resolution_df (pure, no mocking)
# ---------------------------------------------------------------------------


class TestBuildResolutionDf:
    """Tests for PolymarketSource._build_resolution_df."""

    def test_basic(self, polymarket_source):
        """Extracts resolution df from historical_prices and validates schema."""
        question = {
            "id": "0xabc123",
            "historical_prices": [
                {"date": "2024-06-01", "value": 0.5},
                {"date": "2024-06-02", "value": 0.6},
            ],
        }
        result = polymarket_source._build_resolution_df(question)
        assert len(result) == 2
        assert (result["id"] == "0xabc123").all()
        assert list(result.columns) == ["id", "date", "value"]
        ResolutionFrame.validate(result)

    def test_no_benchmark_filter(self, polymarket_source):
        """Pre-benchmark rows are kept (legacy did not filter on benchmark start)."""
        question = {
            "id": "0xabc123",
            "historical_prices": [
                {"date": "2020-01-01", "value": 0.1},
                {"date": "2024-06-01", "value": 0.5},
            ],
        }
        result = polymarket_source._build_resolution_df(question)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _transform_question (pure static method)
# ---------------------------------------------------------------------------


class TestTransformQuestion:
    """Tests for PolymarketSource._transform_question."""

    FETCH_DT = "2026-01-15T00:00:00+00:00"

    def test_basic_unresolved_market(self):
        """Standard unresolved market produces a valid row dict."""
        market = make_polymarket_api_market(
            price_history=make_polymarket_price_history(
                [
                    (1736380800, 0.5),  # 2025-01-09T00:00:00Z
                    (1736467200, 0.6),  # 2025-01-10T00:00:00Z
                ]
            ),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        assert result["id"] == "0xabc123"
        assert result["question"] == "Will X happen by 2026?"
        assert result["background"] == "Background text."
        assert result["url"] == "https://polymarket.com/market/will-x-happen-by-2026"
        assert result["resolved"] is False
        assert result["fetch_datetime"] == self.FETCH_DT
        assert result["freeze_datetime_value_explanation"] == "The market price."
        assert isinstance(result["historical_prices"], list)

    def test_forward_fills_gaps(self):
        """Price history with date gaps gets forward-filled."""
        market = make_polymarket_api_market(
            price_history=make_polymarket_price_history(
                [
                    (1736380800, 0.4),  # 2025-01-09 -> date becomes 2025-01-08
                    (1736640000, 0.7),  # 2025-01-12 -> date becomes 2025-01-11
                ]
            ),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        # Should have forward-filled dates: 08, 09, 10, 11
        assert len(result["historical_prices"]) >= 4

    def test_resolved_market(self):
        """Resolved market sets resolved=True and uses outcome prices."""
        market = make_polymarket_api_market(
            umaResolutionStatus="resolved",
            umaEndDate="2026-01-10T12:00:00Z",
            outcomePrices='["1", "0"]',
            price_history=make_polymarket_price_history(
                [
                    (1736380800, 0.5),
                    (1736467200, 0.6),
                    (1736553600, 0.7),
                ]
            ),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        assert result["resolved"] is True
        assert result["probability"] == 1.0
        assert result["freeze_datetime_value"] == 1.0

    def test_uma_date_used_when_earlier(self):
        """When umaEndDate < endDate, the UMA date is used for the resolution datetime."""
        market = make_polymarket_api_market(
            endDate="2026-06-01T00:00:00Z",
            umaEndDate="2026-01-10T00:00:00Z",
            price_history=make_polymarket_price_history(
                [
                    (1736380800, 0.5),
                    (1736467200, 0.6),
                ]
            ),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        assert "2026-01-10" in result["market_info_resolution_datetime"]

    def test_missing_end_date_returns_none(self):
        """Market missing both endDate and events returns None."""
        market = make_polymarket_api_market(
            price_history=make_polymarket_price_history([(1736380800, 0.5)]),
        )
        del market["endDate"]
        del market["events"]

        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())
        assert result is None

    def test_falls_back_to_event_end_date_when_market_end_date_missing(self):
        """With no market-level endDate, the event's endDate drives the close datetime."""
        market = make_polymarket_api_market(
            events=[{"endDate": "2026-06-15T00:00:00Z"}],
            price_history=make_polymarket_price_history([(1736380800, 0.5)]),
        )
        del market["endDate"]

        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        assert result["market_info_close_datetime"].startswith("2026-06-15")

    def test_single_price_history_entry(self):
        """Single price history entry: probability and freeze value are N/A."""
        market = make_polymarket_api_market(
            price_history=make_polymarket_price_history([(1736380800, 0.5)]),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, set())

        assert result is not None
        assert result["probability"] == "N/A"
        assert result["freeze_datetime_value"] == "N/A"

    def test_invalid_market_skips_resolution_branch(self):
        """A resolved market in invalid_ids keeps its raw probability, not the outcome price."""
        nullified_id = "0x525820c5314f4143091d05079a8d810ecc07c8d5c8954ec2e6b6e163e40de9cb"
        market = make_polymarket_api_market(
            conditionId=nullified_id,
            umaResolutionStatus="resolved",
            umaEndDate="2026-01-10T12:00:00Z",
            outcomePrices='["1", "0"]',
            price_history=make_polymarket_price_history(
                [
                    (1736380800, 0.5),
                    (1736467200, 0.6),
                    (1736553600, 0.7),
                ]
            ),
        )
        result = PolymarketSource._transform_question(market, self.FETCH_DT, {nullified_id})

        assert result is not None
        # resolved flag is True (from umaResolutionStatus), but being in invalid_ids prevents the
        # resolution branch from overriding probability with the outcome price.
        assert result["resolved"] is True
        assert result["probability"] == 0.7


# ---------------------------------------------------------------------------
# _fetch_active_markets_from_api (mock requests.get + _fetch_price_history)
# ---------------------------------------------------------------------------


class TestFetchActiveMarketsFromApi:
    """Tests for PolymarketSource._fetch_active_markets_from_api."""

    _FROZEN_TODAY = date(2026, 1, 15)

    @pytest.fixture(autouse=True)
    def _freeze_today_for_window(self, freeze_today):
        """Freeze 'today' so the resolution-window filter is deterministic across wall-clock time.

        The default market fixture closes 2026-06-01, comfortably after the frozen window cutoff, so
        the existing (non-date) tests are unaffected.
        """
        freeze_today(self._FROZEN_TODAY)

    def _end_date(self, days_from_cutoff: int) -> str:
        """Return an endDate string offset by ``days_from_cutoff`` days from the window cutoff.

        The cutoff is ``today + FREEZE_WINDOW_IN_DAYS``; deriving from the constant keeps these
        tests correct if the freeze window changes.
        """
        cutoff = self._FROZEN_TODAY + timedelta(days=question_curation.FREEZE_WINDOW_IN_DAYS)
        return (cutoff + timedelta(days=days_from_cutoff)).strftime("%Y-%m-%dT00:00:00Z")

    def _mock_response(self, data, next_cursor=None):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"markets": data, "next_cursor": next_cursor}
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_basic_returns_qualifying(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """Binary, liquid, non-catch-all markets are returned."""
        market = make_polymarket_api_market()
        mock_get.return_value = self._mock_response([market])
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 1
        assert result[0]["conditionId"] == "0xabc123"
        assert "price_history" in result[0]

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_non_binary(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """Non-binary markets are excluded."""
        market = make_polymarket_api_market(outcomes='["Over", "Under"]')
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0
        mock_price.assert_not_called()

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_low_liquidity(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """Markets with liquidityNum below the threshold are excluded."""
        market = make_polymarket_api_market(liquidityNum=10000)
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_catch_all(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """Markets with 'other' in the slug are excluded."""
        market = make_polymarket_api_market(slug="who-will-win-other-candidates")
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_pagination(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """Fetches multiple pages until an empty response."""
        m1 = make_polymarket_api_market(conditionId="0x001")
        m2 = make_polymarket_api_market(conditionId="0x002")
        mock_get.side_effect = [
            self._mock_response([m1], next_cursor="cursor1"),
            self._mock_response([m2]),
        ]
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 2
        # The 2nd request must forward the cursor from the 1st response; offset is never sent.
        assert mock_get.call_count == 2
        second_params = mock_get.call_args_list[1].kwargs["params"]
        assert second_params["after_cursor"] == "cursor1"
        assert "offset" not in second_params
        # Liquidity floor is pushed server-side (client-side check stays the authoritative cutoff).
        assert second_params["liquidity_num_min"] == _MIN_MARKET_LIQUIDITY

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_dedupes_market_recurring_across_pages(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """A market re-served on a later page (keyset orders by mutable liquidity) is kept once."""
        market = make_polymarket_api_market(conditionId="0xdupe")
        mock_get.side_effect = [
            self._mock_response([market], next_cursor="cursor1"),
            self._mock_response([market]),
        ]
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 1
        assert result[0]["conditionId"] == "0xdupe"

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_skips_when_price_history_none(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """Markets where _fetch_price_history returns None are excluded."""
        market = make_polymarket_api_market()
        mock_get.return_value = self._mock_response([market])
        mock_price.return_value = None

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_transient_error_retried(self, mock_get, mock_price, mock_sleep, polymarket_source):
        """A transient HTTP error retries the whole fetch via backoff instead of truncating."""
        market = make_polymarket_api_market()
        err_resp = Mock()
        err_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        mock_get.side_effect = [
            err_resp,  # first attempt: transient 500
            self._mock_response([market]),  # retry: success
        ]
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 1
        assert result[0]["conditionId"] == "0xabc123"
        assert mock_get.call_count == 2

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_missing_liquidity_key(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """Markets without a liquidityNum key are excluded."""
        market = make_polymarket_api_market()
        del market["liquidityNum"]
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_market_resolving_within_freeze_window(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """Markets closing within the freeze window are excluded (they can't be forecast)."""
        market = make_polymarket_api_market(endDate=self._end_date(-1))
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0
        # The expensive price-history call is skipped for markets that resolve too soon.
        mock_price.assert_not_called()

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_filters_market_resolving_on_freeze_window_boundary(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """A market closing exactly on the cutoff date is excluded (boundary is inclusive)."""
        market = make_polymarket_api_market(endDate=self._end_date(0))
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_keeps_market_resolving_after_freeze_window(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """Markets closing after the window cutoff are kept."""
        market = make_polymarket_api_market(endDate=self._end_date(1))
        mock_get.return_value = self._mock_response([market])
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 1

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_window_filter_falls_back_to_event_end_date(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """With no top-level endDate, the event's endDate drives the window filter."""
        market = make_polymarket_api_market(events=[{"endDate": self._end_date(-1)}])
        del market["endDate"]
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 0
        mock_price.assert_not_called()

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch("sources.polymarket.requests.get")
    def test_keeps_market_when_close_date_undeterminable(
        self, mock_get, mock_price, mock_sleep, polymarket_source
    ):
        """A market with an undeterminable close date is kept here (left for _transform_question)."""
        market = make_polymarket_api_market()
        del market["endDate"]
        del market["events"]
        mock_get.return_value = self._mock_response([market])
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]

        result = polymarket_source._fetch_active_markets_from_api()

        assert len(result) == 1


# ---------------------------------------------------------------------------
# _fetch_price_history (mock requests.get)
# ---------------------------------------------------------------------------


class TestFetchPriceHistory:
    """Tests for PolymarketSource._fetch_price_history."""

    def _mock_response(self, data):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = data
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.polymarket.time.sleep")
    @patch("sources.polymarket.requests.get")
    def test_basic_returns_history(self, mock_get, mock_sleep, polymarket_source):
        """Successful response returns the history list."""
        history = [{"t": 1736380800, "p": 0.5}, {"t": 1736467200, "p": 0.6}]
        mock_get.return_value = self._mock_response({"history": history})

        result = polymarket_source._fetch_price_history("token_yes")

        assert result == history

    @patch("sources.polymarket.time.sleep")
    @patch("sources.polymarket.requests.get")
    def test_returns_none_on_error(self, mock_get, mock_sleep, polymarket_source):
        """A RequestException returns None."""
        mock_get.side_effect = requests.exceptions.RequestException("timeout")

        result = polymarket_source._fetch_price_history("token_yes")

        assert result is None

    @patch("sources.polymarket.time.sleep")
    @patch("sources.polymarket.requests.get")
    def test_empty_history(self, mock_get, mock_sleep, polymarket_source):
        """A response with an empty history list returns []."""
        mock_get.return_value = self._mock_response({"history": []})

        result = polymarket_source._fetch_price_history("token_yes")

        assert result == []


# ---------------------------------------------------------------------------
# _get_market (mock requests.get) — two-attempt (open then closed), fail loudly
# ---------------------------------------------------------------------------


class TestGetMarket:
    """Tests for PolymarketSource._get_market."""

    def _mock_response(self, data):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"markets": data}
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.polymarket.requests.get")
    def test_returns_single_open_market(self, mock_get, polymarket_source):
        """A single open-market match returns that market without trying closed."""
        market = make_polymarket_api_market()
        mock_get.return_value = self._mock_response([market])

        result = polymarket_source._get_market("0xabc123")

        assert result == market
        assert mock_get.call_count == 1

    @patch("sources.polymarket.requests.get")
    def test_falls_back_to_closed(self, mock_get, polymarket_source):
        """When the open query has no match, the closed query is tried."""
        market = make_polymarket_api_market(closed=True)
        mock_get.side_effect = [
            self._mock_response([]),  # open: no match
            self._mock_response([market]),  # closed: match
        ]

        result = polymarket_source._get_market("0xabc123")

        assert result == market
        assert mock_get.call_count == 2

    @patch("sources.polymarket.requests.get")
    def test_not_found_raises(self, mock_get, polymarket_source):
        """When neither query yields a single market, it raises (fail loudly)."""
        mock_get.side_effect = [
            self._mock_response([]),
            self._mock_response([]),
        ]

        with pytest.raises(ConditionIdMarketNotFoundError):
            polymarket_source._get_market("0xabc123")

    @patch("sources.polymarket.requests.get")
    def test_multiple_results_raises(self, mock_get, polymarket_source):
        """Multiple matches (len != 1) raise rather than guess."""
        m1 = make_polymarket_api_market(conditionId="0x001")
        m2 = make_polymarket_api_market(conditionId="0x002")
        mock_get.side_effect = [
            self._mock_response([m1, m2]),
            self._mock_response([m1, m2]),
        ]

        with pytest.raises(ConditionIdMarketNotFoundError):
            polymarket_source._get_market("0x001")

    @patch("sources.polymarket.time.sleep")
    @patch("sources.polymarket.requests.get")
    def test_transient_error_then_success(self, mock_get, _mock_sleep, polymarket_source):
        """A transient HTTP error (e.g. a 500 blip) is retried via backoff and then succeeds."""
        market = make_polymarket_api_market()
        err_resp = Mock()
        err_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        mock_get.side_effect = [
            err_resp,  # first attempt: transient 500
            self._mock_response([market]),  # retry: open-market match
        ]

        result = polymarket_source._get_market("0xabc123")

        assert result == market
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# fetch() (mock internal methods)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for PolymarketSource.fetch."""

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_basic_no_existing_dfq(
        self, mock_active, mock_transform, mock_sleep, polymarket_source, freeze_today
    ):
        """Fresh fetch with no existing questions returns a PolymarketFetchFrame."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        market = make_polymarket_api_market(price_history=[{"t": 1736380800, "p": 0.5}])
        mock_active.return_value = [market]
        mock_transform.return_value = self._row("0xabc123")

        dff = polymarket_source.fetch()

        assert len(dff) == 1
        assert dff["id"].iloc[0] == "0xabc123"
        PolymarketFetchFrame.validate(dff)

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_refetches_unresolved_existing(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_price,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """Existing unresolved questions are re-fetched via _get_market."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        mock_active.return_value = []  # No new markets
        mock_get_market.return_value = make_polymarket_api_market(conditionId="0xexisting")
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]
        mock_transform.return_value = self._row("0xexisting")

        dfq = make_question_df([{"id": "0xexisting", "resolved": False}])
        dff = polymarket_source.fetch(dfq=dfq)

        mock_get_market.assert_called_once_with("0xexisting")
        assert len(dff) == 1

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_deduplication(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """IDs from the active API overlapping unresolved_ids are not double-fetched."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        market = make_polymarket_api_market(
            conditionId="0xoverlap", price_history=[{"t": 1736380800, "p": 0.5}]
        )
        mock_active.return_value = [market]
        mock_transform.return_value = self._row("0xoverlap")

        dfq = make_question_df([{"id": "0xoverlap", "resolved": False}])
        dff = polymarket_source.fetch(dfq=dfq)

        mock_get_market.assert_not_called()
        assert len(dff) == 1

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_nullified_excluded(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """Nullified IDs are excluded from the re-fetch set."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        new_market = make_polymarket_api_market(
            conditionId="0xnew", price_history=[{"t": 1736380800, "p": 0.5}]
        )
        mock_active.return_value = [new_market]
        mock_transform.return_value = self._row("0xnew")

        nullified_id = "0x525820c5314f4143091d05079a8d810ecc07c8d5c8954ec2e6b6e163e40de9cb"
        dfq = make_question_df([{"id": nullified_id, "resolved": False}])

        polymarket_source.fetch(dfq=dfq)

        mock_get_market.assert_not_called()

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_failed_condition_ids_raises(
        self, mock_active, mock_get_market, mock_sleep, polymarket_source, freeze_today
    ):
        """An unfetchable unresolved condition id makes fetch fail loudly at the end."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        mock_active.return_value = []
        mock_get_market.side_effect = ConditionIdMarketNotFoundError("0xmissing")

        dfq = make_question_df([{"id": "0xmissing", "resolved": False}])

        with pytest.raises(FailedConditionIdsError):
            polymarket_source.fetch(dfq=dfq)

    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_resolved_not_checked_by_default(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """By default, resolved questions are never re-fetched (CHECK flag unset)."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        new_market = make_polymarket_api_market(
            conditionId="0xnew", price_history=[{"t": 1736380800, "p": 0.5}]
        )
        mock_active.return_value = [new_market]
        mock_transform.return_value = self._row("0xnew")

        dfq = make_question_df([{"id": "0xresolved", "resolved": True}])
        gapped_res = make_resolution_df(
            [
                {"id": "0xresolved", "date": "2024-06-01", "value": 0.5},
                {"id": "0xresolved", "date": "2024-06-03", "value": 0.6},  # gap
            ]
        )

        polymarket_source.fetch(dfq=dfq, existing_resolution_files={"0xresolved": gapped_res})

        mock_get_market.assert_not_called()

    @patch.dict(os.environ, {"CHECK_AND_FIX_RESOLVED_DATA": "1"})
    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_price_history")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_resolved_with_gaps_refetched_when_flag_set(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_price,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """With the flag set, a resolved question with date gaps is re-fetched."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        mock_active.return_value = []
        mock_get_market.return_value = make_polymarket_api_market(conditionId="0xresolved")
        mock_price.return_value = [{"t": 1736380800, "p": 0.5}]
        mock_transform.return_value = self._row("0xresolved")

        dfq = make_question_df([{"id": "0xresolved", "resolved": True}])
        gapped_res = make_resolution_df(
            [
                {"id": "0xresolved", "date": "2024-06-01", "value": 0.5},
                {"id": "0xresolved", "date": "2024-06-03", "value": 0.6},  # gap
            ]
        )

        polymarket_source.fetch(dfq=dfq, existing_resolution_files={"0xresolved": gapped_res})

        mock_get_market.assert_called_once_with("0xresolved")

    @patch.dict(os.environ, {"CHECK_AND_FIX_RESOLVED_DATA": "1"})
    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_get_market")
    @patch.object(PolymarketSource, "_transform_question")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_resolved_complete_not_refetched_when_flag_set(
        self,
        mock_active,
        mock_transform,
        mock_get_market,
        mock_sleep,
        polymarket_source,
        freeze_today,
    ):
        """With the flag set, a resolved question with contiguous data is NOT re-fetched."""
        from datetime import date

        freeze_today(date(2026, 1, 15))
        new_market = make_polymarket_api_market(
            conditionId="0xnew", price_history=[{"t": 1736380800, "p": 0.5}]
        )
        mock_active.return_value = [new_market]
        mock_transform.return_value = self._row("0xnew")

        dfq = make_question_df([{"id": "0xresolved", "resolved": True}])
        contiguous_res = make_resolution_df(
            [
                {"id": "0xresolved", "date": "2024-06-01", "value": 0.5},
                {"id": "0xresolved", "date": "2024-06-02", "value": 0.6},
                {"id": "0xresolved", "date": "2024-06-03", "value": 0.7},
            ]
        )

        polymarket_source.fetch(dfq=dfq, existing_resolution_files={"0xresolved": contiguous_res})

        mock_get_market.assert_not_called()

    @patch("sources.polymarket.dates.get_datetime_now", return_value="2099-01-01T00:00:00+00:00")
    @patch("sources.polymarket.time.sleep")
    @patch.object(PolymarketSource, "_fetch_active_markets_from_api")
    def test_fetch_datetime_recorded_on_rows(
        self, mock_active, mock_sleep, mock_now, polymarket_source
    ):
        """The fetch timestamp computed once in fetch() lands on every fetched row."""
        market = make_polymarket_api_market(
            price_history=make_polymarket_price_history([(1736380800, 0.5), (1736467200, 0.6)]),
        )
        mock_active.return_value = [market]

        # _transform_question runs for real, so the computed fetch_datetime lands on the row.
        dff = polymarket_source.fetch()

        assert len(dff) == 1
        assert dff["fetch_datetime"].iloc[0] == "2099-01-01T00:00:00+00:00"

    @staticmethod
    def _row(condition_id):
        """Build a PolymarketFetchFrame-compatible row dict for mocking _transform_question."""
        return {
            "id": condition_id,
            "question": "N/A",
            "background": "N/A",
            "url": "N/A",
            "resolved": False,
            "forecast_horizons": "N/A",
            "freeze_datetime_value": "N/A",
            "freeze_datetime_value_explanation": "The market price.",
            "market_info_resolution_criteria": "N/A",
            "market_info_open_datetime": "N/A",
            "market_info_close_datetime": "N/A",
            "market_info_resolution_datetime": "N/A",
            "fetch_datetime": "2026-01-15T00:00:00+00:00",
            "probability": 0.5,
            "historical_prices": [{"date": "2024-06-01", "value": 0.5}],
        }


# ---------------------------------------------------------------------------
# update() (mock _build_resolution_df)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for PolymarketSource.update."""

    @patch.object(PolymarketSource, "_build_resolution_df")
    def test_new_id_appended(self, mock_build, polymarket_source):
        """An ID in dff not in dfq gets appended."""
        mock_build.return_value = make_resolution_df(
            [{"id": "0xnew", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "0xexisting"}])
        dff = make_polymarket_fetch_df([{"id": "0xnew"}])

        result = polymarket_source.update(dfq, dff)

        assert "0xnew" in result.dfq["id"].values
        assert len(result.dfq) == 2

    @patch.object(PolymarketSource, "_build_resolution_df")
    def test_existing_updated(self, mock_build, polymarket_source):
        """An existing ID's fields are updated from dff."""
        mock_build.return_value = make_resolution_df(
            [{"id": "0xabc123", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "0xabc123", "question": "Old question", "resolved": False}])
        dff = make_polymarket_fetch_df([{"id": "0xabc123", "question": "Updated question"}])

        result = polymarket_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "0xabc123"].iloc[0]
        assert row["question"] == "Updated question"
        assert len(result.dfq) == 1

    @patch.object(PolymarketSource, "_build_resolution_df")
    def test_resolution_file_stored(self, mock_build, polymarket_source):
        """The resolution file from _build_resolution_df is in the result."""
        mock_build.return_value = make_resolution_df(
            [{"id": "0xabc123", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "0xabc123", "resolved": False}])
        dff = make_polymarket_fetch_df([{"id": "0xabc123"}])

        result = polymarket_source.update(dfq, dff)

        assert "0xabc123" in result.resolution_files

    @patch.object(PolymarketSource, "_build_resolution_df")
    def test_strips_transient_fields(self, mock_build, polymarket_source):
        """Transient fields are not present in the output dfq."""
        mock_build.return_value = make_resolution_df(
            [{"id": "0xabc123", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "0xexisting"}])
        dff = make_polymarket_fetch_df([{"id": "0xabc123"}])

        result = polymarket_source.update(dfq, dff)

        for col in ["fetch_datetime", "probability", "historical_prices"]:
            assert col not in result.dfq.columns

    @patch.object(PolymarketSource, "_build_resolution_df")
    def test_output_schema_valid(self, mock_build, polymarket_source):
        """The output dfq passes QuestionFrame validation."""
        mock_build.return_value = make_resolution_df(
            [{"id": "0xnew", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "0xexisting"}])
        dff = make_polymarket_fetch_df([{"id": "0xnew"}])

        result = polymarket_source.update(dfq, dff)
        QuestionFrame.validate(result.dfq)
