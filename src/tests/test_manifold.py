"""Tests for ManifoldSource fetch/update logic."""

from datetime import date
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

from _schemas import ManifoldFetchFrame, QuestionFrame, ResolutionFrame
from sources.manifold import ManifoldSource

from .conftest import (
    make_manifold_api_market,
    make_manifold_bet,
    make_manifold_fetch_df,
    make_manifold_search_result,
    make_question_df,
    make_resolution_df,
)

# ---------------------------------------------------------------------------
# _get_resolved_market_value (pure, no mocking)
# ---------------------------------------------------------------------------


class TestGetResolvedMarketValue:
    """Tests for ManifoldSource._get_resolved_market_value static method."""

    def test_yes_resolution(self):
        """YES resolution returns 1.0."""
        market = make_manifold_api_market(resolution="YES")
        assert ManifoldSource._get_resolved_market_value(market) == 1.0

    def test_no_resolution(self):
        """NO resolution returns 0.0."""
        market = make_manifold_api_market(resolution="NO")
        assert ManifoldSource._get_resolved_market_value(market) == 0.0

    def test_mkt_resolution(self):
        """MKT resolution returns resolutionProbability."""
        market = make_manifold_api_market(resolution="MKT", resolutionProbability=0.73)
        assert ManifoldSource._get_resolved_market_value(market) == 0.73

    def test_cancel_resolution(self):
        """CANCEL resolution returns NaN."""
        market = make_manifold_api_market(resolution="CANCEL")
        assert np.isnan(ManifoldSource._get_resolved_market_value(market))

    def test_unknown_resolution(self):
        """Unknown resolution string returns NaN."""
        market = make_manifold_api_market(resolution="FOOBAR")
        assert np.isnan(ManifoldSource._get_resolved_market_value(market))


# ---------------------------------------------------------------------------
# _build_resolution_df (mock _get_market_bets)
# ---------------------------------------------------------------------------


class TestBuildResolutionDf:
    """Tests for ManifoldSource._build_resolution_df."""

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_already_up_to_date(self, mock_bets, manifold_source, freeze_today):
        """Skips API call if existing data covers through yesterday."""
        freeze_today(date(2026, 1, 15))
        existing = make_resolution_df(
            [
                {"id": "mkt_001", "date": "2024-06-01", "value": 0.5},
                {"id": "mkt_001", "date": "2026-01-14", "value": 0.6},
            ]
        )
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=existing
        )

        assert result.equals(existing)
        mock_bets.assert_not_called()

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_basic_unresolved_market(self, mock_bets, manifold_source, freeze_today):
        """Builds valid time series from filled bets for an unresolved market."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(id="b1", createdTime=1768046400000, probAfter=0.4),  # 2026-01-10
            make_manifold_bet(id="b2", createdTime=1768226400000, probAfter=0.6),  # 2026-01-12
        ]
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=None
        )

        assert result is not None
        assert not result.empty
        assert (result["id"] == "mkt_001").all()
        ResolutionFrame.validate(result)
        # Should have forward-filled dates: 10, 11, 12, 13, 14
        assert len(result) >= 5

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_empty_bets_returns_none(self, mock_bets, manifold_source, freeze_today):
        """No bets returns None."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = []
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=None
        )
        assert result is None

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_no_filled_bets_returns_none(self, mock_bets, manifold_source, freeze_today):
        """All bets with isFilled=False returns None."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(isFilled=False, createdTime=1768046400000),
            make_manifold_bet(isFilled=False, createdTime=1768226400000),
        ]
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=None
        )
        assert result is None

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_forward_fills_gaps(self, mock_bets, manifold_source, freeze_today):
        """Missing dates between bets are forward-filled."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(id="b1", createdTime=1768046400000, probAfter=0.3),  # 2026-01-10
            make_manifold_bet(id="b2", createdTime=1768384800000, probAfter=0.8),  # 2026-01-14
        ]
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=None
        )

        dates_in_df = pd.to_datetime(result["date"]).dt.date.tolist()
        # 11th, 12th, 13th should be forward-filled from the 10th's value
        assert date(2026, 1, 11) in dates_in_df
        assert date(2026, 1, 12) in dates_in_df
        assert date(2026, 1, 13) in dates_in_df

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_resolved_truncates_at_resolution(self, mock_bets, manifold_source, freeze_today):
        """Resolved market: data truncated at resolution date, final row has resolved value."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(id="b1", createdTime=1768046400000, probAfter=0.4),  # 2026-01-10
            make_manifold_bet(id="b2", createdTime=1768226400000, probAfter=0.6),  # 2026-01-12
            make_manifold_bet(id="b3", createdTime=1768384800000, probAfter=0.9),  # 2026-01-14
        ]
        market = make_manifold_api_market(
            isResolved=True,
            resolution="YES",
            resolutionTime=1768310400000,  # 2026-01-13T12:00:00Z
        )
        result = manifold_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="2026-01-13T12:00:00+00:00",
            existing_df=None,
        )

        assert result is not None
        # Last row should be the resolution date with resolved value
        last_date = pd.to_datetime(result["date"].iloc[-1]).date()
        assert last_date == date(2026, 1, 13)
        assert float(result["value"].iloc[-1]) == 1.0
        # No rows after resolution date
        all_dates = pd.to_datetime(result["date"]).dt.date
        assert all(d <= date(2026, 1, 13) for d in all_dates)

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_resolved_cancel_nan_last_row(self, mock_bets, manifold_source, freeze_today):
        """CANCEL resolution: last row is NaN, not forward-filled."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(id="b1", createdTime=1768046400000, probAfter=0.4),  # 2026-01-10
            make_manifold_bet(id="b2", createdTime=1768226400000, probAfter=0.6),  # 2026-01-12
        ]
        market = make_manifold_api_market(
            isResolved=True,
            resolution="CANCEL",
            resolutionTime=1768310400000,  # 2026-01-13T12:00:00Z
        )
        result = manifold_source._build_resolution_df(
            market=market,
            market_info_resolution_datetime="2026-01-13T12:00:00+00:00",
            existing_df=None,
        )

        assert result is not None
        # Last row (resolution date) should be NaN for CANCEL
        assert np.isnan(float(result["value"].iloc[-1]))

    @patch.object(ManifoldSource, "_get_market_bets")
    def test_filters_future_bets(self, mock_bets, manifold_source, freeze_today):
        """Bets with date > yesterday are excluded."""
        freeze_today(date(2026, 1, 15))
        mock_bets.return_value = [
            make_manifold_bet(id="b1", createdTime=1768384800000, probAfter=0.5),  # 2026-01-14
            make_manifold_bet(id="b2", createdTime=1768464000000, probAfter=0.9),  # 2026-01-15
        ]
        market = make_manifold_api_market()
        result = manifold_source._build_resolution_df(
            market=market, market_info_resolution_datetime="N/A", existing_df=None
        )

        assert result is not None
        all_dates = pd.to_datetime(result["date"]).dt.date
        assert all(d <= date(2026, 1, 14) for d in all_dates)


# ---------------------------------------------------------------------------
# _call_search_endpoint (mock requests.get)
# ---------------------------------------------------------------------------


class TestCallSearchEndpoint:
    """Tests for ManifoldSource._call_search_endpoint."""

    def _mock_response(self, markets):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = markets
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.manifold.requests.get")
    def test_basic_returns_qualifying_ids(self, mock_get, manifold_source, freeze_today):
        """Returns IDs for markets meeting all criteria."""
        freeze_today(date(2026, 1, 15))
        mock_get.return_value = self._mock_response(
            [
                make_manifold_search_result(id="a"),
                make_manifold_search_result(id="b"),
            ]
        )
        ids = manifold_source._call_search_endpoint(max_resolution_date=date(2028, 1, 14))
        assert ids == {"a", "b"}

    @patch("sources.manifold.requests.get")
    def test_filters_low_bettors(self, mock_get, manifold_source, freeze_today):
        """Markets with < 17 bettors are excluded."""
        freeze_today(date(2026, 1, 15))
        mock_get.return_value = self._mock_response(
            [
                make_manifold_search_result(id="low", uniqueBettorCount=16),
                make_manifold_search_result(id="ok", uniqueBettorCount=17),
            ]
        )
        ids = manifold_source._call_search_endpoint(max_resolution_date=date(2028, 1, 14))
        assert ids == {"ok"}

    @patch("sources.manifold.requests.get")
    def test_filters_low_liquidity(self, mock_get, manifold_source, freeze_today):
        """Markets with < 120 liquidity are excluded."""
        freeze_today(date(2026, 1, 15))
        mock_get.return_value = self._mock_response(
            [
                make_manifold_search_result(id="low", totalLiquidity=119),
                make_manifold_search_result(id="ok", totalLiquidity=120),
            ]
        )
        ids = manifold_source._call_search_endpoint(max_resolution_date=date(2028, 1, 14))
        assert ids == {"ok"}

    @patch("sources.manifold.requests.get")
    def test_filters_late_resolution(self, mock_get, manifold_source, freeze_today):
        """Markets closing > 730 days from today are excluded."""
        freeze_today(date(2026, 1, 15))
        mock_get.return_value = self._mock_response(
            [
                # 2029-01-01 is way past 730 days from 2026-01-15
                make_manifold_search_result(id="late", closeTime=1861920000000),
                # 2025-06-01 is well within range (already past, even)
                make_manifold_search_result(id="ok", closeTime=1748736000000),
            ]
        )
        ids = manifold_source._call_search_endpoint(max_resolution_date=date(2028, 1, 14))
        assert ids == {"ok"}

    @patch("sources.manifold.requests.get")
    def test_additional_params_passed(self, mock_get, manifold_source, freeze_today):
        """additional_params are merged into API request params."""
        freeze_today(date(2026, 1, 15))
        mock_get.return_value = self._mock_response([])
        manifold_source._call_search_endpoint(
            max_resolution_date=date(2028, 1, 14),
            additional_params={"topicSlug": "ai"},
        )

        mock_get.assert_called_once()
        called_params = mock_get.call_args[1].get("params") or mock_get.call_args[0][1]
        # params could be passed as keyword arg
        if isinstance(called_params, dict):
            assert called_params["topicSlug"] == "ai"
        else:
            # Check kwargs
            assert mock_get.call_args.kwargs["params"]["topicSlug"] == "ai"


# ---------------------------------------------------------------------------
# fetch() (mock _search_markets)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for ManifoldSource.fetch."""

    @patch.object(ManifoldSource, "_search_markets")
    def test_basic_fetch(self, mock_search, manifold_source):
        """Returns sorted ManifoldFetchFrame with correct IDs."""
        mock_search.return_value = {"id_b", "id_a", "id_c"}
        dff = manifold_source.fetch()

        assert len(dff) == 3
        assert dff["id"].tolist() == ["id_a", "id_b", "id_c"]
        ManifoldFetchFrame.validate(dff)

    @patch.object(ManifoldSource, "_search_markets")
    def test_empty_results(self, mock_search, manifold_source):
        """Empty search returns empty valid frame."""
        mock_search.return_value = set()
        dff = manifold_source.fetch()

        assert len(dff) == 0
        ManifoldFetchFrame.validate(dff)


# ---------------------------------------------------------------------------
# update() (mock _get_market + _build_resolution_df)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for ManifoldSource.update."""

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_new_id_appended(self, mock_market, mock_build, manifold_source):
        """IDs in dff not in dfq get appended with defaults."""
        mock_market.return_value = make_manifold_api_market(id="new_001")
        mock_build.return_value = make_resolution_df(
            [{"id": "new_001", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "existing_001"}])
        dff = make_manifold_fetch_df([{"id": "new_001"}])

        result = manifold_source.update(dfq, dff)

        assert "new_001" in result.dfq["id"].values
        assert len(result.dfq) == 2
        new_row = result.dfq[result.dfq["id"] == "new_001"].iloc[0]
        assert new_row["freeze_datetime_value_explanation"] == "The market value."

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_existing_unresolved_updated(self, mock_market, mock_build, manifold_source):
        """Unresolved question fields are updated from market details."""
        mock_market.return_value = make_manifold_api_market(
            id="mkt_001",
            question="Updated question text",
            textDescription="New background",
            url="https://manifold.markets/updated",
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "mkt_001", "date": "2024-06-01", "value": 0.65}]
        )
        dfq = make_question_df([{"id": "mkt_001", "resolved": False}])
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "mkt_001"].iloc[0]
        assert row["question"] == "Updated question text"
        assert row["background"] == "New background"
        assert row["url"] == "https://manifold.markets/updated"

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_market_becomes_resolved(self, mock_market, mock_build, manifold_source):
        """Market with isResolved=True marks dfq row as resolved."""
        mock_market.return_value = make_manifold_api_market(
            id="mkt_001",
            isResolved=True,
            resolution="YES",
            resolutionTime=1768310400000,  # 2026-01-13
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "mkt_001", "date": "2024-06-01", "value": 1.0}]
        )
        dfq = make_question_df([{"id": "mkt_001", "resolved": False}])
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "mkt_001"].iloc[0]
        assert bool(row["resolved"]) is True
        assert row["market_info_resolution_datetime"] != "N/A"

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_resolution_file_stored(self, mock_market, mock_build, manifold_source):
        """Resolution file from _build_resolution_df is in result."""
        res_df = make_resolution_df([{"id": "mkt_001", "date": "2024-06-01", "value": 0.5}])
        mock_market.return_value = make_manifold_api_market(id="mkt_001")
        mock_build.return_value = res_df
        dfq = make_question_df([{"id": "mkt_001", "resolved": False}])
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff)

        assert "mkt_001" in result.resolution_files

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_freeze_datetime_value_set(self, mock_market, mock_build, manifold_source):
        """freeze_datetime_value is set to last value of resolution df."""
        mock_market.return_value = make_manifold_api_market(id="mkt_001")
        mock_build.return_value = make_resolution_df(
            [
                {"id": "mkt_001", "date": "2024-06-01", "value": 0.3},
                {"id": "mkt_001", "date": "2024-06-02", "value": 0.75},
            ]
        )
        dfq = make_question_df([{"id": "mkt_001", "resolved": False}])
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == "mkt_001"].iloc[0]
        assert str(row["freeze_datetime_value"]) == "0.75"

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_build_resolution_returns_none(self, mock_market, mock_build, manifold_source):
        """_build_resolution_df returning None: no resolution file stored."""
        mock_market.return_value = make_manifold_api_market(id="mkt_001")
        mock_build.return_value = None
        dfq = make_question_df([{"id": "mkt_001", "resolved": False}])
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff)

        assert "mkt_001" not in (result.resolution_files or {})

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_regenerates_missing_resolved_files(self, mock_market, mock_build, manifold_source):
        """Resolved questions missing from storage get resolution files regenerated."""
        mock_market.return_value = make_manifold_api_market(
            id="mkt_001", isResolved=True, resolution="YES"
        )
        mock_build.return_value = make_resolution_df(
            [{"id": "mkt_001", "date": "2024-06-01", "value": 1.0}]
        )
        dfq = make_question_df(
            [
                {
                    "id": "mkt_001",
                    "resolved": True,
                    "market_info_resolution_datetime": "2024-07-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff, existing_resolution_ids=set())

        assert "mkt_001" in result.resolution_files

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_skips_resolved_already_in_storage(self, mock_market, mock_build, manifold_source):
        """Resolved questions with files in storage are not re-fetched."""
        dfq = make_question_df(
            [
                {
                    "id": "mkt_001",
                    "resolved": True,
                    "market_info_resolution_datetime": "2024-07-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_manifold_fetch_df([{"id": "mkt_001"}])

        result = manifold_source.update(dfq, dff, existing_resolution_ids={"mkt_001"})

        # _get_market should not be called for the resolved question
        mock_market.assert_not_called()
        assert "mkt_001" not in (result.resolution_files or {})

    @patch.object(ManifoldSource, "_build_resolution_df")
    @patch.object(ManifoldSource, "_get_market")
    def test_output_schema_valid(self, mock_market, mock_build, manifold_source):
        """Output dfq passes QuestionFrame validation."""
        mock_market.return_value = make_manifold_api_market(id="new_001")
        mock_build.return_value = make_resolution_df(
            [{"id": "new_001", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "existing_001"}])
        dff = make_manifold_fetch_df([{"id": "new_001"}])

        result = manifold_source.update(dfq, dff)
        QuestionFrame.validate(result.dfq)


# ---------------------------------------------------------------------------
# _get_market_bets (mock requests.get)
# ---------------------------------------------------------------------------


class TestGetMarketBets:
    """Tests for ManifoldSource._get_market_bets."""

    def _mock_response(self, bets):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = bets
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.manifold.requests.get")
    def test_single_page(self, mock_get, manifold_source):
        """Returns all bets from a single page (< limit)."""
        bets = [make_manifold_bet(id=f"b{i}", createdTime=1768046400000) for i in range(5)]
        mock_get.return_value = self._mock_response(bets)

        result = manifold_source._get_market_bets("mkt_001")

        assert len(result) == 5
        assert mock_get.call_count == 1

    @patch("sources.manifold.requests.get")
    def test_pagination(self, mock_get, manifold_source):
        """Multiple pages fetched until empty page."""
        page1 = [make_manifold_bet(id=f"b{i}", createdTime=1768046400000) for i in range(1000)]
        page2 = [
            make_manifold_bet(id=f"b{i}", createdTime=1768046400000) for i in range(1000, 1500)
        ]
        mock_get.side_effect = [
            self._mock_response(page1),
            self._mock_response(page2),
        ]

        result = manifold_source._get_market_bets("mkt_001")

        assert len(result) == 1500

    @patch("sources.manifold.requests.get")
    def test_stops_at_benchmark_start(self, mock_get, manifold_source):
        """Stops paginating when last bet's createdTime < BENCHMARK_START_DATE_EPOCHTIME_MS."""
        # Use a createdTime before benchmark start (2024-05-01)
        old_epoch_ms = 1704067200000  # 2024-01-01 (before benchmark)
        bets = [make_manifold_bet(id=f"b{i}", createdTime=old_epoch_ms) for i in range(1000)]
        mock_get.return_value = self._mock_response(bets)

        result = manifold_source._get_market_bets("mkt_001")

        assert len(result) == 1000
        # Only one call — stops because last bet is before benchmark start
        assert mock_get.call_count == 1

    @patch("sources.manifold.requests.get")
    def test_pagination_sets_before_param(self, mock_get, manifold_source):
        """Second request includes before=<last_bet_id> from first page."""
        page1 = [make_manifold_bet(id=f"b{i}", createdTime=1768046400000) for i in range(1000)]
        page2 = [make_manifold_bet(id="b1000", createdTime=1768046400000)]
        mock_get.side_effect = [
            self._mock_response(page1),
            self._mock_response(page2),
        ]

        manifold_source._get_market_bets("mkt_001")

        # Second call should have before=last bet's id from page1
        second_call_params = (
            mock_get.call_args_list[1][1].get("params") or mock_get.call_args_list[1][0][1]
        )
        assert second_call_params.get("before") == "b999"

    @patch("sources.manifold.requests.get")
    def test_empty_first_page(self, mock_get, manifold_source):
        """No bets returns empty list."""
        mock_get.return_value = self._mock_response([])

        result = manifold_source._get_market_bets("mkt_001")

        assert result == []
        assert mock_get.call_count == 1
