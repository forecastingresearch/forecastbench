"""Tests for FredSource: nullification rules plus fetch/update logic."""

from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from _schemas import FredFetchFrame, QuestionFrame
from helpers import fred as fred_helper
from sources.fred import FredSource

from .conftest import (
    make_forecast_df,
    make_fred_api_observations,
    make_fred_api_release,
    make_fred_api_series,
    make_fred_fetch_df,
    make_question_df,
    make_resolution_df,
)


@pytest.fixture(autouse=True)
def _no_throttle_sleep():
    """Neutralize the FRED request throttle so tests don't really sleep."""
    with patch("sources.fred.time.sleep"):
        yield


# ---------------------------------------------------------------------------
# Nullification definition
# ---------------------------------------------------------------------------


class TestFredNullificationDefinition:
    """Verify retired FRED series metadata is declared correctly."""

    def test_currcir_removed_from_fetch_pool(self):
        # NULLIFIED_IDS is exposed via the backwards-compat helpers shim (from metadata),
        # mirroring helpers/polymarket.py — the source module doesn't re-export it.
        assert "CURRCIR" in fred_helper.NULLIFIED_IDS
        assert all(question["id"] != "CURRCIR" for question in fred_helper.fred_questions)

    def test_ameribor_nullified(self):
        assert "AMERIBOR" in fred_helper.NULLIFIED_IDS


class TestFredSourceNullification:
    """Test that retired FRED series nullify by forecast date."""

    def test_currcir_pre_cutoff_question_not_nullified(self):
        source = FredSource()
        nullified = source.get_nullified_ids(as_of=date(2025, 10, 31))
        assert "CURRCIR" not in nullified

    def test_currcir_on_cutoff_question_is_nullified(self):
        source = FredSource()
        nullified = source.get_nullified_ids(as_of=date(2025, 11, 1))
        assert "CURRCIR" in nullified

    def test_currcir_pre_cutoff_question_resolves_normally(self):
        source = FredSource()
        df = make_forecast_df(
            [
                {
                    "id": "CURRCIR",
                    "source": "fred",
                    "forecast_due_date": "2025-10-31",
                    "resolution_date": "2025-12-31",
                }
            ]
        )
        dfq = make_question_df([{"id": "CURRCIR"}])
        dfr = make_resolution_df(
            [
                {"id": "CURRCIR", "date": "2025-10-31", "value": 100.0},
                {"id": "CURRCIR", "date": "2025-12-31", "value": 101.0},
            ]
        )

        result, _ = source.resolve(df, dfq, dfr, forecast_due_date=date(2025, 10, 31))

        row = result[result["id"] == "CURRCIR"].iloc[0]
        assert row["resolved_to"] == 1.0
        assert bool(row["resolved"]) is True

    def test_currcir_post_cutoff_question_is_nullified(self):
        source = FredSource()
        df = make_forecast_df(
            [
                {
                    "id": "CURRCIR",
                    "source": "fred",
                    "forecast_due_date": "2025-11-01",
                    "resolution_date": "2025-12-31",
                }
            ]
        )
        dfq = make_question_df([{"id": "CURRCIR"}])
        dfr = make_resolution_df([{"id": "CURRCIR", "date": "2025-10-31", "value": 100.0}])

        result, _ = source.resolve(df, dfq, dfr, forecast_due_date=date(2025, 11, 1))

        row = result[result["id"] == "CURRCIR"].iloc[0]
        assert pd.isna(row["resolved_to"])
        assert bool(row["resolved"]) is True

    def test_currcir_pre_cutoff_question_with_missing_resolution_data_is_unresolved(self):
        # A pre-cutoff forecast whose resolution_date lands after FRED stopped publishing
        # CURRCIR has no matching row in dfr, so resolve() returns resolved_to=NaN and
        # resolved=False. resolve_all() then drops the row as unresolved, which is the
        # intended nullification path for pre-cutoff forecasts resolving past the data cutoff.
        source = FredSource()
        df = make_forecast_df(
            [
                {
                    "id": "CURRCIR",
                    "source": "fred",
                    "forecast_due_date": "2025-10-15",
                    "resolution_date": "2026-01-31",
                }
            ]
        )
        dfq = make_question_df([{"id": "CURRCIR"}])
        dfr = make_resolution_df(
            [
                {"id": "CURRCIR", "date": "2025-10-15", "value": 100.0},
                {"id": "CURRCIR", "date": "2025-10-31", "value": 100.5},
            ]
        )

        result, _ = source.resolve(df, dfq, dfr, forecast_due_date=date(2025, 10, 15))

        row = result[result["id"] == "CURRCIR"].iloc[0]
        assert pd.isna(row["resolved_to"])
        assert bool(row["resolved"]) is False


# ---------------------------------------------------------------------------
# _throttle
# ---------------------------------------------------------------------------


class TestThrottle:
    """Tests for FredSource._throttle (FRED 2 req/s rate limit)."""

    def test_sleeps_when_called_too_soon(self, fred_source):
        """Sleeps the remaining interval when requests are too close together."""
        with patch("sources.fred.time.monotonic", side_effect=[100.1, 100.1]), patch(
            "sources.fred.time.sleep"
        ) as mock_sleep:
            fred_source._last_request_time = 100.0  # elapsed = 0.1 < 0.6
            fred_source._throttle()
            mock_sleep.assert_called_once()
            assert abs(mock_sleep.call_args[0][0] - 0.5) < 1e-9

    def test_no_sleep_when_interval_elapsed(self, fred_source):
        """Does not sleep when enough time has already passed."""
        with patch("sources.fred.time.monotonic", side_effect=[101.0, 101.0]), patch(
            "sources.fred.time.sleep"
        ) as mock_sleep:
            fred_source._last_request_time = 100.0  # elapsed = 1.0 > 0.6
            fred_source._throttle()
            mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# _combine_dicts (pure, no mocking)
# ---------------------------------------------------------------------------


class TestCombineDicts:
    """Tests for FredSource._combine_dicts static method."""

    def test_disjoint_dicts(self):
        d1 = {"A": {"id": "A", "series_name": "Series A"}}
        d2 = {"B": {"id": "B", "question": "Old question"}}
        result = FredSource._combine_dicts(d1, d2)
        assert set(result.keys()) == {"A", "B"}
        assert result["A"]["series_name"] == "Series A"
        assert result["B"]["question"] == "Old question"

    def test_overlapping_keys_merge(self):
        d1 = {"A": {"id": "A", "series_name": "New name"}}
        d2 = {"A": {"id": "A", "question": "Old question", "background": "Old bg"}}
        result = FredSource._combine_dicts(d1, d2)
        assert result["A"]["series_name"] == "New name"
        assert result["A"]["question"] == "Old question"

    def test_empty_dicts(self):
        assert FredSource._combine_dicts({}, {}) == {}
        assert FredSource._combine_dicts({"A": {"x": 1}}, {}) == {"A": {"x": 1}}
        assert FredSource._combine_dicts({}, {"B": {"y": 2}}) == {"B": {"y": 2}}


# ---------------------------------------------------------------------------
# _forward_fill_observations (pure, no mocking)
# ---------------------------------------------------------------------------


class TestForwardFillObservations:
    """Tests for FredSource._forward_fill_observations static method."""

    def test_fills_gaps(self):
        observations = [
            {"id": "X", "date": "2026-03-10", "value": 1.0},
            {"id": "X", "date": "2026-03-13", "value": 2.0},
        ]
        result = FredSource._forward_fill_observations(observations, date(2026, 3, 14))

        dates_list = [r["date"] for r in result]
        assert "2026-03-11" in dates_list
        assert "2026-03-12" in dates_list
        assert "2026-03-14" in dates_list
        assert result[1]["value"] == 1.0  # 03-11 filled from 03-10
        assert result[2]["value"] == 1.0  # 03-12 filled from 03-10
        assert result[3]["value"] == 2.0  # 03-13 actual value

    def test_deduplicates_dates(self):
        observations = [
            {"id": "X", "date": "2026-03-10", "value": 1.0},
            {"id": "X", "date": "2026-03-10", "value": 2.0},
        ]
        result = FredSource._forward_fill_observations(observations, date(2026, 3, 10))
        assert len(result) == 1
        assert result[0]["value"] == 2.0

    def test_extends_to_yesterday(self):
        observations = [{"id": "X", "date": "2026-03-10", "value": 5.0}]
        result = FredSource._forward_fill_observations(observations, date(2026, 3, 12))
        assert len(result) == 3  # 10, 11, 12
        assert result[-1]["date"] == "2026-03-12"
        assert result[-1]["value"] == 5.0

    def test_preserves_id(self):
        observations = [{"id": "DGS10", "date": "2026-03-10", "value": 4.0}]
        result = FredSource._forward_fill_observations(observations, date(2026, 3, 11))
        assert all(r["id"] == "DGS10" for r in result)


# ---------------------------------------------------------------------------
# _transform_series (pure, no mocking)
# ---------------------------------------------------------------------------


class TestTransformSeries:
    """Tests for FredSource._transform_series static method."""

    CURRENT_TIME = "2026-03-18T00:00:00+00:00"

    def _combined_question(self, **overrides):
        base = {
            "series_name": "the 10-year Treasury yield",
            "release": make_fred_api_release(),
            "series": [make_fred_api_series()],
            "observations": [{"id": "DGS10", "date": "2026-03-16", "value": 4.30}],
        }
        base.update(overrides)
        return base

    def test_standard_daily_series(self):
        row = FredSource._transform_series("DGS10", self._combined_question(), self.CURRENT_TIME)

        assert row["id"] == "DGS10"
        assert "{resolution_date}" in row["question"]
        assert "{forecast_due_date}" in row["question"]
        assert row["resolved"] is False
        assert row["probability"] == 4.30
        assert row["freeze_datetime_value"] == 4.30
        assert row["url"] == "https://fred.stlouisfed.org/series/DGS10"
        assert 7 in row["forecast_horizons"]
        FredFetchFrame.validate(pd.DataFrame([row]))

    def test_monthly_series_excludes_7day(self):
        series = make_fred_api_series(frequency_short="M", frequency="Monthly")
        row = FredSource._transform_series(
            "PAYEMS", self._combined_question(series=[series]), self.CURRENT_TIME
        )
        assert 7 not in row["forecast_horizons"]
        assert 30 in row["forecast_horizons"]

    def test_bank_only_series_uses_existing_question(self):
        cq = self._combined_question()
        del cq["series_name"]
        cq["question"] = "Will old series increase?"
        row = FredSource._transform_series("OLD_ID", cq, self.CURRENT_TIME)
        assert row["question"] == "Will old series increase?"

    def test_background_contains_release_and_series_info(self):
        row = FredSource._transform_series("DGS10", self._combined_question(), self.CURRENT_TIME)
        assert "Percent" in row["background"]
        assert "Not Seasonally Adjusted" in row["background"]

    def test_freeze_value_explanation(self):
        row = FredSource._transform_series("DGS10", self._combined_question(), self.CURRENT_TIME)
        assert "Market Yield" in row["freeze_datetime_value_explanation"]
        assert "H.15" in row["freeze_datetime_value_explanation"]


# ---------------------------------------------------------------------------
# _fetch_observations (mock _fetch_paginated_data)
# ---------------------------------------------------------------------------


class TestFetchObservations:
    """Tests for FredSource._fetch_observations."""

    @patch.object(FredSource, "_fetch_paginated_data")
    def test_filters_dot_values(self, mock_paginated, fred_source, freeze_today):
        freeze_today(date(2026, 3, 18))
        mock_paginated.return_value = make_fred_api_observations(
            date_values=[("2026-03-14", "4.25"), ("2026-03-15", "."), ("2026-03-16", "4.30")]
        )
        result = fred_source._fetch_observations("DGS10")

        assert len(result) == 2
        assert all(r["value"] != "." for r in result)
        assert all(isinstance(r["value"], float) for r in result)

    @patch.object(FredSource, "_fetch_paginated_data")
    def test_stale_data_returns_none(self, mock_paginated, fred_source, freeze_today):
        freeze_today(date(2026, 3, 18))
        mock_paginated.return_value = make_fred_api_observations(
            date_values=[("2025-12-01", "100.0")]
        )
        result = fred_source._fetch_observations("STALE_SERIES")
        assert result is None

    @patch.object(FredSource, "_fetch_paginated_data")
    def test_recent_data_returns_observations(self, mock_paginated, fred_source, freeze_today):
        freeze_today(date(2026, 3, 18))
        mock_paginated.return_value = make_fred_api_observations(
            date_values=[("2026-03-10", "1.5"), ("2026-03-16", "1.7")]
        )
        result = fred_source._fetch_observations("DGS10")
        assert len(result) == 2
        assert result[0] == {"id": "DGS10", "date": "2026-03-10", "value": 1.5}


# ---------------------------------------------------------------------------
# _fetch_paginated_data (mock requests.get)
# ---------------------------------------------------------------------------


class TestFetchPaginatedData:
    """Tests for FredSource._fetch_paginated_data."""

    def _mock_response(self, data):
        resp = Mock()
        resp.json.return_value = data
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.fred.requests.get")
    def test_single_page(self, mock_get, fred_source):
        mock_get.return_value = self._mock_response({"releases": [{"id": 1}, {"id": 2}]})
        result = fred_source._fetch_paginated_data(
            url="http://test",
            params={"api_key": "k", "file_type": "json"},
            field_name="releases",
            pagination=False,
        )
        assert len(result) == 2
        assert mock_get.call_count == 1

    @patch("sources.fred.requests.get")
    def test_multi_page(self, mock_get, fred_source):
        mock_get.side_effect = [
            self._mock_response({"data": [{"id": 1}]}),
            self._mock_response({"data": [{"id": 2}]}),
            self._mock_response({"data": []}),
        ]
        result = fred_source._fetch_paginated_data(
            url="http://test",
            params={"api_key": "k", "file_type": "json", "limit": 1},
            field_name="data",
            pagination=True,
        )
        assert len(result) == 2
        assert mock_get.call_count == 3

    @patch("sources.fred.requests.get")
    def test_max_pages(self, mock_get, fred_source):
        mock_get.side_effect = [self._mock_response({"data": [{"id": i}]}) for i in range(10)]
        result = fred_source._fetch_paginated_data(
            url="http://test",
            params={"api_key": "k", "file_type": "json", "limit": 1},
            field_name="data",
            pagination=3,
        )
        assert len(result) == 3
        assert mock_get.call_count == 3

    @patch("sources.fred.requests.get")
    def test_empty_first_page(self, mock_get, fred_source):
        mock_get.return_value = self._mock_response({"data": []})
        result = fred_source._fetch_paginated_data(
            url="http://test",
            params={"api_key": "k", "file_type": "json"},
            field_name="data",
            pagination=True,
        )
        assert result == []

    @patch.object(FredSource, "_throttle")
    @patch("sources.fred.requests.get")
    def test_throttles_before_every_request(self, mock_get, mock_throttle, fred_source):
        """FRED's ~2 req/s limit is honored: _throttle runs before each request (prod fix)."""
        mock_get.side_effect = [
            self._mock_response({"data": [{"id": 1}]}),
            self._mock_response({"data": [{"id": 2}]}),
            self._mock_response({"data": []}),
        ]
        fred_source._fetch_paginated_data(
            url="http://test",
            params={"api_key": "k", "file_type": "json", "limit": 1},
            field_name="data",
            pagination=True,
        )
        assert mock_throttle.call_count == mock_get.call_count


# ---------------------------------------------------------------------------
# fetch() (mock private API methods + patch _FRED_QUESTIONS)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for FredSource.fetch."""

    @patch.object(FredSource, "questions", [{"id": "DGS10", "series_name": "10-year Treasury"}])
    @patch.object(FredSource, "_fetch_observations")
    @patch.object(FredSource, "_fetch_series_info")
    @patch.object(FredSource, "_fetch_release")
    def test_basic_fetch(self, mock_release, mock_series, mock_obs, fred_source, freeze_today):
        freeze_today(date(2026, 3, 18))
        mock_release.return_value = make_fred_api_release()
        mock_series.return_value = [make_fred_api_series()]
        mock_obs.return_value = [
            {"id": "DGS10", "date": "2026-03-16", "value": 4.30},
            {"id": "DGS10", "date": "2026-03-17", "value": 4.35},
        ]

        dff = fred_source.fetch(dfq=None)

        assert len(dff) == 1
        assert dff.iloc[0]["id"] == "DGS10"
        # fetch threads `yesterday` (03-17) into the embedded series' forward-fill,
        # and freeze/probability take the latest (yesterday's) value.
        resolutions = dff.iloc[0]["resolutions"]
        assert resolutions[-1]["date"] == "2026-03-17"
        assert dff.iloc[0]["probability"] == 4.35
        FredFetchFrame.validate(dff)

    @patch.object(
        FredSource,
        "questions",
        [{"id": "GOOD", "series_name": "Good series"}, {"id": "BAD", "series_name": "Bad series"}],
    )
    @patch.object(FredSource, "_fetch_observations")
    @patch.object(FredSource, "_fetch_series_info")
    @patch.object(FredSource, "_fetch_release")
    def test_series_with_no_observations_dropped(
        self, mock_release, mock_series, mock_obs, fred_source, freeze_today
    ):
        freeze_today(date(2026, 3, 18))
        mock_release.return_value = make_fred_api_release()
        mock_series.return_value = [make_fred_api_series()]
        mock_obs.side_effect = [
            [{"id": "GOOD", "date": "2026-03-16", "value": 1.0}],
            None,  # BAD has no observations
        ]

        dff = fred_source.fetch(dfq=None)

        assert len(dff) == 1
        assert dff.iloc[0]["id"] == "GOOD"

    @patch.object(FredSource, "questions", [{"id": "DGS10", "series_name": "10-year Treasury"}])
    @patch.object(FredSource, "_fetch_observations")
    @patch.object(FredSource, "_fetch_series_info")
    @patch.object(FredSource, "_fetch_release")
    def test_bank_only_questions_included(
        self, mock_release, mock_series, mock_obs, fred_source, freeze_today
    ):
        freeze_today(date(2026, 3, 18))
        mock_release.return_value = make_fred_api_release()
        mock_series.return_value = [make_fred_api_series()]
        mock_obs.return_value = [{"id": "DGS10", "date": "2026-03-16", "value": 4.30}]
        dfq = make_question_df(
            [
                {"id": "DGS10", "question": "Will DGS10 increase?"},
                {"id": "OLD_SERIES", "question": "Will OLD increase?"},
            ]
        )

        dff = fred_source.fetch(dfq=dfq)

        assert "OLD_SERIES" in dff["id"].values

    @patch.object(FredSource, "questions", [{"id": "DGS10", "series_name": "10-year Treasury"}])
    @patch.object(FredSource, "_fetch_observations")
    @patch.object(FredSource, "_fetch_series_info")
    @patch.object(FredSource, "_fetch_release")
    def test_nullified_ids_excluded(
        self, mock_release, mock_series, mock_obs, fred_source, freeze_today
    ):
        freeze_today(date(2026, 3, 18))
        mock_release.return_value = make_fred_api_release()
        mock_series.return_value = [make_fred_api_series()]
        mock_obs.return_value = [{"id": "DGS10", "date": "2026-03-16", "value": 4.30}]
        dfq = make_question_df([{"id": "DGS10"}, {"id": "AMERIBOR"}])  # AMERIBOR nullified

        dff = fred_source.fetch(dfq=dfq)

        assert "AMERIBOR" not in dff["id"].values

    def test_api_key_required(self):
        src = FredSource()
        with pytest.raises(RuntimeError, match="api_key must be set"):
            src.fetch()


# ---------------------------------------------------------------------------
# update() (pure data transformation)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for FredSource.update."""

    def test_basic_update(self, fred_source):
        dfq = make_question_df([{"id": "EXISTING"}])
        dff = make_fred_fetch_df([{"id": "DGS10"}])

        result = fred_source.update(dfq, dff)

        assert "DGS10" in result.dfq["id"].values
        assert "DGS10" in result.resolution_files
        QuestionFrame.validate(result.dfq)

    def test_new_question_inserted(self, fred_source):
        dfq = make_question_df([{"id": "EXISTING"}])
        dff = make_fred_fetch_df([{"id": "NEW_SERIES"}])

        result = fred_source.update(dfq, dff)

        assert len(result.dfq) == 2
        assert set(result.dfq["id"].tolist()) == {"EXISTING", "NEW_SERIES"}

    def test_existing_question_updated(self, fred_source):
        dfq = make_question_df([{"id": "DGS10", "question": "Old text"}])
        dff = make_fred_fetch_df([{"id": "DGS10", "question": "New text"}])

        result = fred_source.update(dfq, dff)

        assert len(result.dfq) == 1
        assert result.dfq.iloc[0]["question"] == "New text"

    def test_resolution_files_extracted(self, fred_source):
        resolutions = [
            {"id": "X", "date": "2026-03-14", "value": 1.0},
            {"id": "X", "date": "2026-03-15", "value": 1.5},
        ]
        dff = make_fred_fetch_df([{"id": "X", "resolutions": resolutions}])
        dfq = make_question_df([{"id": "placeholder"}]).iloc[:0]

        result = fred_source.update(dfq, dff)

        assert "X" in result.resolution_files
        df_res = result.resolution_files["X"]
        assert len(df_res) == 2
        assert list(df_res.columns) == ["id", "date", "value"]

    def test_transient_fields_stripped(self, fred_source):
        dfq = make_question_df([{"id": "placeholder"}]).iloc[:0]
        dff = make_fred_fetch_df([{"id": "DGS10"}])

        result = fred_source.update(dfq, dff)

        for col in ["fetch_datetime", "probability", "resolutions"]:
            assert col not in result.dfq.columns

    def test_nullified_ids_dropped_from_dfq(self, fred_source):
        dfq = make_question_df([{"id": "AMERIBOR"}, {"id": "CURRCIR"}, {"id": "DGS10"}])
        dff = make_fred_fetch_df([{"id": "DGS10"}])

        result = fred_source.update(dfq, dff)

        assert "AMERIBOR" not in result.dfq["id"].values
        assert "CURRCIR" not in result.dfq["id"].values

    def test_update_works_without_api_key(self):
        src = FredSource()  # no api_key set
        dfq = make_question_df([{"id": "placeholder"}]).iloc[:0]
        dff = make_fred_fetch_df([{"id": "DGS10"}])
        result = src.update(dfq, dff)
        assert "DGS10" in result.dfq["id"].values
