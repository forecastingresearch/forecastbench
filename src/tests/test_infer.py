"""Tests for InferSource fetch/update logic."""

from datetime import date
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from _schemas import InferFetchFrame, QuestionFrame, ResolutionFrame
from sources.infer import InferSource

from .conftest import (
    make_infer_api_question,
    make_infer_fetch_df,
    make_infer_prediction_set,
    make_question_df,
    make_resolution_df,
)

# ---------------------------------------------------------------------------
# _transform_question (pure, no mocking)
# ---------------------------------------------------------------------------


class TestTransformQuestion:
    """Tests for InferSource._transform_question static method."""

    CURRENT_TIME = "2026-01-15T00:00:00+00:00"

    def test_standard_active_question(self):
        """All fields populated, output matches InferFetchFrame schema."""
        q = make_infer_api_question()
        row = InferSource._transform_question(q, self.CURRENT_TIME)

        assert row["id"] == "9999"
        assert row["question"] == q["name"]
        assert row["probability"] == 0.65
        assert row["nullify_question"] is False
        assert row["resolved"] is False
        assert row["market_info_resolution_datetime"] == "N/A"
        assert row["fetch_datetime"] == self.CURRENT_TIME
        # Verify schema compliance
        df = pd.DataFrame([row])
        InferFetchFrame.validate(df)

    def test_resolved_question(self):
        """Resolved question has resolution datetime set."""
        q = make_infer_api_question(
            **{
                "resolved?": True,
                "resolved_at": "2026-01-10T12:00:00.000Z",
                "scoring_end_time": "2026-02-01T00:00:00.000-05:00",
            }
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)

        assert bool(row["resolved"]) is True
        assert row["market_info_resolution_datetime"] != "N/A"
        assert "2026-01-10" in row["market_info_resolution_datetime"]

    def test_non_binary_question_nullified(self):
        """Non-YesNo question types get nullified."""
        q = make_infer_api_question(type="Forecast::MultipleChoiceQuestion")
        row = InferSource._transform_question(q, self.CURRENT_TIME)

        assert row["nullify_question"] is True
        assert row["probability"] == "N/A"

    def test_missing_datetime_fields(self):
        """None datetimes produce N/A strings."""
        q = make_infer_api_question(
            scoring_start_time=None,
            scoring_end_time=None,
            ends_at=None,
            resolved_at=None,
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)

        assert row["market_info_open_datetime"] == "N/A"
        assert row["market_info_close_datetime"] == "N/A"

    def test_close_datetime_picks_earlier(self):
        """Close datetime is min(scoring_end_time, ends_at)."""
        q = make_infer_api_question(
            scoring_end_time="2026-03-01T00:00:00.000-05:00",
            ends_at="2026-06-01T04:00:00.000Z",
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)
        assert "2026-03" in row["market_info_close_datetime"]

        # Reverse: ends_at is earlier
        q2 = make_infer_api_question(
            scoring_end_time="2026-09-01T00:00:00.000-05:00",
            ends_at="2026-06-01T04:00:00.000Z",
        )
        row2 = InferSource._transform_question(q2, self.CURRENT_TIME)
        assert "2026-06" in row2["market_info_close_datetime"]

    def test_resolution_datetime_picks_earlier(self):
        """Resolution datetime is min(resolved_at, close_datetime)."""
        q = make_infer_api_question(
            **{
                "resolved?": True,
                "resolved_at": "2026-02-01T00:00:00.000Z",
                "scoring_end_time": "2026-06-01T00:00:00.000-05:00",
            }
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)
        assert "2026-02-01" in row["market_info_resolution_datetime"]

    def test_answers_swapped_order(self):
        """Extracts Yes probability even when No is first."""
        q = make_infer_api_question(
            answers=[
                {"name": "No", "probability": 0.3, "predictions_count": 10},
                {"name": "Yes", "probability": 0.7, "predictions_count": 10},
            ]
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)
        assert row["probability"] == 0.7

    def test_single_answer(self):
        """Single-answer question still extracts probability."""
        q = make_infer_api_question(
            answers=[{"name": "Yes", "probability": 0.8, "predictions_count": 5}]
        )
        # Single answer → len != 2, so probability is N/A (binary check fails)
        row = InferSource._transform_question(q, self.CURRENT_TIME)
        assert row["probability"] == "N/A"

    def test_clarifications_joined(self):
        """Multiple clarifications are joined into one string."""
        q = make_infer_api_question(
            clarifications=[
                {"content": "Clarification 1."},
                {"content": "Clarification 2."},
            ]
        )
        row = InferSource._transform_question(q, self.CURRENT_TIME)
        assert "Clarification 1." in row["market_info_resolution_criteria"]
        assert "Clarification 2." in row["market_info_resolution_criteria"]


# ---------------------------------------------------------------------------
# _finalize_resolution_df (pure, no mocking)
# ---------------------------------------------------------------------------


class TestFinalizeResolutionDf:
    """Tests for InferSource._finalize_resolution_df static method."""

    def test_filters_before_benchmark_start(self):
        """Rows before BENCHMARK_START_DATE are dropped."""
        df = pd.DataFrame(
            {
                "id": ["A", "A", "A"],
                "date": pd.to_datetime(["2020-01-01", "2024-06-01", "2024-07-01"]),
                "value": [0.1, 0.2, 0.3],
            }
        )
        result = InferSource._finalize_resolution_df(df)
        assert len(result) == 2
        assert result["value"].tolist() == [0.2, 0.3]

    def test_validates_schema(self):
        """Output is a valid ResolutionFrame."""
        df = pd.DataFrame(
            {
                "id": ["A"],
                "date": pd.to_datetime(["2024-06-01"]),
                "value": [0.5],
            }
        )
        result = InferSource._finalize_resolution_df(df)
        ResolutionFrame.validate(result)

    def test_only_keeps_id_date_value(self):
        """Extra columns are stripped."""
        df = pd.DataFrame(
            {
                "id": ["A"],
                "date": pd.to_datetime(["2024-06-01"]),
                "value": [0.5],
                "extra": ["junk"],
            }
        )
        result = InferSource._finalize_resolution_df(df)
        assert list(result.columns) == ["id", "date", "value"]


# ---------------------------------------------------------------------------
# _build_resolution_file (mock _get_historical_forecasts)
# ---------------------------------------------------------------------------


class TestBuildResolutionFile:
    """Tests for InferSource._build_resolution_file."""

    def _question(self, **overrides):
        base = {
            "id": "200",
            "nullify_question": False,
            "market_info_resolution_datetime": "N/A",
            "probability": 0.6,
        }
        base.update(overrides)
        return base

    @patch.object(InferSource, "_get_historical_forecasts")
    def test_nullified_no_existing(self, mock_hist, infer_source, freeze_today):
        """Nullified question with no existing data returns single NaN row."""
        freeze_today(date(2026, 1, 15))
        q = self._question(nullify_question=True)
        df = infer_source._build_resolution_file(q, resolved=False, existing_df=None)

        assert len(df) == 1
        assert np.isnan(df["value"].iloc[0])
        mock_hist.assert_not_called()

    @patch.object(InferSource, "_get_historical_forecasts")
    def test_nullified_with_existing(self, mock_hist, infer_source, freeze_today):
        """Nullified question with existing data sets all values to NaN."""
        freeze_today(date(2026, 1, 15))
        existing = make_resolution_df(
            [
                {"id": "200", "date": "2024-06-01", "value": 0.5},
                {"id": "200", "date": "2024-06-02", "value": 0.6},
            ]
        )
        q = self._question(nullify_question=True)
        df = infer_source._build_resolution_file(q, resolved=False, existing_df=existing)

        assert df["value"].isna().all()
        mock_hist.assert_not_called()

    @patch.object(InferSource, "_get_historical_forecasts")
    def test_already_up_to_date(self, mock_hist, infer_source, freeze_today):
        """Skips API call if existing data covers through yesterday."""
        freeze_today(date(2026, 1, 15))
        existing = make_resolution_df(
            [
                {"id": "200", "date": "2024-06-01", "value": 0.5},
                {"id": "200", "date": "2026-01-14", "value": 0.6},
            ]
        )
        q = self._question()
        df = infer_source._build_resolution_file(q, resolved=False, existing_df=existing)

        assert df.equals(existing)
        mock_hist.assert_not_called()

    @patch.object(InferSource, "_get_historical_forecasts")
    def test_fetches_when_stale(self, mock_hist, infer_source, freeze_today):
        """Calls _get_historical_forecasts when existing data is stale."""
        freeze_today(date(2026, 1, 15))
        mock_hist.return_value = make_resolution_df(
            [
                {"id": "200", "date": "2024-06-01", "value": 0.5},
                {"id": "200", "date": "2026-01-14", "value": 0.65},
            ]
        )
        existing = make_resolution_df([{"id": "200", "date": "2024-06-01", "value": 0.5}])
        q = self._question()
        df = infer_source._build_resolution_file(q, resolved=False, existing_df=existing)

        assert not df.empty
        mock_hist.assert_called_once()

    @patch.object(InferSource, "_get_historical_forecasts")
    def test_resolved_truncates_and_appends(self, mock_hist, infer_source, freeze_today):
        """Resolved question truncates at resolution date and appends final row."""
        freeze_today(date(2026, 1, 15))
        mock_hist.return_value = make_resolution_df(
            [
                {"id": "200", "date": "2024-06-01", "value": 0.4},
                {"id": "200", "date": "2026-01-10", "value": 0.6},
                {"id": "200", "date": "2026-01-12", "value": 0.7},
            ]
        )
        q = self._question(
            market_info_resolution_datetime="2026-01-11T00:00:00+00:00",
            probability=1.0,
        )
        df = infer_source._build_resolution_file(q, resolved=True, existing_df=None)

        # Should have rows up to resolution date
        assert not df.empty
        # Last row should be the resolution value
        assert float(df.iloc[-1]["value"]) == 1.0


# ---------------------------------------------------------------------------
# fetch() (mock _fetch_questions_from_api)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for InferSource.fetch."""

    @patch.object(InferSource, "_fetch_questions_from_api")
    def test_basic_fetch(self, mock_api, infer_source):
        """Returns InferFetchFrame with correct rows."""
        mock_api.return_value = [
            make_infer_api_question(id=200),
            make_infer_api_question(id=201),
        ]
        dff = infer_source.fetch()

        assert len(dff) == 2
        InferFetchFrame.validate(dff)

    @patch.object(InferSource, "_fetch_questions_from_api")
    def test_active_filter(self, mock_api, infer_source):
        """Only active binary questions with predictions pass the filter."""
        mock_api.return_value = [
            make_infer_api_question(id=1, state="active"),
            make_infer_api_question(id=2, state="closed"),  # filtered out
            make_infer_api_question(id=3, type="Forecast::MultipleChoiceQuestion"),  # filtered out
            make_infer_api_question(
                id=4,
                answers=[
                    {"name": "Yes", "probability": 0.5, "predictions_count": 0},
                    {"name": "No", "probability": 0.5, "predictions_count": 0},
                ],
            ),  # filtered out (no predictions)
        ]
        dff = infer_source.fetch()
        assert len(dff) == 1
        assert dff.iloc[0]["id"] == "1"

    @patch.object(InferSource, "_fetch_questions_from_api")
    def test_deduplication_active_wins(self, mock_api, infer_source):
        """When same ID appears in both active and existing, active version wins."""
        mock_api.side_effect = [
            [make_infer_api_question(id=100, state="closed")],  # existing re-fetch
            [make_infer_api_question(id=100, state="active")],  # active fetch
        ]
        dfq = make_question_df([{"id": "100", "resolved": False}])
        dff = infer_source.fetch(dfq=dfq, files_in_storage=[])

        assert len(dff) == 1

    @patch.object(InferSource, "_fetch_questions_from_api")
    def test_resolved_without_files_refetched(self, mock_api, infer_source):
        """Resolved questions missing resolution files are re-fetched."""
        mock_api.side_effect = [
            [make_infer_api_question(id=100, state="resolved", **{"resolved?": True})],
            [],  # no active
        ]
        dfq = make_question_df([{"id": "100", "resolved": True}])
        # No resolution file in storage → should re-fetch
        dff = infer_source.fetch(dfq=dfq, files_in_storage=[])

        assert len(dff) == 1
        mock_api.assert_any_call(status="all", question_ids=["100"])

    @patch.object(InferSource, "_fetch_questions_from_api")
    def test_empty_dfq(self, mock_api, infer_source):
        """Works with no existing questions."""
        mock_api.side_effect = [
            [make_infer_api_question(id=300)],
        ]
        dff = infer_source.fetch(dfq=None, files_in_storage=[])
        assert len(dff) == 1

    def test_api_key_required(self):
        """Raises RuntimeError if api_key not set."""
        src = InferSource()  # no api_key
        with pytest.raises(RuntimeError, match="api_key must be set"):
            src.fetch()


# ---------------------------------------------------------------------------
# update() (mock _build_resolution_file)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for InferSource.update."""

    @patch.object(InferSource, "_build_resolution_file")
    def test_basic_update(self, mock_build, infer_source):
        """Returns UpdateResult with valid dfq and resolution files."""
        mock_build.return_value = make_resolution_df(
            [{"id": "200", "date": "2024-06-01", "value": 0.65}]
        )
        dfq = make_question_df([{"id": "100"}])
        dff = make_infer_fetch_df([{"id": "200"}])

        result = infer_source.update(dfq, dff)

        assert "200" in result.dfq["id"].values
        assert "200" in result.resolution_files
        QuestionFrame.validate(result.dfq)

    @patch.object(InferSource, "_build_resolution_file")
    def test_new_question_inserted(self, mock_build, infer_source):
        """Question not in dfq gets appended."""
        mock_build.return_value = make_resolution_df(
            [{"id": "300", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "100"}])
        dff = make_infer_fetch_df([{"id": "300"}])

        result = infer_source.update(dfq, dff)
        assert len(result.dfq) == 2
        assert set(result.dfq["id"].tolist()) == {"100", "300"}

    @patch.object(InferSource, "_build_resolution_file")
    def test_existing_question_updated(self, mock_build, infer_source):
        """Existing question fields are updated in place."""
        mock_build.return_value = make_resolution_df(
            [{"id": "100", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "100", "question": "Old text"}])
        dff = make_infer_fetch_df([{"id": "100", "question": "New text"}])

        result = infer_source.update(dfq, dff)
        assert len(result.dfq) == 1
        assert result.dfq.iloc[0]["question"] == "New text"

    @patch.object(InferSource, "_build_resolution_file")
    def test_nullified_marked_resolved(self, mock_build, infer_source):
        """Nullified questions are marked as resolved in dfq."""
        mock_build.return_value = make_resolution_df(
            [{"id": "200", "date": "2024-06-01", "value": np.nan}]
        )
        dfq = make_question_df([{"id": "100"}])
        dff = make_infer_fetch_df([{"id": "200", "nullify_question": True}])

        result = infer_source.update(dfq, dff)
        row = result.dfq[result.dfq["id"] == "200"].iloc[0]
        assert bool(row["resolved"]) is True

    @patch.object(InferSource, "_build_resolution_file")
    def test_transient_fields_stripped(self, mock_build, infer_source):
        """fetch_datetime, probability, nullify_question not in output dfq."""
        mock_build.return_value = make_resolution_df(
            [{"id": "200", "date": "2024-06-01", "value": 0.5}]
        )
        dfq = make_question_df([{"id": "placeholder"}]).iloc[:0]
        dff = make_infer_fetch_df([{"id": "200"}])

        result = infer_source.update(dfq, dff)
        for col in ["fetch_datetime", "probability", "nullify_question"]:
            assert col not in result.dfq.columns

    def test_api_key_required(self):
        """Raises RuntimeError if api_key not set."""
        src = InferSource()
        dfq = make_question_df([{"id": "100"}])
        dff = make_infer_fetch_df([{"id": "200"}])
        with pytest.raises(RuntimeError, match="api_key must be set"):
            src.update(dfq, dff)


# ---------------------------------------------------------------------------
# _get_historical_forecasts (mock requests.get)
# ---------------------------------------------------------------------------


class TestGetHistoricalForecasts:
    """Tests for InferSource._get_historical_forecasts."""

    def _mock_response(self, prediction_sets):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"prediction_sets": prediction_sets}
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.infer.requests.get")
    def test_basic_fetch_no_existing(self, mock_get, infer_source, freeze_today):
        """Builds time series from scratch."""
        freeze_today(date(2026, 1, 15))
        mock_get.side_effect = [
            self._mock_response(
                [
                    make_infer_prediction_set("2026-01-10T12:00:00.000Z", 0.4),
                    make_infer_prediction_set("2026-01-12T14:00:00.000Z", 0.6),
                ]
            ),
            self._mock_response([]),  # empty page stops pagination
        ]

        df = infer_source._get_historical_forecasts(None, "200")

        assert not df.empty
        assert list(df.columns) == ["id", "date", "value"]
        assert (df["id"] == "200").all()
        # Should have forward-filled dates between 10th and 14th
        assert len(df) >= 4

    @patch("sources.infer.requests.get")
    def test_incremental_with_existing(self, mock_get, infer_source, freeze_today):
        """Only fetches newer predictions when existing data provided."""
        freeze_today(date(2026, 1, 15))
        existing = make_resolution_df(
            [
                {"id": "200", "date": "2026-01-10", "value": 0.4},
                {"id": "200", "date": "2026-01-11", "value": 0.4},
            ]
        )
        mock_get.side_effect = [
            self._mock_response([make_infer_prediction_set("2026-01-13T12:00:00.000Z", 0.7)]),
            self._mock_response([]),
        ]

        df = infer_source._get_historical_forecasts(existing, "200")

        assert not df.empty
        # Should contain both old and new data, forward-filled
        assert len(df) >= 4

    @patch("sources.infer.requests.get")
    def test_forward_fill_gaps(self, mock_get, infer_source, freeze_today):
        """Missing dates between predictions are forward-filled."""
        freeze_today(date(2026, 1, 15))
        mock_get.side_effect = [
            self._mock_response(
                [
                    make_infer_prediction_set("2026-01-10T12:00:00.000Z", 0.3),
                    make_infer_prediction_set("2026-01-13T12:00:00.000Z", 0.8),
                ]
            ),
            self._mock_response([]),
        ]

        df = infer_source._get_historical_forecasts(None, "200")

        # Dates 10, 11, 12, 13, 14 should exist (14 = today-1)
        dates_in_df = pd.to_datetime(df["date"]).dt.date.tolist()
        assert date(2026, 1, 11) in dates_in_df  # forward-filled
        assert date(2026, 1, 12) in dates_in_df  # forward-filled

    @patch("sources.infer.requests.get")
    @patch("sources.infer.time.sleep")
    def test_rate_limit_retry(self, mock_sleep, mock_get, infer_source, freeze_today):
        """429 response triggers retry after sleep."""
        freeze_today(date(2026, 1, 15))

        rate_limit_resp = Mock()
        rate_limit_resp.raise_for_status.side_effect = __import__("requests").exceptions.HTTPError(
            response=Mock(status_code=429)
        )

        ok_resp = self._mock_response([make_infer_prediction_set("2026-01-10T12:00:00.000Z", 0.5)])
        empty_resp = self._mock_response([])

        mock_get.side_effect = [rate_limit_resp, ok_resp, empty_resp]

        df = infer_source._get_historical_forecasts(None, "200")

        assert not df.empty
        mock_sleep.assert_called_once_with(10)
