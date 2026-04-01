"""Integration tests: end-to-end resolution pipelines with small realistic data."""

from datetime import date

import numpy as np
import pandas as pd

from _fb_types import SourceQuestionBank
from resolve._impute import impute_missing_forecasts
from resolve._prepare import check_and_prepare_forecast_file, set_resolution_dates
from resolve.explode_question_set import explode_question_set
from resolve.resolve_all import resolve_all
from sources import SOURCES
from tests.conftest import make_question_df, make_question_set_df, make_resolution_df

# ---------------------------------------------------------------------------
# Integration Test 1: Market source end-to-end
# ---------------------------------------------------------------------------


class TestMarketEndToEnd:
    """Market source: question set → explode → resolve_all → verify resolved_to."""

    def test_market_pipeline(self, freeze_today):
        freeze_today(date(2025, 2, 1))

        # Build question set with 2 standard + 1 combo market question
        question_set_df = make_question_set_df(
            [
                {"id": "m1", "source": "metaculus", "resolution_dates": "N/A"},
                {"id": "m2", "source": "metaculus", "resolution_dates": "N/A"},
                {
                    "id": ("m1", "m2"),
                    "source": "metaculus",
                    "resolution_dates": "N/A",
                },
                # A dataset question to generate resolution dates
                {
                    "id": "d1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
            ]
        )

        # Explode
        exploded = explode_question_set(question_set_df, "2025-01-01")

        # Only market questions in this test
        exploded = exploded[exploded["source"] == "metaculus"].copy()
        assert len(exploded) > 0

        # Build question bank
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
        question_bank = {
            "metaculus": SourceQuestionBank(dfq=dfq, dfr=dfr),
        }

        # Resolve
        result, _ = resolve_all(
            exploded,
            question_bank=question_bank,
            sources={"metaculus": SOURCES["metaculus"]},
            forecast_due_date=date(2025, 1, 1),
        )

        # All rows should be resolved (yesterday = Jan 31)
        assert len(result) > 0
        assert result["resolved_to"].notna().all()


# ---------------------------------------------------------------------------
# Integration Test 2: Dataset source end-to-end
# ---------------------------------------------------------------------------


class TestDatasetEndToEnd:
    """Dataset source: question set → explode → resolve_all → verify resolved_to."""

    def test_dataset_pipeline(self, freeze_today):
        """Dataset source: explode → resolve → verify specific resolved_to values."""
        freeze_today(date(2025, 3, 1))

        # Build question set
        question_set_df = make_question_set_df(
            [
                {
                    "id": "d1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08", "2025-01-31"],
                },
                {
                    "id": "d2",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08", "2025-01-31"],
                },
            ]
        )

        # Explode
        exploded = explode_question_set(question_set_df, "2025-01-01")
        assert len(exploded) == 4  # 2 questions x 2 dates

        # Build question bank
        dfq = make_question_df([{"id": "d1"}, {"id": "d2"}])
        dfr = make_resolution_df(
            [
                {"id": "d1", "date": "2025-01-01", "value": 100},
                {"id": "d1", "date": "2025-01-08", "value": 110},  # > 100 → 1.0
                {"id": "d1", "date": "2025-01-31", "value": 90},  # < 100 → 0.0
                {"id": "d2", "date": "2025-01-01", "value": 50},
                {"id": "d2", "date": "2025-01-08", "value": 60},  # > 50 → 1.0
                {"id": "d2", "date": "2025-01-31", "value": 70},  # > 50 → 1.0
            ]
        )
        question_bank = {
            "fred": SourceQuestionBank(dfq=dfq, dfr=dfr),
        }

        # Resolve
        result, _ = resolve_all(
            exploded,
            question_bank=question_bank,
            sources={"fred": SOURCES["fred"]},
            forecast_due_date=date(2025, 1, 1),
        )

        # Check specific resolutions
        d1_jan8 = result[
            (result["id"] == "d1") & (result["resolution_date"] == pd.Timestamp("2025-01-08"))
        ]
        assert len(d1_jan8) == 1
        assert d1_jan8.iloc[0]["resolved_to"] == 1.0

        d1_jan31 = result[
            (result["id"] == "d1") & (result["resolution_date"] == pd.Timestamp("2025-01-31"))
        ]
        assert len(d1_jan31) == 1
        assert d1_jan31.iloc[0]["resolved_to"] == 0.0


# ---------------------------------------------------------------------------
# Integration Test 3: Full mixed pipeline
# ---------------------------------------------------------------------------


class TestFullMixedPipeline:
    """Full pipeline: prepare → explode → resolve → impute."""

    def test_full_pipeline(self, freeze_today):
        """Full pipeline: prepare → explode → resolve → impute with mixed sources."""
        freeze_today(date(2025, 3, 1))

        # 1. Build question set and explode
        question_set_df = make_question_set_df(
            [
                {
                    "id": "d1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
                {
                    "id": "m1",
                    "source": "metaculus",
                    "resolution_dates": "N/A",
                },
            ]
        )
        exploded = explode_question_set(question_set_df, "2025-01-01")

        # 2. Build question bank
        dfq_fred = make_question_df([{"id": "d1"}])
        dfr_fred = make_resolution_df(
            [
                {"id": "d1", "date": "2025-01-01", "value": 100},
                {"id": "d1", "date": "2025-01-08", "value": 120},
            ]
        )
        dfq_metaculus = make_question_df([{"id": "m1", "resolved": False}])
        dfr_metaculus = make_resolution_df(
            [
                {"id": "m1", "date": "2025-01-01", "value": 0.4},
                {"id": "m1", "date": "2025-01-08", "value": 0.6},
                {"id": "m1", "date": "2025-02-28", "value": 0.9},
            ]
        )
        question_bank = {
            "fred": SourceQuestionBank(dfq=dfq_fred, dfr=dfr_fred),
            "metaculus": SourceQuestionBank(dfq=dfq_metaculus, dfr=dfr_metaculus),
        }

        # 3. Resolve
        resolved, _ = resolve_all(
            exploded,
            question_bank=question_bank,
            sources={"fred": SOURCES["fred"], "metaculus": SOURCES["metaculus"]},
            forecast_due_date=date(2025, 1, 1),
        )

        assert len(resolved) > 0

        # 4. Build forecast file and merge
        forecast_df = pd.DataFrame(
            {
                "id": ["d1", "m1"],
                "source": ["fred", "metaculus"],
                "direction": [(), ()],
                "forecast": [np.nan, 0.65],  # d1 missing, m1 present
                "resolution_date": ["2025-01-08", "2025-01-08"],
            }
        )
        prepared = check_and_prepare_forecast_file(forecast_df, "2025-01-01", "test_org")
        merged = set_resolution_dates(prepared, resolved)

        # 5. Impute
        result = impute_missing_forecasts(merged, "test_org", "test_model_org", "test_model")

        # Verify imputation happened
        d1_rows = result[result["id"] == "d1"]
        if len(d1_rows) > 0:
            assert d1_rows.iloc[0]["forecast"] == 0.5  # default imputation
            assert bool(d1_rows.iloc[0]["imputed"]) is True

        m1_rows = result[result["id"] == "m1"]
        if len(m1_rows) > 0:
            assert m1_rows.iloc[0]["forecast"] == 0.65  # original forecast preserved
            assert bool(m1_rows.iloc[0]["imputed"]) is False
