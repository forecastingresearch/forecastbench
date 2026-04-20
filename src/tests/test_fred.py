"""Tests for FRED-specific nullification rules."""

from datetime import date

import pandas as pd

from helpers import fred as fred_helper
from questions.fred.update_questions.main import update_questions
from sources.fred import NULLIFIED_IDS, NULLIFIED_QUESTIONS, FredSource
from tests.conftest import make_forecast_df, make_question_df, make_resolution_df


class TestFredNullificationDefinition:
    """Verify retired FRED series metadata is declared correctly."""

    def test_currcir_removed_from_fetch_pool(self):
        assert "CURRCIR" in NULLIFIED_IDS
        assert NULLIFIED_QUESTIONS["CURRCIR"] == date(2025, 11, 1)
        assert all(question["id"] != "CURRCIR" for question in fred_helper.fred_questions)


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

        result, _ = source.resolve(df, dfq, dfr, as_of=date(2025, 10, 31))

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

        result, _ = source.resolve(df, dfq, dfr, as_of=date(2025, 11, 1))

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

        result, _ = source.resolve(df, dfq, dfr, as_of=date(2025, 10, 15))

        row = result[result["id"] == "CURRCIR"].iloc[0]
        assert pd.isna(row["resolved_to"])
        assert bool(row["resolved"]) is False


class TestFredFetchAndUpdateRegression:
    """Test that legacy CURRCIR rows are no longer carried forward."""

    def test_update_questions_drops_nullified_currcir_from_dfq(self):
        dfq = make_question_df([{"id": "CURRCIR"}, {"id": "AAA10Y"}])

        result = update_questions(dfq, pd.DataFrame())

        assert "CURRCIR" not in result["id"].values
        assert "AAA10Y" in result["id"].values
