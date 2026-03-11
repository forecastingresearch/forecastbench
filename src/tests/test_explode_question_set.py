"""Tests for resolve/explode_question_set.py."""

from datetime import date

from resolve.explode_question_set import explode_question_set
from tests.conftest import make_question_set_df


class TestExplodeQuestionSet:
    """Test question set expansion by resolution_date x direction."""

    def test_market_sources_get_all_resolution_dates(self, freeze_today):
        freeze_today(date(2025, 3, 1))

        df = make_question_set_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08", "2025-01-31"],
                },
                {
                    "id": "m1",
                    "source": "metaculus",
                    "resolution_dates": "N/A",
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        # Market source should get both dates (all resolution dates from the set)
        market_rows = result[result["source"] == "metaculus"]
        market_dates = set(market_rows["resolution_date"].dt.strftime("%Y-%m-%d"))
        assert market_dates == {"2025-01-08", "2025-01-31"}

    def test_dataset_sources_keep_own_dates(self, freeze_today):
        freeze_today(date(2025, 3, 1))

        df = make_question_set_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
                {
                    "id": "q2",
                    "source": "fred",
                    "resolution_dates": ["2025-01-31"],
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        q1_dates = set(result[result["id"] == "q1"]["resolution_date"].dt.strftime("%Y-%m-%d"))
        q2_dates = set(result[result["id"] == "q2"]["resolution_date"].dt.strftime("%Y-%m-%d"))
        assert q1_dates == {"2025-01-08"}
        assert q2_dates == {"2025-01-31"}

    def test_filters_future_dates(self, freeze_today):
        freeze_today(date(2025, 1, 15))

        df = make_question_set_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08", "2025-01-31"],
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        # Jan 31 is in the future (today=Jan 15), only Jan 8 should remain
        dates = set(result["resolution_date"].dt.strftime("%Y-%m-%d"))
        assert dates == {"2025-01-08"}

    def test_combo_questions_get_direction_permutations(self, freeze_today):
        freeze_today(date(2025, 3, 1))

        df = make_question_set_df(
            [
                {
                    "id": ("q1", "q2"),
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        # 2 sub-questions → 4 direction permutations: (1,1), (1,-1), (-1,1), (-1,-1)
        assert len(result) == 4
        directions = set(result["direction"].apply(tuple))
        assert directions == {(1, 1), (1, -1), (-1, 1), (-1, -1)}

    def test_single_questions_get_empty_direction(self, freeze_today):
        freeze_today(date(2025, 3, 1))

        df = make_question_set_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        assert len(result) == 1
        assert result.iloc[0]["direction"] == ()

    def test_output_columns(self, freeze_today):
        freeze_today(date(2025, 3, 1))

        df = make_question_set_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "resolution_dates": ["2025-01-08"],
                },
            ]
        )
        result = explode_question_set(df, "2025-01-01")

        expected_cols = {"id", "source", "direction", "forecast_due_date", "resolution_date"}
        assert set(result.columns) == expected_cols
