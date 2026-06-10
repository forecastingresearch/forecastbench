"""Tests for helpers/question_curation.py: forecast round date arithmetic."""

from datetime import date

from helpers import question_curation


class TestGetNextForecastDueDate:
    """Test the 14-day round cycle anchored at the tournament start date."""

    def test_round_date_returns_itself(self):
        assert question_curation.get_next_forecast_due_date(date(2025, 3, 2)) == "2025-03-02"

    def test_day_after_round_returns_next_round(self):
        assert question_curation.get_next_forecast_due_date(date(2025, 3, 3)) == "2025-03-16"

    def test_tournament_start_is_a_round_date(self):
        assert question_curation.get_next_forecast_due_date(date(2024, 7, 21)) == "2024-07-21"

    def test_day_before_round_returns_next_day(self):
        assert question_curation.get_next_forecast_due_date(date(2025, 3, 1)) == "2025-03-02"
