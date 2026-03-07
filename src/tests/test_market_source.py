"""Tests for MarketSource: combo resolution date logic, _resolve, and static helpers."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from sources._market import MarketSource
from tests.conftest import make_forecast_df, make_question_df, make_resolution_df

# ---------------------------------------------------------------------------
# _get_combo_question_resolution_date_helper
# ---------------------------------------------------------------------------


class TestGetComboQuestionResolutionDateHelper:
    """Exhaustive branch coverage for combo question resolution date logic."""

    helper = staticmethod(MarketSource._get_combo_question_resolution_date_helper)

    d0 = date(2025, 1, 10)
    d1 = date(2025, 1, 20)

    def test_both_unresolved_returns_none(self):
        assert (
            self.helper(
                is_resolved0=False,
                is_resolved1=False,
                dir0=1,
                dir1=1,
                resolved_to0=np.nan,
                resolved_to1=np.nan,
                resolution_date0=self.d0,
                resolution_date1=self.d1,
            )
            is None
        )

    def test_both_nan_returns_min(self):
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=np.nan,
            resolved_to1=np.nan,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_zero_nan_one_diff_dir_returns_min(self):
        # resolved_to0 = NaN, one_diff_dir (dir1=1, resolved_to1=0) → min
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=np.nan,
            resolved_to1=0,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_zero_nan_one_same_dir_returns_date0(self):
        # resolved_to0 = NaN, one_same_dir (dir1=1, resolved_to1=1) → resolution_date0
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=np.nan,
            resolved_to1=1,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_one_nan_zero_diff_dir_returns_min(self):
        # resolved_to1 = NaN, zero_diff_dir (dir0=1, resolved_to0=0) → min
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=0,
            resolved_to1=np.nan,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_one_nan_zero_same_dir_returns_date1(self):
        # resolved_to1 = NaN, zero_same_dir (dir0=1, resolved_to0=1) → resolution_date1
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=1,
            resolved_to1=np.nan,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d1

    def test_both_same_dir_returns_max(self):
        # Both same_dir: dir0=1 resolved_to0=1, dir1=1 resolved_to1=1 → max
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=1,
            resolved_to1=1,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d1

    def test_both_diff_dir_returns_min(self):
        # Both diff_dir: dir0=1 resolved_to0=0, dir1=1 resolved_to1=0 → min
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=0,
            resolved_to1=0,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_zero_same_one_diff_returns_date1(self):
        # zero_same_dir + one_diff_dir → resolution_date1
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=1,
            resolved_to1=0,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d1

    def test_one_same_zero_diff_returns_date0(self):
        # one_same_dir + zero_diff_dir → resolution_date0
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=0,
            resolved_to1=1,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_only_zero_resolved_diff_dir(self):
        # Only question 0 resolved, diff_dir → resolution_date0
        result = self.helper(
            is_resolved0=True,
            is_resolved1=False,
            dir0=1,
            dir1=1,
            resolved_to0=0,
            resolved_to1=np.nan,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_only_one_resolved_diff_dir(self):
        # is_resolved0=False, resolved_to0=NaN → enters NaN branch first.
        # one_diff_dir=True (dir1=1, resolved_to1=0) → min(d0, d1) = d0
        result = self.helper(
            is_resolved0=False,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=np.nan,
            resolved_to1=0,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0

    def test_negative_direction_same_dir(self):
        # dir0=-1, resolved_to0=0 is same_dir (-1 and 0)
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=-1,
            dir1=-1,
            resolved_to0=0,
            resolved_to1=0,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d1  # both same_dir → max

    def test_negative_direction_diff_dir(self):
        # dir0=-1, resolved_to0=1 is diff_dir (-1 and 1)
        result = self.helper(
            is_resolved0=True,
            is_resolved1=True,
            dir0=-1,
            dir1=-1,
            resolved_to0=1,
            resolved_to1=1,
            resolution_date0=self.d0,
            resolution_date1=self.d1,
        )
        assert result == self.d0  # both diff_dir → min


# ---------------------------------------------------------------------------
# _get_combo_question_resolution_date (wrapper)
# ---------------------------------------------------------------------------


class TestGetComboQuestionResolutionDate:
    """Test the wrapper that catches ValueError and returns None."""

    def test_returns_none_when_both_unresolved(self):
        result = MarketSource._get_combo_question_resolution_date(
            is_resolved0=False,
            is_resolved1=False,
            dir0=1,
            dir1=1,
            resolved_to0=np.nan,
            resolved_to1=np.nan,
            resolution_date0=date(2025, 1, 10),
            resolution_date1=date(2025, 1, 20),
        )
        assert result is None

    def test_returns_date_when_resolved(self):
        result = MarketSource._get_combo_question_resolution_date(
            is_resolved0=True,
            is_resolved1=True,
            dir0=1,
            dir1=1,
            resolved_to0=1,
            resolved_to1=1,
            resolution_date0=date(2025, 1, 10),
            resolution_date1=date(2025, 1, 20),
        )
        assert result == date(2025, 1, 20)


# ---------------------------------------------------------------------------
# _get_market_resolution_date
# ---------------------------------------------------------------------------


class TestGetMarketResolutionDate:
    """Test resolution date = min(close_date, resolution_date)."""

    def test_close_before_resolution(self):
        row = make_question_df(
            [
                {
                    "id": "q1",
                    "market_info_close_datetime": "2025-01-10T00:00:00Z",
                    "market_info_resolution_datetime": "2025-02-01T00:00:00Z",
                }
            ]
        )
        result = MarketSource._get_market_resolution_date(row)
        assert result == date(2025, 1, 10)

    def test_resolution_before_close(self):
        row = make_question_df(
            [
                {
                    "id": "q1",
                    "market_info_close_datetime": "2025-03-01T00:00:00Z",
                    "market_info_resolution_datetime": "2025-02-01T00:00:00Z",
                }
            ]
        )
        result = MarketSource._get_market_resolution_date(row)
        assert result == date(2025, 2, 1)

    def test_invalid_close_date_uses_resolution(self):
        row = make_question_df(
            [
                {
                    "id": "q1",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "2025-02-01T00:00:00Z",
                }
            ]
        )
        result = MarketSource._get_market_resolution_date(row)
        assert result == date(2025, 2, 1)

    def test_both_invalid_returns_date_max(self):
        row = make_question_df(
            [
                {
                    "id": "q1",
                    "market_info_close_datetime": "N/A",
                    "market_info_resolution_datetime": "N/A",
                }
            ]
        )
        result = MarketSource._get_market_resolution_date(row)
        assert result == date.max


# ---------------------------------------------------------------------------
# MarketSource._resolve
# ---------------------------------------------------------------------------


class TestMarketResolve:
    """Integration-style tests for market resolution. Tests input→output only."""

    def test_single_unresolved_question(self, market_source, freeze_today):
        """Unresolved question resolves to yesterday's market value."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                }
            ]
        )
        dfq = make_question_df([{"id": "q1", "resolved": False}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 0.3},
                {"id": "q1", "date": "2025-01-14", "value": 0.7},
            ]
        )

        result, warnings = market_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "metaculus"]
        assert len(result_source) == 1
        assert result_source.iloc[0]["resolved_to"] == 0.7

    def test_resolved_question_binary_value(self, market_source, freeze_today):
        """Resolved question with binary value uses that value."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                }
            ]
        )
        dfq = make_question_df(
            [
                {
                    "id": "q1",
                    "resolved": True,
                    "market_info_close_datetime": "2025-01-12T00:00:00Z",
                    "market_info_resolution_datetime": "2025-01-12T00:00:00Z",
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 0.3},
                {"id": "q1", "date": "2025-01-14", "value": 0.5},
                {"id": "q1", "date": "2025-01-12", "value": 1},
            ]
        )

        result, warnings = market_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "metaculus"]
        assert result_source.iloc[0]["resolved_to"] == 1

    def test_resolved_non_binary_value_generates_warning(self, market_source, freeze_today):
        """Resolved question with non-binary value → NaN + warning."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                }
            ]
        )
        dfq = make_question_df(
            [
                {
                    "id": "q1",
                    "resolved": True,
                    "market_info_close_datetime": "2025-02-01T00:00:00Z",
                    "market_info_resolution_datetime": "2025-02-01T00:00:00Z",
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 0.3},
                {"id": "q1", "date": "2025-01-14", "value": 0.5},
                {"id": "q1", "date": "2025-02-01", "value": 0.75},
            ]
        )

        result, warnings = market_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "metaculus"]
        assert pd.isna(result_source.iloc[0]["resolved_to"])
        assert len(warnings) > 0

    def test_resolved_before_forecast_due_date_nullifies(self, market_source, freeze_today):
        """Market resolved before forecast_due_date → resolved_to = NaN."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-10",
                    "resolution_date": "2025-01-14",
                }
            ]
        )
        dfq = make_question_df(
            [
                {
                    "id": "q1",
                    "resolved": True,
                    "market_info_close_datetime": "2025-01-05T00:00:00Z",
                    "market_info_resolution_datetime": "2025-01-05T00:00:00Z",
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-05", "value": 1},
                {"id": "q1", "date": "2025-01-10", "value": 1},
                {"id": "q1", "date": "2025-01-14", "value": 1},
            ]
        )

        result, warnings = market_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "metaculus"]
        assert pd.isna(result_source.iloc[0]["resolved_to"])

    def test_combo_question_resolves(self, market_source, freeze_today):
        """Combo question product of direction-adjusted sub-question values."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                },
                {
                    "id": "q2",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                },
                {
                    "id": ("q1", "q2"),
                    "source": "metaculus",
                    "direction": (1, 1),
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                },
            ]
        )
        dfq = make_question_df([{"id": "q1", "resolved": False}, {"id": "q2", "resolved": False}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 0.3},
                {"id": "q1", "date": "2025-01-14", "value": 0.8},
                {"id": "q2", "date": "2025-01-01", "value": 0.4},
                {"id": "q2", "date": "2025-01-14", "value": 0.6},
            ]
        )

        result, warnings = market_source._resolve(df, dfq, dfr)
        combo_row = result[result["id"].apply(lambda x: isinstance(x, tuple))]
        assert len(combo_row) == 1
        # Both dir=1, so resolved_to = 0.8 * 0.6 = 0.48
        assert abs(combo_row.iloc[0]["resolved_to"] - 0.48) < 1e-9

    def test_missing_id_in_dfr_raises(self, market_source, freeze_today):
        """Missing resolution data for a question raises ValueError."""
        freeze_today(date(2025, 1, 15))

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-14",
                }
            ]
        )
        dfq = make_question_df([{"id": "q1", "resolved": False}])
        dfr = make_resolution_df([{"id": "q_other", "date": "2025-01-14", "value": 0.5}])

        with pytest.raises(ValueError, match="Missing resolution values"):
            market_source._resolve(df, dfq, dfr)
