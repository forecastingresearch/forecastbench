"""Tests for resolve/_prepare.py: forecast file validation and resolution date setting."""

import numpy as np
import pandas as pd
import pytest

from resolve._prepare import (
    check_and_prepare_forecast_file,
    convert_and_bound_dates,
    set_resolution_dates,
)

# ---------------------------------------------------------------------------
# convert_and_bound_dates
# ---------------------------------------------------------------------------


class TestConvertAndBoundDates:
    """Test safe date conversion."""

    def test_normal_date(self):
        result = convert_and_bound_dates("2025-01-15")
        assert result == pd.Timestamp("2025-01-15")

    def test_overflow_date_returns_max(self):
        result = convert_and_bound_dates("9999-12-31")
        assert result == pd.Timestamp("2262-04-11")


# ---------------------------------------------------------------------------
# check_and_prepare_forecast_file
# ---------------------------------------------------------------------------


class TestCheckAndPrepareForecastFile:
    """Test forecast file validation pipeline."""

    def _make_valid_df(self, source="metaculus", forecast_due_date="2025-01-01"):
        """Build a minimal valid forecast DataFrame."""
        return pd.DataFrame(
            {
                "id": ["q1"],
                "source": [source],
                "direction": [()],
                "forecast": [0.5],
                "resolution_date": ["2025-01-08"],  # 7 days from forecast_due_date
            }
        )

    def test_drops_invalid_sources(self):
        df = pd.DataFrame(
            {
                "id": ["q1", "q2"],
                "source": ["metaculus", "invalid_source"],
                "direction": [(), ()],
                "forecast": [0.5, 0.5],
                "resolution_date": ["2025-01-08", "2025-01-08"],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert len(result) == 1
        assert result.iloc[0]["source"] == "metaculus"

    def test_drops_nan_forecasts(self):
        df = pd.DataFrame(
            {
                "id": ["q1", "q2"],
                "source": ["metaculus", "metaculus"],
                "direction": [(), ()],
                "forecast": [0.5, np.nan],
                "resolution_date": ["2025-01-08", "2025-01-08"],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert len(result) == 1

    def test_drops_forecasts_outside_range(self):
        df = pd.DataFrame(
            {
                "id": ["q1", "q2", "q3"],
                "source": ["metaculus", "metaculus", "metaculus"],
                "direction": [(), (), ()],
                "forecast": [0.5, -0.1, 1.5],
                "resolution_date": ["2025-01-08", "2025-01-08", "2025-01-08"],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert len(result) == 1

    def test_drops_invalid_dataset_resolution_dates(self):
        """Dataset sources must have resolution dates matching valid horizons."""
        df = pd.DataFrame(
            {
                "id": ["q1", "q2"],
                "source": ["fred", "fred"],
                "direction": [(), ()],
                "forecast": [0.5, 0.5],
                "resolution_date": [
                    "2025-01-08",  # 7 days → valid
                    "2025-01-10",  # 9 days → invalid
                ],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert len(result) == 1

    def test_accepts_any_market_resolution_date(self):
        """Market sources accept any resolution date."""
        df = pd.DataFrame(
            {
                "id": ["q1"],
                "source": ["metaculus"],
                "direction": [()],
                "forecast": [0.5],
                "resolution_date": ["2025-06-15"],  # arbitrary date
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert len(result) == 1

    def test_makes_columns_hashable(self):
        """List values in id/direction become tuples."""
        df = pd.DataFrame(
            {
                "id": [["q1", "q2"]],
                "source": ["metaculus"],
                "direction": [[1, -1]],
                "forecast": [0.5],
                "resolution_date": ["2025-01-08"],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert isinstance(result.iloc[0]["id"], tuple)
        assert isinstance(result.iloc[0]["direction"], tuple)

    def test_rejects_duplicate_dataset_forecasts(self):
        """Duplicate dataset forecasts raise ValueError."""
        df = pd.DataFrame(
            {
                "id": ["q1", "q1"],
                "source": ["fred", "fred"],
                "direction": [(), ()],
                "forecast": [0.5, 0.6],
                "resolution_date": ["2025-01-08", "2025-01-08"],
            }
        )
        with pytest.raises(ValueError, match="Duplicate Rows"):
            check_and_prepare_forecast_file(df, "2025-01-01", "test_org")

    def test_drops_extra_columns(self):
        """Extra columns are dropped."""
        df = pd.DataFrame(
            {
                "id": ["q1"],
                "source": ["metaculus"],
                "direction": [()],
                "forecast": [0.5],
                "resolution_date": ["2025-01-08"],
                "extra_col": ["should_be_dropped"],
                "reasoning": ["also_dropped"],
            }
        )
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert "extra_col" not in result.columns
        assert "reasoning" not in result.columns

    def test_adds_forecast_due_date_column(self):
        df = self._make_valid_df()
        result = check_and_prepare_forecast_file(df, "2025-01-01", "test_org")
        assert "forecast_due_date" in result.columns
        assert result.iloc[0]["forecast_due_date"] == pd.Timestamp("2025-01-01")


# ---------------------------------------------------------------------------
# set_resolution_dates
# ---------------------------------------------------------------------------


class TestSetResolutionDates:
    """Test merging forecast file with resolved question set."""

    def test_market_sources_join_on_resolved_dates(self):
        """Market sources drop their resolution_date and join on the resolved set's dates."""
        df = pd.DataFrame(
            {
                "id": ["q1", "q1"],
                "source": ["metaculus", "metaculus"],
                "direction": [(), ()],
                "forecast": [0.5, 0.5],
                "forecast_due_date": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "resolution_date": pd.to_datetime(["2025-01-08", "2025-01-08"]),  # will be dropped
            }
        )
        df_question_resolutions = pd.DataFrame(
            {
                "id": ["q1", "q1"],
                "source": ["metaculus", "metaculus"],
                "direction": [(), ()],
                "forecast_due_date": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "resolution_date": pd.to_datetime(["2025-01-14", "2025-01-15"]),
                "resolved_to": [0.7, 0.8],
            }
        )

        result = set_resolution_dates(df, df_question_resolutions)
        # Should have the resolved set's dates, not the original
        assert set(result["resolution_date"].dt.strftime("%Y-%m-%d")) == {
            "2025-01-14",
            "2025-01-15",
        }

    def test_dataset_sources_match_on_resolution_date(self):
        """Dataset sources join on resolution_date too."""
        df = pd.DataFrame(
            {
                "id": ["q1"],
                "source": ["fred"],
                "direction": [()],
                "forecast": [0.5],
                "forecast_due_date": pd.to_datetime(["2025-01-01"]),
                "resolution_date": pd.to_datetime(["2025-01-31"]),
            }
        )
        df_question_resolutions = pd.DataFrame(
            {
                "id": ["q1"],
                "source": ["fred"],
                "direction": [()],
                "forecast_due_date": pd.to_datetime(["2025-01-01"]),
                "resolution_date": pd.to_datetime(["2025-01-31"]),
                "resolved_to": [1.0],
            }
        )

        result = set_resolution_dates(df, df_question_resolutions)
        assert len(result) == 1
        assert result.iloc[0]["resolved_to"] == 1.0
