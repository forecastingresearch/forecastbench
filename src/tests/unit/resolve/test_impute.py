"""Tests for resolve/_impute.py: missing forecast imputation."""

import numpy as np
import pandas as pd

from resolve._impute import impute_missing_forecasts


class TestImputeMissingForecasts:
    """Test forecast imputation logic."""

    def _make_df(self, forecasts, sources=None, market_values=None, naive_values=None):
        """Build a DataFrame for imputation testing."""
        n = len(forecasts)
        return pd.DataFrame(
            {
                "id": [f"q{i}" for i in range(n)],
                "source": sources or ["metaculus"] * n,
                "forecast": forecasts,
                "market_value_on_due_date": market_values or [0.6] * n,
                "market_value_on_due_date_minus_one": naive_values or [0.55] * n,
            }
        )

    def test_no_nan_is_noop(self):
        df = self._make_df([0.3, 0.7])
        result = impute_missing_forecasts(df, "org", "model_org", "model")
        assert (result["imputed"] == False).all()  # noqa: E712
        assert list(result["forecast"]) == [0.3, 0.7]

    def test_default_imputation_is_0_5(self):
        df = self._make_df([0.3, np.nan])
        result = impute_missing_forecasts(df, "org", "model_org", "model")
        assert result.iloc[1]["forecast"] == 0.5
        assert bool(result.iloc[1]["imputed"]) is True
        assert bool(result.iloc[0]["imputed"]) is False

    def test_imputed_forecaster_uses_market_value_on_due_date(self):
        df = self._make_df(
            [np.nan],
            sources=["metaculus"],
            market_values=[0.8],
        )
        result = impute_missing_forecasts(
            df, "ForecastBench", "ForecastBench", "Imputed Forecaster"
        )
        assert result.iloc[0]["forecast"] == 0.8

    def test_naive_forecaster_uses_market_value_minus_one(self):
        df = self._make_df(
            [np.nan],
            sources=["metaculus"],
            naive_values=[0.45],
        )
        result = impute_missing_forecasts(df, "ForecastBench", "ForecastBench", "Naive Forecaster")
        assert result.iloc[0]["forecast"] == 0.45

    def test_non_benchmark_org_always_uses_default(self):
        """Non-benchmark organizations always impute to 0.5."""
        df = self._make_df(
            [np.nan],
            sources=["metaculus"],
            market_values=[0.8],
        )
        result = impute_missing_forecasts(df, "some_org", "some_model_org", "Imputed Forecaster")
        assert result.iloc[0]["forecast"] == 0.5

    def test_dataset_source_always_uses_default_for_benchmark(self):
        """Dataset sources always get 0.5 even for benchmark models."""
        df = self._make_df(
            [np.nan],
            sources=["fred"],
            market_values=[0.8],
        )
        result = impute_missing_forecasts(
            df, "ForecastBench", "ForecastBench", "Imputed Forecaster"
        )
        # fred is a dataset source, not in MARKET_SOURCE_NAMES → default 0.5
        assert result.iloc[0]["forecast"] == 0.5
