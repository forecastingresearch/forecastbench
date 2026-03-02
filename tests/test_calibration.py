"""Tests for calibration metrics: ECE, Brier decomposition, sharpness.

These tests duplicate the calibration logic from src/leaderboard/main.py to avoid importing
the full main module (which pulls in pyfixest, joblib, etc.). The functions under test are
compute_calibration_metrics() and compute_calibration_curve_data().
"""

import numpy as np
import pandas as pd

# ── Inline copies of the functions under test ──────────────────────────────────
# Keep in sync with src/leaderboard/main.py


def compute_calibration_metrics(df, n_bins=10):
    """Compute calibration metrics per model."""
    bin_edges = np.linspace(0, 1, n_bins + 1)
    df = df.copy()
    df["bin"] = np.digitize(df["forecast"], bin_edges, right=True).clip(1, n_bins)

    overall_base_rate = df["resolved_to"].mean()
    overall_uncertainty = overall_base_rate * (1 - overall_base_rate)

    rows = []
    for (model_pk, org, model_org, model), grp in df.groupby(
        ["model_pk", "organization", "model_organization", "model"]
    ):
        n_total = len(grp)
        ece = 0.0
        reliability = 0.0
        resolution = 0.0

        for _, bin_grp in grp.groupby("bin"):
            n_k = len(bin_grp)
            weight = n_k / n_total
            forecast_mean = bin_grp["forecast"].mean()
            observed_rate = bin_grp["resolved_to"].mean()
            ece += weight * abs(forecast_mean - observed_rate)
            reliability += weight * (forecast_mean - observed_rate) ** 2
            resolution += weight * (observed_rate - overall_base_rate) ** 2

        sharpness = grp["forecast"].std()

        rows.append(
            {
                "model_pk": model_pk,
                "organization": org,
                "model_organization": model_org,
                "model": model,
                "ece": round(ece, 6),
                "reliability": round(reliability, 6),
                "resolution": round(resolution, 6),
                "uncertainty": round(overall_uncertainty, 6),
                "sharpness": round(sharpness, 6),
                "n_forecasts": n_total,
            }
        )

    return pd.DataFrame(rows)


def compute_calibration_curve_data(df, n_bins=10):
    """Compute per-bin calibration curve data."""
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bin_midpoints = (bin_edges[:-1] + bin_edges[1:]) / 2
    df = df.copy()
    df["bin"] = np.digitize(df["forecast"], bin_edges, right=True).clip(1, n_bins)

    rows = []
    for (model_pk, org, model), grp in df.groupby(["model_pk", "organization", "model"]):
        for bin_idx, bin_grp in grp.groupby("bin"):
            rows.append(
                {
                    "model_pk": model_pk,
                    "organization": org,
                    "model": model,
                    "bin_midpoint": round(bin_midpoints[bin_idx - 1], 3),
                    "forecast_mean": round(bin_grp["forecast"].mean(), 4),
                    "resolution_rate": round(bin_grp["resolved_to"].mean(), 4),
                    "n_bin": len(bin_grp),
                }
            )

    return pd.DataFrame(rows)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_forecasts(
    forecasts, outcomes, model_pk="test_model", organization="TestOrg", model="test"
):
    """Build a minimal DataFrame matching the expected schema."""
    return pd.DataFrame(
        {
            "forecast": forecasts,
            "resolved_to": outcomes,
            "brier_score": [(f - o) ** 2 for f, o in zip(forecasts, outcomes)],
            "model_pk": model_pk,
            "organization": organization,
            "model_organization": organization,
            "model": model,
        }
    )


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestBrierDecomposition:
    """Verify reliability - resolution + uncertainty ≈ mean(brier_score)."""

    def test_decomposition_identity_synthetic(self):
        """With known data, the Brier decomposition identity should hold."""
        rng = np.random.default_rng(42)
        n = 2000
        forecasts = rng.uniform(0, 1, n)
        outcomes = (rng.uniform(0, 1, n) < forecasts).astype(float)

        df = _make_forecasts(forecasts.tolist(), outcomes.tolist())
        metrics = compute_calibration_metrics(df, n_bins=10)

        assert len(metrics) == 1
        row = metrics.iloc[0]

        decomp = row["reliability"] - row["resolution"] + row["uncertainty"]
        mean_brier = df["brier_score"].mean()

        assert abs(decomp - mean_brier) < 0.01, (
            f"Brier decomposition failed: {row['reliability']:.4f} - {row['resolution']:.4f} "
            f"+ {row['uncertainty']:.4f} = {decomp:.4f} vs mean_brier = {mean_brier:.4f}"
        )

    def test_perfect_calibration_has_zero_ece(self):
        """A perfectly calibrated forecaster should have ECE ≈ 0."""
        rng = np.random.default_rng(123)
        forecasts = []
        outcomes = []
        for p in [0.1, 0.3, 0.5, 0.7, 0.9]:
            n_per_bin = 500
            forecasts.extend([p] * n_per_bin)
            outcomes.extend(rng.binomial(1, p, n_per_bin).tolist())

        df = _make_forecasts(forecasts, outcomes)
        metrics = compute_calibration_metrics(df, n_bins=10)

        row = metrics.iloc[0]
        assert row["ece"] < 0.03, f"ECE too high for well-calibrated forecaster: {row['ece']:.4f}"

    def test_overconfident_forecaster_has_high_ece(self):
        """A forecaster who always says 0.95 when base rate is 0.5 should have high ECE."""
        rng = np.random.default_rng(456)
        n = 1000
        forecasts = [0.95] * n
        outcomes = rng.binomial(1, 0.5, n).astype(float).tolist()

        df = _make_forecasts(forecasts, outcomes)
        metrics = compute_calibration_metrics(df, n_bins=10)

        row = metrics.iloc[0]
        assert (
            row["ece"] > 0.3
        ), f"ECE should be high for overconfident forecaster: {row['ece']:.4f}"

    def test_multiple_models(self):
        """Metrics should return one row per model."""
        rng = np.random.default_rng(789)
        dfs = []
        for i in range(3):
            n = 500
            forecasts = rng.uniform(0, 1, n)
            outcomes = (rng.uniform(0, 1, n) < forecasts).astype(float)
            dfs.append(
                _make_forecasts(
                    forecasts.tolist(),
                    outcomes.tolist(),
                    model_pk=f"model_{i}",
                    model=f"model_{i}",
                )
            )

        df = pd.concat(dfs, ignore_index=True)
        metrics = compute_calibration_metrics(df, n_bins=10)
        assert len(metrics) == 3


class TestCalibrationCurves:
    """Test compute_calibration_curve_data."""

    def test_curve_data_shape(self):
        """Curve data should have at most n_bins rows per model."""
        rng = np.random.default_rng(101)
        n = 1000
        forecasts = rng.uniform(0, 1, n)
        outcomes = (rng.uniform(0, 1, n) < forecasts).astype(float)

        df = _make_forecasts(forecasts.tolist(), outcomes.tolist())
        curves = compute_calibration_curve_data(df, n_bins=10)

        assert len(curves) <= 10
        assert set(curves.columns) == {
            "model_pk",
            "organization",
            "model",
            "bin_midpoint",
            "forecast_mean",
            "resolution_rate",
            "n_bin",
        }

    def test_bin_counts_sum_to_total(self):
        """Sum of n_bin across bins should equal total forecasts."""
        rng = np.random.default_rng(202)
        n = 800
        forecasts = rng.uniform(0, 1, n)
        outcomes = (rng.uniform(0, 1, n) < forecasts).astype(float)

        df = _make_forecasts(forecasts.tolist(), outcomes.tolist())
        curves = compute_calibration_curve_data(df, n_bins=10)

        assert curves["n_bin"].sum() == n
