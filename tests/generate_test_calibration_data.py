"""Generate test calibration data files for local website verification.

Run: python tests/generate_test_calibration_data.py
Then: cd src/www.forecastbench.org && bundle exec jekyll serve

This writes to src/www.forecastbench.org/assets/data/ so the calibration page can render.
The generated data uses synthetic forecasts with known calibration properties.
"""

import json
import os
import sys

import numpy as np
import pandas as pd

ASSET_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "src",
    "www.forecastbench.org",
    "assets",
    "data",
)

MODELS = [
    ("well_calibrated", "Synthetic", "Well-Calibrated Model"),
    ("overconfident", "Synthetic", "Overconfident Model"),
    ("underconfident", "Synthetic", "Underconfident Model"),
    ("sharp", "Synthetic", "Sharp Model"),
]

N_BINS = 10


def generate_forecasts(model_type, rng, n=2000):
    """Generate synthetic forecasts with known calibration properties."""
    base = rng.uniform(0.05, 0.95, n)

    if model_type == "well_calibrated":
        forecasts = base
    elif model_type == "overconfident":
        # Push toward extremes
        forecasts = np.where(base > 0.5, base + (1 - base) * 0.4, base * 0.6)
    elif model_type == "underconfident":
        # Push toward 0.5
        forecasts = 0.5 + (base - 0.5) * 0.5
    elif model_type == "sharp":
        # Mostly extreme probabilities
        forecasts = np.where(base > 0.5, 0.85 + rng.uniform(0, 0.14, n), 0.01 + rng.uniform(0, 0.14, n))
    else:
        forecasts = base

    forecasts = np.clip(forecasts, 0.01, 0.99)
    # Outcomes follow true base rate (well-calibrated ground truth)
    outcomes = (rng.uniform(0, 1, n) < base).astype(float)
    return forecasts, outcomes


def compute_metrics_and_curves(all_data):
    """Compute calibration metrics and curve data from synthetic forecasts."""
    bin_edges = np.linspace(0, 1, N_BINS + 1)
    bin_midpoints = (bin_edges[:-1] + bin_edges[1:]) / 2

    metrics_rows = []
    curve_rows = []

    overall_base_rate = np.concatenate([d["outcomes"] for d in all_data]).mean()
    overall_uncertainty = overall_base_rate * (1 - overall_base_rate)

    for d in all_data:
        forecasts = d["forecasts"]
        outcomes = d["outcomes"]
        bins = np.digitize(forecasts, bin_edges, right=True).clip(1, N_BINS)
        n_total = len(forecasts)

        ece = 0.0
        reliability = 0.0
        resolution = 0.0

        for b in range(1, N_BINS + 1):
            mask = bins == b
            if not mask.any():
                continue
            n_k = mask.sum()
            weight = n_k / n_total
            f_mean = forecasts[mask].mean()
            o_rate = outcomes[mask].mean()
            ece += weight * abs(f_mean - o_rate)
            reliability += weight * (f_mean - o_rate) ** 2
            resolution += weight * (o_rate - overall_base_rate) ** 2

            curve_rows.append({
                "model_pk": d["model_pk"],
                "organization": d["organization"],
                "model": d["model"],
                "bin_midpoint": round(float(bin_midpoints[b - 1]), 3),
                "forecast_mean": round(float(f_mean), 4),
                "resolution_rate": round(float(o_rate), 4),
                "n_bin": int(n_k),
            })

        metrics_rows.append({
            "model_pk": d["model_pk"],
            "organization": d["organization"],
            "model_organization": d["organization"],
            "model": d["model"],
            "ece": round(ece, 6),
            "reliability": round(reliability, 6),
            "resolution": round(resolution, 6),
            "uncertainty": round(overall_uncertainty, 6),
            "sharpness": round(float(forecasts.std()), 6),
            "n_forecasts": n_total,
        })

    return pd.DataFrame(metrics_rows), curve_rows


def main():
    rng = np.random.default_rng(42)
    all_data = []

    for model_pk, org, model_name in MODELS:
        forecasts, outcomes = generate_forecasts(model_pk, rng)
        all_data.append({
            "model_pk": model_pk,
            "organization": org,
            "model": model_name,
            "forecasts": forecasts,
            "outcomes": outcomes,
        })

    df_metrics, curve_rows = compute_metrics_and_curves(all_data)

    os.makedirs(ASSET_DIR, exist_ok=True)

    for lb_type in ["baseline", "tournament"]:
        df_metrics.to_csv(
            os.path.join(ASSET_DIR, f"calibration_metrics_{lb_type}.csv"),
            index=False,
        )
        with open(os.path.join(ASSET_DIR, f"calibration_curves_{lb_type}.json"), "w") as f:
            json.dump(curve_rows, f, indent=2)

    print(f"Wrote calibration data to {os.path.abspath(ASSET_DIR)}")
    print(f"  Metrics: {len(df_metrics)} models")
    print(f"  Curves: {len(curve_rows)} bins")
    print()
    print("Verify decomposition identity (reliability - resolution + uncertainty ≈ mean Brier):")
    for d, row in zip(all_data, df_metrics.itertuples()):
        brier = ((d["forecasts"] - d["outcomes"]) ** 2).mean()
        decomp = row.reliability - row.resolution + row.uncertainty
        print(f"  {row.model:25s}  decomp={decomp:.4f}  brier={brier:.4f}  diff={abs(decomp-brier):.6f}")


if __name__ == "__main__":
    main()
