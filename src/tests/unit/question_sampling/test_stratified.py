"""Unit tests for the market stratification engine on a multi-bin ``dfq``.

The e2e fixtures put every market in one composite bin, so cross-bin allocation, capping and
per-bin decorrelation are pinned here. Close datetimes are offsets from ``FORECAST_DATETIME``, the
due date the binning takes.
"""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from question_sampling._binning import (
    add_bin_columns,
    calculate_bin_weights,
    create_composite_bins,
)
from question_sampling._sampling import stratified_sample_questions

FORECAST_DATETIME = datetime(2026, 10, 13, tzinfo=timezone.utc)

# Market-value probabilities that each land in a distinct, defined MV bin (both in the 0.125-weight
# tier, so two MV bins under the same TH share an equal composite weight — used for decorrelation).
_MV_A = 0.35  # "0.3-0.4%"
_MV_B = 0.45  # "0.4-0.5%"

# Day offsets from FORECAST_DATETIME that land in TH bins of different weight.
_TH_SHORT_DAYS = 15  # "8-25d", weight 0.20
_TH_LONG_DAYS = 40  # "26-50d", weight 0.35


def _close_iso(days_until_close: int) -> str:
    """Return an ISO close datetime that lands ``days_until_close`` after the forecast due date."""
    return (FORECAST_DATETIME + timedelta(days=days_until_close)).isoformat()


def _make_market_dfq(supply: dict[tuple[float, int], int]) -> pd.DataFrame:
    """Build a binned and weighted market ``dfq`` from a ``{(market_value, days): count}`` map.

    Runs the real ``add_bin_columns``, ``create_composite_bins`` and ``calculate_bin_weights``.
    ``local_k`` numbers the rows within each bin for the decorrelation test.
    """
    rows = []
    idx = 0
    for (market_value, days), count in supply.items():
        for k in range(count):
            rows.append(
                {
                    "id": f"q{idx}",
                    "source": "manifold",
                    "freeze_datetime_value": str(market_value),
                    "market_info_close_datetime": _close_iso(days),
                    "local_k": k,
                }
            )
            idx += 1
    dfq = pd.DataFrame(rows)
    dfq = add_bin_columns(dfq, FORECAST_DATETIME)
    dfq = create_composite_bins(dfq)
    dfq = calculate_bin_weights(dfq)
    return dfq


def _bin_counts(sampled: pd.DataFrame) -> dict[str, int]:
    """Re-derive composite-bin counts on a sampled frame (the engine drops nothing internally)."""
    binned = create_composite_bins(add_bin_columns(sampled, FORECAST_DATETIME))
    return binned["composite_bin"].value_counts().to_dict()


class TestCreateCompositeBins:
    """``create_composite_bins`` joins the two dimension labels into ``"<mv>_<th>"``."""

    def test_representative_rows_land_in_expected_bins_end_to_end(self):
        # Drive the real binning from raw market value + close datetime to pin reachability.
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 1,
                (_MV_B, _TH_LONG_DAYS): 1,
            }
        )
        assert set(dfq["composite_bin"]) == {"0.3-0.4%_8-25d", "0.4-0.5%_26-50d"}


class TestCalculateBinWeights:
    """``calculate_bin_weights`` multiplies the two dimension weights, then normalizes to sum 1."""

    def test_weights_sum_to_one_over_populated_bins(self):
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 5,
                (_MV_B, _TH_SHORT_DAYS): 5,
                (_MV_A, _TH_LONG_DAYS): 5,
                (_MV_B, _TH_LONG_DAYS): 5,
            }
        )
        per_bin = dfq[["composite_bin", "bin_weight"]].drop_duplicates()
        assert len(per_bin) == 4  # 2 MV bins x 2 TH bins, all reachable
        assert per_bin["bin_weight"].sum() == pytest.approx(1.0)

    def test_equal_weight_market_value_bins_under_same_horizon(self):
        # _MV_A and _MV_B share the 0.125 MV-weight tier, so under one TH they split 50/50.
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_LONG_DAYS): 10,
                (_MV_B, _TH_LONG_DAYS): 10,
            }
        )
        weights = (
            dfq[["composite_bin", "bin_weight"]]
            .drop_duplicates()
            .set_index("composite_bin")["bin_weight"]
        )
        assert weights["0.3-0.4%_26-50d"] == pytest.approx(0.5)
        assert weights["0.4-0.5%_26-50d"] == pytest.approx(0.5)

    def test_higher_weight_horizon_gets_more_weight(self):
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 10,
                (_MV_A, _TH_LONG_DAYS): 10,
            }
        )
        weights = (
            dfq[["composite_bin", "bin_weight"]]
            .drop_duplicates()
            .set_index("composite_bin")["bin_weight"]
        )
        assert weights["0.3-0.4%_26-50d"] > weights["0.3-0.4%_8-25d"]


class TestStratifiedSampleQuestions:
    """Cross-bin totals under ample and short supply.

    Per-bin apportionment (largest remainders, small bins sharing the leftover) and a bin that runs
    out handing its slots to the others are pinned in ``test_bin_quota_apportionment.py``.
    """

    def test_total_equals_target_when_supply_is_ample(self):
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 50,
                (_MV_B, _TH_SHORT_DAYS): 50,
                (_MV_A, _TH_LONG_DAYS): 50,
                (_MV_B, _TH_LONG_DAYS): 50,
            }
        )
        n_target = 40
        out = stratified_sample_questions(dfq, n_target, random_state=np.random.RandomState(0))
        assert len(out) == n_target

    def test_target_exceeding_total_supply_returns_all_available(self):
        supply = {
            (_MV_A, _TH_SHORT_DAYS): 3,
            (_MV_B, _TH_LONG_DAYS): 4,
        }
        dfq = _make_market_dfq(supply)
        out = stratified_sample_questions(dfq, 100, random_state=np.random.RandomState(0))
        assert len(out) == 7
        assert set(out["id"]) == set(dfq["id"])


class TestReproducibilityAndDecorrelation:
    """An int seed is reproducible and, threaded through the bins, decorrelates them."""

    @staticmethod
    def _ample_four_bin_dfq() -> pd.DataFrame:
        return _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 100,
                (_MV_B, _TH_SHORT_DAYS): 100,
                (_MV_A, _TH_LONG_DAYS): 100,
                (_MV_B, _TH_LONG_DAYS): 100,
            }
        )

    def test_same_int_seed_is_reproducible(self):
        dfq = self._ample_four_bin_dfq()
        a = stratified_sample_questions(dfq, 40, random_state=0)
        b = stratified_sample_questions(dfq, 40, random_state=0)
        assert sorted(a["id"]) == sorted(b["id"])

    def test_selection_depends_on_seed(self):
        # Drawing 40 of 400, two seeds giving the same id set is vanishingly unlikely.
        dfq = self._ample_four_bin_dfq()
        a = stratified_sample_questions(dfq, 40, random_state=0)
        c = stratified_sample_questions(dfq, 40, random_state=7)
        assert sorted(a["id"]) != sorted(c["id"])

    def test_threaded_state_decorrelates_equal_weight_bins(self):
        # One RandomState threaded through the per-bin draws picks different positions in two
        # identical bins; a fresh RandomState(seed) per bin would pick the same ones.
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_LONG_DAYS): 100,
                (_MV_B, _TH_LONG_DAYS): 100,
            }
        )
        n_target = 20
        out = stratified_sample_questions(dfq, n_target, random_state=0)
        binned = create_composite_bins(add_bin_columns(out, FORECAST_DATETIME))
        picks = {
            name: tuple(sorted(grp["local_k"])) for name, grp in binned.groupby("composite_bin")
        }
        bins = list(picks)
        assert len(bins) == 2
        assert len(picks[bins[0]]) == len(picks[bins[1]])
        assert picks[bins[0]] != picks[bins[1]]


class TestEdgeCases:
    """Empty input and zero target short-circuit to an empty frame without raising."""

    def test_empty_dfq_returns_empty(self):
        out = stratified_sample_questions(pd.DataFrame(), 10)
        assert isinstance(out, pd.DataFrame)
        assert out.empty

    def test_zero_target_returns_empty(self):
        dfq = _make_market_dfq({(_MV_A, _TH_SHORT_DAYS): 5})
        out = stratified_sample_questions(dfq, 0)
        assert isinstance(out, pd.DataFrame)
        assert out.empty
