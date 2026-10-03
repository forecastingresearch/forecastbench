"""Unit tests for the market stratification engine (the most bug-prone curate logic).

The e2e fixtures collapse every market into a *single* composite bin, so cross-bin allocation,
``min(target, available)`` capping, the rounding-adjustment loop, and per-bin decorrelation are
never exercised end-to-end. These tests build a genuinely multi-bin ``dfq`` (>=2 market-value bins
x >=2 time-horizon bins) and pin the engine's *semantics* (counts / weights / capping /
reproducibility) rather than baking selected ids into a golden.

Time-horizon bins are reachable only relative to ``question_curation.FORECAST_DATETIME`` (which is
derived from ``FREEZE_DATETIME`` at import), so close datetimes here are always computed as an
offset from that anchor instead of hardcoded — keeping the tests date-independent.
"""

from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from curate_questions.create_question_set.main import (
    add_bin_columns,
    calculate_bin_weights,
    create_composite_bins,
    stratified_sample_questions,
)
from helpers import question_curation

# Market-value probabilities that each land in a distinct, defined MV bin (both in the 0.125-weight
# tier, so two MV bins under the same TH share an equal composite weight — used for decorrelation).
_MV_A = 0.35  # -> "0.3-0.4%"
_MV_B = 0.45  # -> "0.4-0.5%"

# Day offsets from FORECAST_DATETIME that land in distinct TH bins of *different* weight.
_TH_SHORT_DAYS = 15  # -> "8-25d"   (weight 0.20)
_TH_LONG_DAYS = 40  # -> "26-50d"  (weight 0.35, the higher-weight horizon)


def _close_iso(days_until_close: int) -> str:
    """Return an ISO close datetime that lands ``days_until_close`` after the forecast due date."""
    return (question_curation.FORECAST_DATETIME + timedelta(days=days_until_close)).isoformat()


def _make_market_dfq(supply: dict[tuple[float, int], int]) -> pd.DataFrame:
    """Build a binned+weighted market ``dfq`` from a ``{(market_value, days): count}`` supply map.

    Runs the real ``add_bin_columns`` -> ``create_composite_bins`` -> ``calculate_bin_weights``
    pipeline so the returned frame carries ``market_value_bin``/``time_horizon_bin``/
    ``composite_bin``/``bin_weight`` exactly as ``stratified_sample_questions`` expects. A
    monotonically increasing ``local_k`` per bin gives a stable within-bin layout for the
    decorrelation tests.
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
    dfq = add_bin_columns(dfq)
    dfq = create_composite_bins(dfq)
    dfq = calculate_bin_weights(dfq)
    return dfq


def _bin_counts(sampled: pd.DataFrame) -> dict[str, int]:
    """Re-derive composite-bin counts on a sampled frame (the engine drops nothing internally)."""
    binned = create_composite_bins(add_bin_columns(sampled))
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
        # Same MV bin across two horizons: 26-50d (0.35) must out-weigh 8-25d (0.20).
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
    """Cross-bin allocation, capping, and the rounding-adjustment loop."""

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

    def test_per_bin_counts_track_bin_weight_within_rounding(self):
        # All four bins amply supplied: each bin's count must equal round(n_target * weight)
        # (no capping in play), and the per-bin shares stay within +/-1 of the weighted target.
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_SHORT_DAYS): 100,
                (_MV_B, _TH_SHORT_DAYS): 100,
                (_MV_A, _TH_LONG_DAYS): 100,
                (_MV_B, _TH_LONG_DAYS): 100,
            }
        )
        n_target = 40
        out = stratified_sample_questions(dfq, n_target, random_state=np.random.RandomState(0))
        counts = _bin_counts(out)
        weights = (
            dfq[["composite_bin", "bin_weight"]]
            .drop_duplicates()
            .set_index("composite_bin")["bin_weight"]
        )
        assert sum(counts.values()) == n_target
        for bin_name, weight in weights.items():
            expected = round(n_target * weight)
            assert abs(counts.get(bin_name, 0) - expected) <= 1

    def test_undersupplied_high_weight_bin_caps_and_reallocates(self):
        # The two high-weight (26-50d) bins are SCARCE; the two low-weight (8-25d) bins are plentiful.
        # Capping pins each scarce bin at its supply, and the shortage is refilled from the
        # remaining bins (highest weight first, so it may all land in one bin) until the total
        # hits n_target — never exceeding any bin's availability.
        supply = {
            (_MV_A, _TH_SHORT_DAYS): 50,
            (_MV_B, _TH_SHORT_DAYS): 50,
            (_MV_A, _TH_LONG_DAYS): 2,
            (_MV_B, _TH_LONG_DAYS): 2,
        }
        dfq = _make_market_dfq(supply)
        n_target = 40
        out = stratified_sample_questions(dfq, n_target, random_state=np.random.RandomState(0))
        counts = _bin_counts(out)

        assert len(out) == n_target  # shortage fully reallocated
        # Scarce high-weight bins are capped exactly at their (small) supply.
        assert counts.get("0.3-0.4%_26-50d", 0) == 2
        assert counts.get("0.4-0.5%_26-50d", 0) == 2
        # No bin is over-drawn beyond what it can supply.
        avail = {
            "0.3-0.4%_8-25d": 50,
            "0.4-0.5%_8-25d": 50,
            "0.3-0.4%_26-50d": 2,
            "0.4-0.5%_26-50d": 2,
        }
        for bin_name, got in counts.items():
            assert got <= avail[bin_name]

    def test_target_exceeding_total_supply_returns_all_available(self):
        # When n_target outstrips every bin's supply, capping bounds each draw at availability;
        # the result is the whole frame (engine never samples with replacement).
        supply = {
            (_MV_A, _TH_SHORT_DAYS): 3,
            (_MV_B, _TH_LONG_DAYS): 4,
        }
        dfq = _make_market_dfq(supply)
        out = stratified_sample_questions(dfq, 100, random_state=np.random.RandomState(0))
        assert len(out) == 7
        assert set(out["id"]) == set(dfq["id"])


class TestReproducibilityAndDecorrelation:
    """The int-seed -> ``_as_random_state`` promotion is reproducible *and* decorrelates per bin."""

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
        # Not a hard guarantee, but across 400 -> 40 the odds two seeds pick the same id-set are
        # negligible; this pins that the seed actually drives selection.
        dfq = self._ample_four_bin_dfq()
        a = stratified_sample_questions(dfq, 40, random_state=0)
        c = stratified_sample_questions(dfq, 40, random_state=7)
        assert sorted(a["id"]) != sorted(c["id"])

    def test_threaded_state_decorrelates_equal_weight_bins(self):
        # Two equal-weight bins with an IDENTICAL within-bin layout (local_k 0..N-1). Threading one
        # RandomState advances the generator between the two per-bin .sample() calls, so the picked
        # local positions DIFFER across bins. Reusing RandomState(seed) fresh per bin (the bug this
        # guards against) would pick the SAME positions in both.
        dfq = _make_market_dfq(
            {
                (_MV_A, _TH_LONG_DAYS): 100,
                (_MV_B, _TH_LONG_DAYS): 100,
            }
        )
        n_target = 20
        out = stratified_sample_questions(dfq, n_target, random_state=0)  # int -> threaded
        binned = create_composite_bins(add_bin_columns(out))
        picks = {
            name: tuple(sorted(grp["local_k"])) for name, grp in binned.groupby("composite_bin")
        }
        bins = list(picks)
        assert len(bins) == 2
        assert len(picks[bins[0]]) == len(picks[bins[1]])  # equal weight -> equal n
        # Threaded RandomState: decorrelated -> the two bins pick different local positions.
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
