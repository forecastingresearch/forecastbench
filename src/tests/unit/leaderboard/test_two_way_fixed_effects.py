"""Unit tests for the production ranking path: 2FE difficulty-adjusted Brier and the bootstrap.

``pyfixest`` is degenerate on tiny input, so ``score_models`` and
``generate_simulated_leaderboards`` run on a 225-dataset / 50-market fixture with the baselines,
and the results are golden-pinned.
"""

import pandas as pd
import pytest

from leaderboard.main import (
    MarketQuestionAdjustment,
    brier_skill_score,
    combine_forecasting_rounds,
    generate_simulated_leaderboards,
    peer_score,
    score_models,
    two_way_fixed_effects,
)
from tests._golden import check_golden
from tests.factories import make_leaderboard_entries

_SCORERS = [two_way_fixed_effects, peer_score, brier_skill_score]
_ADJ = MarketQuestionAdjustment.MARKET_BRIER
_GOLDEN_COLS = [
    "model_pk",
    "two_way_fixed_effects_dataset",
    "two_way_fixed_effects_market",
    "two_way_fixed_effects_overall",
    "peer_score_overall",
    "brier_skill_score_overall",
    "brier_index_overall",
    "n_dataset",
    "n_market",
    "n_overall",
]


@pytest.fixture(scope="module")
def scored():
    # Module-scoped: score_models (the pyfixest 2FE fit) is the expensive step; the tests only read.
    combined = combine_forecasting_rounds([make_leaderboard_entries()])
    return score_models(combined, _SCORERS, _ADJ)


def test_score_models_runs_2fe_without_rank_deficiency(scored):
    lb, qfe = scored
    assert set(lb["model"]) == {
        "Good Model",
        "Bad Model",
        "Naive Forecaster",
        "Imputed Forecaster",
        "Always 0.5",
    }
    assert {"two_way_fixed_effects_overall", "brier_index_overall"} <= set(lb.columns)
    assert (lb["n_dataset"] == 225).all() and (lb["n_market"] == 50).all()
    assert set(qfe) == {"dataset", "market"}


def test_better_brier_ranks_higher(scored):
    lb, _ = scored
    by_model = lb.set_index("model")
    # Lower difficulty-adjusted Brier is better; the Brier Index (higher=better) must agree.
    assert (
        by_model.loc["Good Model", "two_way_fixed_effects_overall"]
        < by_model.loc["Bad Model", "two_way_fixed_effects_overall"]
    )
    assert (
        by_model.loc["Good Model", "brier_index_overall"]
        > by_model.loc["Bad Model", "brier_index_overall"]
    )
    # Always 0.5 is the rescale anchor: its difficulty-adjusted overall sits at 0.25 by construction.
    assert by_model.loc["Always 0.5", "two_way_fixed_effects_overall"] == pytest.approx(
        0.25, abs=1e-6
    )


def test_simulated_leaderboards_are_deterministic_with_seed(monkeypatch):
    monkeypatch.setenv("NUM_CPUS", "1")  # joblib runs sequentially in-process
    monkeypatch.setenv("RANDOM_SEED", "0")  # env.RANDOM_SEED seeds every bootstrap replicate
    combined = combine_forecasting_rounds([make_leaderboard_entries()])
    a = generate_simulated_leaderboards(combined, two_way_fixed_effects, _ADJ, N=2)
    b = generate_simulated_leaderboards(combined, two_way_fixed_effects, _ADJ, N=2)
    for left, right in zip(a, b):
        pd.testing.assert_frame_equal(left, right)


def test_scored_leaderboard_golden(scored):
    lb, _ = scored
    check_golden("leaderboard_2fe", lb, key="model_pk", cols=_GOLDEN_COLS)


def test_bootstrap_overall_scores_golden(monkeypatch):
    # Pin the actual bootstrap replicate draws (not just run-to-run equality): seeded + sequential.
    monkeypatch.setenv("NUM_CPUS", "1")
    monkeypatch.setenv("RANDOM_SEED", "0")
    combined = combine_forecasting_rounds([make_leaderboard_entries()])
    _, _, overall = generate_simulated_leaderboards(combined, two_way_fixed_effects, _ADJ, N=2)
    check_golden("leaderboard_2fe_bootstrap", overall.reset_index(), key="model_pk")
