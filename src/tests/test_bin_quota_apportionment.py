"""Market sampling apportions bin quotas by largest remainder, so small bins are not rounded away."""

import numpy as np
import pandas as pd

from curate_questions.create_question_set import main as create_question_set


def _pool(bin_weights: dict[str, float], available: dict[str, int] | int) -> pd.DataFrame:
    """A pool with one composite bin per key, holding `available` rows each."""
    rows = []
    for name, weight in bin_weights.items():
        n = available if isinstance(available, int) else available[name]
        rows += [
            {"id": f"{name}-{i}", "composite_bin": name, "bin_weight": weight} for i in range(n)
        ]
    return pd.DataFrame(rows)


def test_small_bins_share_the_leftover_slots():
    """Ten bins at 0.04 each have a target of 0.4 at n=10; rounding each on its own gave them nothing."""
    weights = {"big": 0.6, **{f"small{i}": 0.04 for i in range(10)}}
    sampled = create_question_set.stratified_sample_questions(
        _pool(weights, available=20), n_target=10, random_state=np.random.RandomState(0)
    )
    counts = sampled["composite_bin"].value_counts()
    assert len(sampled) == 10
    assert counts["big"] == 6
    assert (counts.drop("big") == 1).sum() == 4


def test_leftover_slots_go_to_the_largest_remainders_first():
    """Targets 3.7, 3.2, 3.1 at n=10 floor to 9; the one spare slot goes to the 0.7 remainder."""
    weights = {"a": 0.37, "b": 0.32, "c": 0.31}
    sampled = create_question_set.stratified_sample_questions(
        _pool(weights, available=20), n_target=10, random_state=np.random.RandomState(0)
    )
    counts = sampled["composite_bin"].value_counts()
    assert counts.to_dict() == {"a": 4, "b": 3, "c": 3}


def test_bin_that_runs_out_hands_its_slots_to_bins_with_room():
    """A half-weight bin with one row contributes one; the other bin fills the rest."""
    weights = {"thin": 0.5, "deep": 0.5}
    sampled = create_question_set.stratified_sample_questions(
        _pool(weights, available={"thin": 1, "deep": 20}),
        n_target=10,
        random_state=np.random.RandomState(0),
    )
    counts = sampled["composite_bin"].value_counts()
    assert counts.to_dict() == {"deep": 9, "thin": 1}
