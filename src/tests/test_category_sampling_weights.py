"""Market sampling draws within each bin with per-category weights, so a category can be downsampled."""

import numpy as np
import pandas as pd

from curate_questions.create_question_set import main as create_question_set
from helpers import constants


def _one_bin_pool(categories: list[str]) -> pd.DataFrame:
    """A pool where every row falls in the same composite bin, so only category weights matter."""
    n = len(categories)
    return pd.DataFrame(
        {
            "id": [f"q{i}" for i in range(n)],
            "category": categories,
            "composite_bin": ["0.3-0.4%_8-30d"] * n,
            "bin_weight": [1.0] * n,
        }
    )


def test_downweighted_category_is_rarely_drawn(monkeypatch):
    monkeypatch.setitem(create_question_set.CATEGORY_SAMPLING_WEIGHTS, "Sports", 1e-9)
    pool = _one_bin_pool(["Sports"] * 50 + ["Politics & Governance"] * 50)
    sampled = create_question_set.stratified_sample_questions(
        pool, n_target=20, random_state=np.random.RandomState(0)
    )
    assert len(sampled) == 20
    assert (sampled["category"] == "Sports").sum() == 0


def test_bin_made_only_of_downweighted_category_still_fills():
    pool = _one_bin_pool(["Sports"] * 30)
    sampled = create_question_set.stratified_sample_questions(
        pool, n_target=10, random_state=np.random.RandomState(0)
    )
    assert len(sampled) == 10


def test_category_weights_cover_every_category_once_and_are_positive():
    # A missing category would give NaN draw weights; a zero weight could make a bin unfillable.
    assert set(create_question_set.CATEGORY_SAMPLING_WEIGHTS) == set(constants.QUESTION_CATEGORIES)
    assert all(w > 0 for w in create_question_set.CATEGORY_SAMPLING_WEIGHTS.values())
