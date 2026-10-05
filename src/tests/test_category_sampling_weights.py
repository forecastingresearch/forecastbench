"""Market sampling thins the pool by category before binning, so a category can be downsampled."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from helpers import constants
from question_sampling.create_question_set import (
    CATEGORY_SAMPLING_WEIGHTS,
    sample_market_questions,
    thin_pool_by_category,
)

FORECAST_DATETIME = datetime(2026, 10, 13, tzinfo=timezone.utc)


def _pool(categories: list[str], value: float, days_to_close: int) -> pd.DataFrame:
    """Market questions that all land in one composite bin."""
    close = (FORECAST_DATETIME + timedelta(days=days_to_close)).isoformat()
    return pd.DataFrame(
        {
            "id": [f"{value}-{days_to_close}-{i}" for i in range(len(categories))],
            "category": categories,
            "freeze_datetime_value": [str(value)] * len(categories),
            "market_info_close_datetime": [close] * len(categories),
        }
    )


def test_downweighted_category_is_thinned_out_of_the_pool(monkeypatch):
    monkeypatch.setitem(CATEGORY_SAMPLING_WEIGHTS, "Sports", 1e-9)
    pool = _pool(["Sports"] * 500 + ["Politics & Governance"] * 500, 0.45, 30)
    thinned = thin_pool_by_category(pool, random_state=np.random.RandomState(0))
    assert (thinned["category"] == "Sports").sum() == 0
    assert (thinned["category"] == "Politics & Governance").sum() == 500


def test_full_weight_categories_are_kept_whole():
    pool = _pool(["Politics & Governance"] * 200 + ["Science & Tech"] * 200, 0.45, 30)
    thinned = thin_pool_by_category(pool, random_state=np.random.RandomState(0))
    assert sorted(thinned["id"]) == sorted(pool["id"])


def test_thinning_is_reproducible_with_a_seed(monkeypatch):
    monkeypatch.setitem(CATEGORY_SAMPLING_WEIGHTS, "Sports", 0.5)
    pool = _pool(["Sports"] * 400, 0.45, 30)
    first = thin_pool_by_category(pool, random_state=np.random.RandomState(7))
    second = thin_pool_by_category(pool, random_state=np.random.RandomState(7))
    assert 100 < len(first) < 300
    assert first["id"].tolist() == second["id"].tolist()


def test_bin_holding_only_a_downweighted_category_no_longer_forces_it_into_the_set(monkeypatch):
    """A within-bin weight cannot refuse the only questions in a bin; thinning before binning can."""
    monkeypatch.setitem(CATEGORY_SAMPLING_WEIGHTS, "Sports", 1e-9)
    sports_only_bin = _pool(["Sports"] * 20, 0.45, 30)
    other_bins = pd.concat(
        [
            _pool(["Politics & Governance"] * 60, 0.15, 15),
            _pool(["Economics & Business"] * 60, 0.65, 60),
            _pool(["Science & Tech"] * 60, 0.85, 80),
        ]
    )
    pool = pd.concat([sports_only_bin, other_bins], ignore_index=True)
    sampled = sample_market_questions(
        pool,
        n_target=40,
        forecast_datetime=FORECAST_DATETIME,
        random_state=np.random.RandomState(0),
    )
    assert len(sampled) == 40
    assert (sampled["category"] == "Sports").sum() == 0


def test_category_weights_cover_every_category_once_and_are_keep_probabilities():
    # A missing category would thin to NaN; a weight outside (0, 1] is not a probability.
    assert set(CATEGORY_SAMPLING_WEIGHTS) == set(constants.QUESTION_CATEGORIES)
    assert all(0 < w <= 1 for w in CATEGORY_SAMPLING_WEIGHTS.values())
