"""Unit tests for the allocation wrappers and the market-value and time-horizon bins.

``allocate_by_share`` itself is covered in ``test_allocate_by_share.py``.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from question_sampling._allocation import allocate_across_categories, allocate_across_sources
from question_sampling._binning import get_market_value_bin, get_time_horizon_bin

FORECAST_DATETIME = datetime(2026, 10, 13, tzinfo=timezone.utc)


def test_allocate_across_categories_is_even_and_sums():
    dfq = pd.DataFrame({"category": ["x"] * 10 + ["y"] * 10 + ["z"] * 10})
    out = allocate_across_categories(9, dfq)
    assert sum(out.values()) == 9
    assert out == {"x": 3, "y": 3, "z": 3}


def test_allocate_across_sources_honours_each_sources_share():
    # Unequal shares, so a wrapper that fell back to an even split would be caught.
    questions = {
        "fred": {"num_questions_available": 100, "sampling_share": 0.9},
        "manifold": {"num_questions_available": 100, "sampling_share": 0.1},
    }
    out = allocate_across_sources(questions, 40)
    assert out["fred"]["num_questions_to_sample"] == 36
    assert out["manifold"]["num_questions_to_sample"] == 4
    assert "num_questions_to_sample" not in questions["fred"]


class TestBinning:
    @pytest.mark.parametrize("value", ["N/A", None, "not-a-number"])
    def test_market_value_unknown_inputs(self, value):
        assert get_market_value_bin(value) == "unknown"

    def test_market_value_known_input_lands_in_a_bin(self):
        assert get_market_value_bin(0.5) != "unknown"

    @pytest.mark.parametrize(
        ("value", "label"),
        [
            (0.0, "0.0-0.01%"),  # the first bin's floor is inclusive
            (0.1, "0.1-0.2%"),  # an interior edge belongs to the bin it opens...
            (0.4, "0.4-0.5%"),
            (0.9, "0.8-0.9%"),  # ...except 0.9, which the config keeps in the 0.8-0.9 bin
            (0.99, "0.99-1.0%"),
            (1.0, "unknown"),  # the last bin's ceiling is exclusive
        ],
    )
    def test_market_value_bin_edges(self, value, label):
        assert get_market_value_bin(value) == label

    @pytest.mark.parametrize(
        ("days", "label"),
        [
            (0, "0-7d"),
            (7, "0-7d"),  # both ends of a horizon bin are inclusive
            (8, "8-25d"),
            (25, "8-25d"),
            (26, "26-50d"),
            (365, "181-365d"),
            (366, "366-infd"),
        ],
    )
    def test_time_horizon_bin_edges(self, days, label):
        close = (FORECAST_DATETIME + timedelta(days=days)).isoformat()
        assert get_time_horizon_bin(close, FORECAST_DATETIME) == label

    @pytest.mark.parametrize("value", ["N/A", None, "not-a-date"])
    def test_time_horizon_unknown_inputs(self, value):
        assert get_time_horizon_bin(value, FORECAST_DATETIME) == "unknown"
