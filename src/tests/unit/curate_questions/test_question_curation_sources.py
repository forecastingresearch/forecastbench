"""Sampling excludes INFER, Manifold and Wikipedia, which the registry still resolves.

Sampling shares in each source type sum to 1.
"""

import math

import pytest

from helpers import question_curation
from sources import DATASET_SOURCE_NAMES, MARKET_SOURCE_NAMES


def test_infer_not_sampled():
    assert "infer" not in question_curation.MARKET_SOURCES
    assert "infer" not in question_curation.FREEZE_QUESTION_MARKET_SOURCES


def test_infer_still_a_market_source_for_resolution():
    assert "infer" in MARKET_SOURCE_NAMES


def test_wikipedia_not_sampled():
    assert "wikipedia" not in question_curation.DATA_SOURCES
    assert "wikipedia" not in question_curation.FREEZE_QUESTION_DATA_SOURCES


def test_wikipedia_still_a_dataset_source_for_resolution():
    assert "wikipedia" in DATASET_SOURCE_NAMES


def test_yfinance_still_a_dataset_source_for_resolution():
    assert "yfinance" in DATASET_SOURCE_NAMES


def test_manifold_not_sampled():
    assert "manifold" not in question_curation.MARKET_SOURCES
    assert "manifold" not in question_curation.FREEZE_QUESTION_MARKET_SOURCES


def test_manifold_still_a_market_source_for_resolution():
    assert "manifold" in MARKET_SOURCE_NAMES


@pytest.mark.parametrize(
    "sources",
    [
        question_curation.FREEZE_QUESTION_MARKET_SOURCES,
        question_curation.FREEZE_QUESTION_DATA_SOURCES,
    ],
    ids=["market", "data"],
)
def test_sampling_shares_sum_to_one(sources):
    assert math.isclose(sum(source["sampling_share"] for source in sources.values()), 1)
