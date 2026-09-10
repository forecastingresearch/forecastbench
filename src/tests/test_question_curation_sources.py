"""Sampling excludes INFER and Wikipedia; the source registry still includes them for resolution."""

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
