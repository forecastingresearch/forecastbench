"""Sampling should exclude INFER; resolution/categorization should still include it."""

from helpers import question_curation
from sources import MARKET_SOURCE_NAMES


def test_infer_not_sampled():
    assert "infer" not in question_curation.MARKET_SOURCES
    assert "infer" not in question_curation.FREEZE_QUESTION_MARKET_SOURCES


def test_infer_still_a_market_source_for_resolution():
    assert "infer" in MARKET_SOURCE_NAMES
