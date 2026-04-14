"""Tests for _fb_types.py and _schemas.py: types, schemas, and source ClassVars."""

from datetime import date

import pandas as pd
import pytest

from _fb_types import NullifiedQuestion, SourceQuestionBank, SourceType
from sources import SOURCES

# ---------------------------------------------------------------------------
# _fb_types.py
# ---------------------------------------------------------------------------


class TestSourceType:
    """Test SourceType enum."""

    def test_values_exist(self):
        assert SourceType.MARKET is not None
        assert SourceType.DATASET is not None

    def test_distinct(self):
        assert SourceType.MARKET != SourceType.DATASET


class TestNullifiedQuestion:
    """Test NullifiedQuestion dataclass."""

    def test_construction(self):
        nq = NullifiedQuestion(id="q1", nullification_start_date=date(2024, 1, 1))
        assert nq.id == "q1"
        assert nq.nullification_start_date == date(2024, 1, 1)

    def test_frozen(self):
        nq = NullifiedQuestion(id="q1", nullification_start_date=date(2024, 1, 1))
        with pytest.raises(AttributeError):
            nq.id = "q2"


class TestSourceQuestionBank:
    """Test SourceQuestionBank dataclass."""

    def test_construction(self):
        dfq = pd.DataFrame({"id": ["q1"]})
        dfr = pd.DataFrame({"id": ["q1"], "date": ["2025-01-01"], "value": [100]})
        sqb = SourceQuestionBank(dfq=dfq, dfr=dfr)
        assert len(sqb.dfq) == 1
        assert sqb.hash_mapping is None

    def test_with_hash_mapping(self):
        dfq = pd.DataFrame({"id": ["q1"]})
        dfr = pd.DataFrame({"id": ["q1"], "date": ["2025-01-01"], "value": [100]})
        sqb = SourceQuestionBank(dfq=dfq, dfr=dfr, hash_mapping={"h1": {"key": "v"}})
        assert sqb.hash_mapping == {"h1": {"key": "v"}}


# ---------------------------------------------------------------------------
# Concrete source ClassVars
# ---------------------------------------------------------------------------


_EXPECTED_SOURCES = {
    "acled": SourceType.DATASET,
    "dbnomics": SourceType.DATASET,
    "fred": SourceType.DATASET,
    "infer": SourceType.MARKET,
    "manifold": SourceType.MARKET,
    "metaculus": SourceType.MARKET,
    "polymarket": SourceType.MARKET,
    "wikipedia": SourceType.DATASET,
    "yfinance": SourceType.DATASET,
}


class TestConcreteSourceClassVars:
    """Verify all concrete sources have correct ClassVars."""

    @pytest.mark.parametrize("name", sorted(_EXPECTED_SOURCES.keys()))
    def test_source_name_and_type(self, name):
        source = SOURCES[name]
        assert source.name == name
        assert source.source_type == _EXPECTED_SOURCES[name]

    def test_all_sources_registered(self):
        assert set(SOURCES.keys()) == set(_EXPECTED_SOURCES.keys())
