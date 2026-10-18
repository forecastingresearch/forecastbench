"""Sources retire questions from the sampling pool; the bank keeps them so published sets resolve."""

import json
from datetime import date

import pandas as pd
import pytest

from _fb_types import NullifiedQuestion, SourceType
from question_sampling._prepare import drop_nullified_questions, drop_retired_questions
from sources import SOURCE_METADATA
from sources._base import BaseSource
from sources.acled import AcledSource
from sources.registry import SOURCES

ACLED_MORE = "last30Days.gt.30DayAvgOverPast360Days"
ACLED_X10 = "last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1"


def _questions(ids: list[str], **columns) -> pd.DataFrame:
    return pd.DataFrame({"id": ids, "question": ["Will it?"] * len(ids), **columns})


def _acled(mapping: dict[str, dict]) -> AcledSource:
    """Return an ACLED source whose hash mapping holds `mapping`."""
    source = AcledSource()
    source.populate_hash_mapping(json.dumps(mapping))
    return source


def _acled_question(key: str = ACLED_MORE, country: str = "Yemen") -> dict:
    return {"key": key, "event_type": "Battles", "country": country}


class TestRetiredIds:
    @pytest.mark.parametrize("source_name", ["metaculus", "kalshi", "polymarket", "fred"])
    def test_every_retired_id_is_dropped_and_the_others_stay(self, source_name):
        retired = sorted(SOURCE_METADATA[source_name]["retired_question_ids"])
        assert retired, "the retired id set must not be empty"
        result = drop_retired_questions(
            dfq=_questions(retired + ["kept"]), source=SOURCES[source_name]
        )
        assert result["id"].tolist() == ["kept"]

    def test_a_retired_question_is_not_nullified(self):
        """Retirement only leaves the pool: the question keeps resolving, unlike nullification."""
        source = SOURCES["fred"]
        assert "SOFRINDEX" in source.retired_question_ids
        assert "SOFRINDEX" not in source.get_nullified_ids()


class TestDbnomics:
    def test_weather_questions_are_retired(self):
        dfq = _questions(
            ["meteofrance_TEMPERATURE_celsius.07005.D", "ECB_FM_M.U2.EUR.4F.KR.DFR.LEV"]
        )
        result = drop_retired_questions(dfq=dfq, source=SOURCES["dbnomics"])
        assert result["id"].tolist() == ["ECB_FM_M.U2.EUR.4F.KR.DFR.LEV"]

    def test_question_types_are_the_providers(self):
        dfq = _questions(
            ["meteofrance_TEMPERATURE_celsius.07005.D", "ECB_CISS_D.U2.Z0Z.4F.EC.SS_FXN.CON"]
        )
        assert SOURCES["dbnomics"].get_question_types(dfq).tolist() == ["meteofrance", "ECB"]


class TestYfinance:
    def test_single_ticker_questions_are_retired(self):
        """Only pair questions (id X_Y) are sampled; single-ticker rows stay for resolution only."""
        result = drop_retired_questions(
            dfq=_questions(["AAPL", "AAPL_MSFT", "MSFT"]), source=SOURCES["yfinance"]
        )
        assert result["id"].tolist() == ["AAPL_MSFT"]

    def test_a_retired_ticker_retires_every_pair_it_is_in(self, monkeypatch):
        source = SOURCES["yfinance"]
        monkeypatch.setattr(type(source), "retired_question_ids", frozenset({"AAPL"}))
        dfq = _questions(["AAPL", "AAPL_MSFT", "GOOG_AAPL", "GOOG_MSFT"])
        result = drop_retired_questions(dfq=dfq, source=source)
        assert result["id"].tolist() == ["GOOG_MSFT"]


class TestAcled:
    def test_ten_times_questions_are_retired(self):
        source = _acled({"x10": _acled_question(ACLED_X10), "more": _acled_question(ACLED_MORE)})
        dfq = _questions(["x10", "more"], freeze_datetime_value=["11.5", "10.5"])
        result = drop_retired_questions(dfq=dfq, source=source)
        assert result["id"].tolist() == ["more"]

    def test_questions_with_a_zero_baseline_are_retired(self):
        """A zero 30-day average makes the question "will any event happen", so it is not sampled."""
        ids = ["zero", "small", "large"]
        source = _acled({qid: _acled_question(country="Spain") for qid in ids})
        dfq = _questions(ids, freeze_datetime_value=["0.0", "0.08333333333333333", "325.75"])
        result = drop_retired_questions(dfq=dfq, source=source)
        assert result["id"].tolist() == ["small", "large"]

    def test_a_freeze_value_that_is_not_a_number_is_refused(self):
        """ACLED writes a number; anything else is a bug upstream, not a question to sample."""
        source = _acled({"bad": _acled_question(), "good": _acled_question()})
        dfq = _questions(["bad", "good"], freeze_datetime_value=["None", "1.5"])
        with pytest.raises(ValueError):
            drop_retired_questions(dfq=dfq, source=source)

    def test_questions_under_a_retired_country_spelling_are_retired(self):
        """ACLED served Akrotiri and Dhekelia under two spellings; only the current one is sampled."""
        source = _acled(
            {
                "old": _acled_question(country="Akrotiri and Dekhelia"),
                "current": _acled_question(country="Akrotiri and Dhekelia"),
            }
        )
        dfq = _questions(["old", "current"], freeze_datetime_value=["1.5", "1.5"])
        result = drop_retired_questions(dfq=dfq, source=source)
        assert result["id"].tolist() == ["current"]

    def test_question_types_come_from_the_hash_mapping(self):
        source = _acled({"a": _acled_question(ACLED_X10), "b": _acled_question(ACLED_MORE)})
        dfq = _questions(["a", "b"], freeze_datetime_value=["1", "1"])
        assert source.get_question_types(dfq).tolist() == [ACLED_X10, ACLED_MORE]

    def test_an_unmapped_id_fails_loudly(self):
        """Without its hash mapping ACLED cannot tell its questions apart, so it must not guess."""
        source = _acled({"mapped": _acled_question()})
        dfq = _questions(["mapped", "unmapped"], freeze_datetime_value=["1", "1"])
        with pytest.raises(KeyError, match="unmapped"):
            source.is_retired(dfq)

    def test_an_empty_frame_keeps_its_columns(self):
        dfq = _questions([], freeze_datetime_value=[])
        result = drop_retired_questions(dfq=dfq, source=_acled({}))
        assert result.empty and list(result.columns) == list(dfq.columns)


class _Stub(BaseSource):
    """A source with a single kind of question and no metadata entry."""

    name = "stub"
    source_type = SourceType.DATASET
    nullified_questions = []
    _allow_missing_metadata = True

    def _resolve(self, df, dfq, dfr):
        raise NotImplementedError

    def fetch(self, **kwargs):
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        raise NotImplementedError


class TestSourceContract:
    def test_a_source_retires_nothing_by_default(self):
        dfq = _questions(["a", "b"])
        assert _Stub().is_retired(dfq).tolist() == [False, False]
        assert _Stub().get_question_types(dfq).isna().all()

    def test_retiring_an_undeclared_question_type_is_refused(self):
        """A typo in the retired types must fail at import rather than retire nothing."""
        with pytest.raises(TypeError, match="question_types"):

            class _Typo(_Stub):
                retired_question_types = frozenset({"typo"})

    def test_declaring_question_types_requires_a_classifier(self):
        with pytest.raises(TypeError, match="get_question_types"):

            class _Unclassified(_Stub):
                question_types = frozenset({"one", "two"})


def test_drop_nullified_questions_whatever_their_start_date(monkeypatch):
    """A nullified question is never sampled again, even when its nullification starts later."""
    source = SOURCES["metaculus"]
    monkeypatch.setattr(
        type(source),
        "nullified_questions",
        [
            NullifiedQuestion(id="gone", nullification_start_date=date(2020, 1, 1)),
            NullifiedQuestion(id="later", nullification_start_date=date(2999, 1, 1)),
        ],
    )
    dfq = pd.DataFrame({"id": ["gone", "later", "kept"], "question": ["a", "b", "c"]})
    result = drop_nullified_questions(dfq=dfq, source=source)
    assert result["id"].tolist() == ["kept"]
