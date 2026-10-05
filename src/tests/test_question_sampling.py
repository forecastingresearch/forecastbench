"""End-to-end tests of question set creation: filtering, allocation, sampling and output shape."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from helpers import constants, question_curation
from question_sampling import create_question_set
from question_sampling.create_question_set import QUESTION_SET_COLUMNS
from sources.registry import SOURCES
from tests.conftest import make_question_df

FREEZE_DATETIME = datetime(2026, 10, 3)
FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=10)
CATEGORIES = ["Politics & Governance", "Economics & Business", "Science & Tech"]


def _iso_after_due(days: int) -> str:
    return (FORECAST_DATETIME + timedelta(days=days)).isoformat()


def _market_questions(prefix: str, n: int = 10) -> pd.DataFrame:
    """Market questions spread over probabilities and close dates well inside the sampling window."""
    rows = [
        {
            "id": f"{prefix}{i}",
            "question": f"{prefix} question {i}?",
            "url": f"https://example.com/{prefix}{i}",
            "freeze_datetime_value": str(0.15 + 0.08 * i),
            "market_info_close_datetime": _iso_after_due(20 + 15 * i),
        }
        for i in range(n)
    ]
    return make_question_df(rows)


def _dataset_questions(prefix: str, n: int = 10) -> pd.DataFrame:
    rows = [
        {
            "id": f"{prefix}{i}",
            "question": f"Will there be more {prefix} {i}?",
            "url": f"https://example.com/{prefix}{i}",
            "freeze_datetime_value": str(10 + i),
            "forecast_horizons": list(constants.FORECAST_HORIZONS_IN_DAYS),
        }
        for i in range(n)
    ]
    return make_question_df(rows)


def _metadata(questions: dict[str, pd.DataFrame], invalid: set[str] = frozenset()) -> pd.DataFrame:
    rows = []
    for source, dfq in questions.items():
        for i, qid in enumerate(dfq["id"]):
            rows.append(
                {
                    "source": source,
                    "id": qid,
                    "category": CATEGORIES[i % len(CATEGORIES)],
                    "valid_question": qid not in invalid,
                    "english": True,
                }
            )
    return pd.DataFrame(rows, columns=constants.META_DATA_FILE_COLUMNS)


def _sources(*names: str) -> dict:
    return {name: SOURCES[name] for name in names}


def _shares(*names: str) -> dict:
    return {
        name: question_curation.FREEZE_QUESTION_SOURCES[name]["sampling_share"] for name in names
    }


def _ids(question_set: dict) -> set[tuple[str, str]]:
    return {(q["source"], q["id"]) for q in question_set["questions"]}


def _create(questions, dfmeta, seed=0, **kwargs):
    return create_question_set(
        dfmeta=dfmeta,
        questions=questions,
        sources=_sources(*questions),
        sampling_shares=_shares(*questions),
        freeze_datetime=FREEZE_DATETIME,
        forecast_datetime=FORECAST_DATETIME,
        num_llm_questions_by_type={"market": 6, "dataset": 4},
        num_human_questions_by_type={"market": 2, "dataset": 2},
        random_state=np.random.RandomState(seed),
        **kwargs,
    )


@pytest.fixture()
def bank():
    questions = {
        "polymarket": _market_questions("pm"),
        "kalshi": _market_questions("ks"),
        "fred": _dataset_questions("fred"),
        "acled": _dataset_questions("acled"),
    }
    return questions, _metadata(questions)


class TestQuestionSetShape:
    def test_llm_and_human_set_sizes_and_nesting(self, bank):
        questions, dfmeta = bank
        result = _create(questions, dfmeta)

        llm, human = result.llm_question_set, result.human_question_set
        assert len(llm["questions"]) == 10
        assert len(human["questions"]) == 4
        assert _ids(human) <= _ids(llm)

        # Even allocation across the sources of each question type.
        llm_counts = pd.Series([q["source"] for q in llm["questions"]]).value_counts()
        assert llm_counts.to_dict() == {"polymarket": 3, "kalshi": 3, "fred": 2, "acled": 2}

    def test_question_set_metadata_and_columns(self, bank):
        questions, dfmeta = bank
        result = _create(questions, dfmeta)

        llm = result.llm_question_set
        assert llm["forecast_due_date"] == "2026-10-13"
        assert llm["question_set"] == "2026-10-13-llm.json"
        assert result.human_question_set["question_set"] == "2026-10-13-human.json"
        for question in llm["questions"]:
            assert list(question) == QUESTION_SET_COLUMNS
            assert question["freeze_datetime"] == FREEZE_DATETIME.isoformat()
            source = SOURCES[question["source"]]
            assert question["source_intro"] == source.source_intro
            assert question["resolution_criteria"] == source.resolution_criteria.format(
                url=question["url"]
            )

    def test_resolution_dates_follow_the_forecast_horizons(self, bank):
        questions, dfmeta = bank
        result = _create(questions, dfmeta)

        expected_dataset_dates = [
            (FORECAST_DATETIME + timedelta(days=h)).date().isoformat()
            for h in constants.FORECAST_HORIZONS_IN_DAYS
        ]
        for question in result.llm_question_set["questions"]:
            if question["source"] in ("fred", "acled"):
                assert question["resolution_dates"] == expected_dataset_dates
            else:
                assert question["resolution_dates"] == "N/A"

    def test_same_seed_gives_the_same_question_sets(self, bank):
        questions, dfmeta = bank
        assert _create(questions, dfmeta, seed=7) == _create(questions, dfmeta, seed=7)


class TestEligibilityFilters:
    def test_ineligible_questions_are_never_sampled(self, bank):
        questions, dfmeta = bank
        pm, fred = questions["polymarket"], questions["fred"]

        extra_market = make_question_df(
            [
                {"id": "resolved", "resolved": True, "freeze_datetime_value": "0.5"},
                {"id": "no_freeze", "resolved": False, "freeze_datetime_value": "N/A"},
                {"id": "too_soon", "resolved": False, "freeze_datetime_value": "0.5"},
                {"id": "too_late", "resolved": False, "freeze_datetime_value": "0.5"},
                {"id": "invalid", "resolved": False, "freeze_datetime_value": "0.5"},
                {"id": "other_category", "resolved": False, "freeze_datetime_value": "0.5"},
            ]
        )
        extra_market["market_info_close_datetime"] = [
            _iso_after_due(30),
            _iso_after_due(30),
            _iso_after_due(3),
            _iso_after_due(400),
            _iso_after_due(30),
            _iso_after_due(30),
        ]
        questions["polymarket"] = pd.concat([pm, extra_market], ignore_index=True)

        extra_dataset = make_question_df(
            [
                {"id": "AMERIBOR", "freeze_datetime_value": "5.0", "forecast_horizons": [30]},
                {"id": "no_horizons", "freeze_datetime_value": "5.0", "forecast_horizons": []},
            ]
        )
        questions["fred"] = pd.concat([fred, extra_dataset], ignore_index=True)

        dfmeta = _metadata(questions, invalid={"invalid"})
        dfmeta.loc[dfmeta["id"] == "other_category", "category"] = "Other"

        # Sample everything that is eligible so an ineligible question showing up is impossible to
        # miss.
        result = create_question_set(
            dfmeta=dfmeta,
            questions=questions,
            sources=_sources(*questions),
            sampling_shares=_shares(*questions),
            freeze_datetime=FREEZE_DATETIME,
            forecast_datetime=FORECAST_DATETIME,
            num_llm_questions_by_type={"market": 20, "dataset": 20},
            num_human_questions_by_type={"market": 2, "dataset": 2},
            random_state=0,
        )

        sampled = _ids(result.llm_question_set)
        assert {s for s, _ in sampled} == set(questions)
        for ineligible in (
            "resolved",
            "no_freeze",
            "too_soon",
            "too_late",
            "invalid",
            "other_category",
        ):
            assert ("polymarket", ineligible) not in sampled
        assert ("fred", "AMERIBOR") not in sampled  # nullified in sources/_metadata.py
        assert ("fred", "no_horizons") not in sampled
        assert len(sampled) == 40

    def test_stale_forecast_horizons_are_not_asked(self, bank):
        questions, dfmeta = bank
        questions["fred"]["forecast_horizons"] = [[7, 30, 365, 1095, 1825, 3650]] * len(
            questions["fred"]
        )

        result = _create(questions, dfmeta)

        current = [
            (FORECAST_DATETIME + timedelta(days=h)).date().isoformat()
            for h in [7, 30, 365]
            if h in constants.FORECAST_HORIZONS_IN_DAYS
        ]
        fred_questions = [q for q in result.llm_question_set["questions"] if q["source"] == "fred"]
        assert fred_questions
        assert all(q["resolution_dates"] == current for q in fred_questions)

    def test_source_without_questions_is_skipped(self, bank):
        questions, dfmeta = bank
        questions["fred"] = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)

        result = _create(questions, dfmeta)

        sources = {q["source"] for q in result.llm_question_set["questions"]}
        assert sources == {"polymarket", "kalshi", "acled"}
        assert len(result.llm_question_set["questions"]) == 10
