"""End-to-end tests of question set creation: filtering, allocation, sampling and output shape."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from helpers import constants, question_curation
from question_sampling import create_question_set
from sources import SOURCE_METADATA
from sources.registry import SOURCES
from tests.conftest import make_question_df

# tz-aware UTC, like the question bank datetimes and `question_curation.FREEZE_DATETIME`.
FREEZE_DATETIME = datetime(2026, 10, 3, tzinfo=timezone.utc)
FORECAST_DATETIME = FREEZE_DATETIME + timedelta(days=10)
CATEGORIES = ["Politics & Governance", "Economics & Business", "Science & Tech"]

# The key order of a published question. Pinned literally so that dropping or reordering a column
# of the question set files fails here rather than in the forecasters that read them.
PUBLISHED_QUESTION_KEYS = [
    "id",
    "source",
    "question",
    "resolution_criteria",
    "background",
    "market_info_open_datetime",
    "market_info_close_datetime",
    "market_info_resolution_criteria",
    "url",
    "freeze_datetime",
    "freeze_datetime_value",
    "freeze_datetime_value_explanation",
    "source_intro",
    "resolution_dates",
]


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


@pytest.fixture(autouse=True)
def acled_hash_mapping(monkeypatch):
    """ACLED types its questions through its hash mapping, so map the ids the tests mint."""
    mapping = {
        f"acled{i}": {
            "key": "last30Days.gt.30DayAvgOverPast360Days",
            "event_type": "Battles",
            "country": "Yemen",
        }
        for i in range(100)
    }
    monkeypatch.setattr(SOURCES["acled"], "hash_mapping", mapping)


def _ids(question_set: dict) -> set[tuple[str, str]]:
    return {(q["source"], q["id"]) for q in question_set["questions"]}


def _create(questions, dfmeta, seed=0, random_state=None, **kwargs):
    if random_state is None:
        random_state = np.random.RandomState(seed)
    return create_question_set(
        dfmeta=dfmeta,
        questions=questions,
        sources=_sources(*questions),
        sampling_shares=_shares(*questions),
        freeze_datetime=FREEZE_DATETIME,
        forecast_datetime=FORECAST_DATETIME,
        num_llm_questions_by_type={"market": 6, "dataset": 4},
        num_human_questions_by_type={"market": 2, "dataset": 2},
        random_state=random_state,
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
            assert list(question) == PUBLISHED_QUESTION_KEYS
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

    def test_an_int_seed_is_one_generator_for_the_whole_run(self, bank):
        """An int seed must behave like the equivalent generator: one sequence shared by every draw.

        Otherwise each source re-seeds from the int and draws the same row positions as every
        other source, in the LLM pass and in the human pass.
        """
        questions, dfmeta = bank
        assert _create(questions, dfmeta, random_state=0) == _create(
            questions, dfmeta, random_state=np.random.RandomState(0)
        )


class TestEligibilityFilters:
    def test_ineligible_questions_are_never_sampled(self, bank):
        """The sampled set is exactly the eligible set, so a filter that stops working is caught.

        The request equals the number of eligible questions, so a question that leaked through a
        filter would be drawn and displace an eligible one. The ineligible market rows close
        inside a sampled time-horizon bin (the too-soon/too-late dates are carried by the
        resolution datetime) and each ineligible dataset row has a category of its own, so a leak
        is drawn whatever the seed instead of being hidden by a zero-weight bin or by chance.
        """
        questions, dfmeta = bank
        pm, fred = questions["polymarket"], questions["fred"]

        def market_row(question_id: str, **overrides) -> dict:
            return {
                "id": question_id,
                "resolved": False,
                "freeze_datetime_value": "0.5",
                "market_info_close_datetime": _iso_after_due(30),
                "market_info_resolution_datetime": "N/A",
                **overrides,
            }

        extra_market = make_question_df(
            [
                market_row("resolved", resolved=True),
                market_row("no_freeze", freeze_datetime_value="N/A"),
                market_row("too_soon", market_info_resolution_datetime=_iso_after_due(3)),
                market_row("too_late", market_info_resolution_datetime=_iso_after_due(400)),
                market_row("invalid"),
                market_row("not_english"),
                market_row("other_category"),
            ]
        )
        questions["polymarket"] = pd.concat([pm, extra_market], ignore_index=True)

        def dataset_row(question_id: str, **overrides) -> dict:
            return {
                "id": question_id,
                "freeze_datetime_value": "5.0",
                "forecast_horizons": [30],
                **overrides,
            }

        retired_id = sorted(SOURCE_METADATA["fred"]["retired_question_ids"])[0]
        extra_dataset = make_question_df(
            [
                dataset_row("AMERIBOR"),  # nullified in sources/_metadata.py
                dataset_row(retired_id),
                dataset_row("no_horizons", forecast_horizons=[]),
                dataset_row("no_freeze", freeze_datetime_value="N/A"),
            ]
        )
        questions["fred"] = pd.concat([fred, extra_dataset], ignore_index=True)

        dfmeta = _metadata(questions, invalid={"invalid"})
        dfmeta.loc[dfmeta["id"] == "not_english", "english"] = False
        dfmeta.loc[dfmeta["id"] == "other_category", "category"] = "Other"
        own_categories = [
            "Healthcare & Biology",
            "Environment & Energy",
            "Arts & Recreation",
            "Security & Defense",
        ]
        for question_id, category in zip(extra_dataset["id"], own_categories):
            is_row = (dfmeta["source"] == "fred") & (dfmeta["id"] == question_id)
            dfmeta.loc[is_row, "category"] = category

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

        eligible = {
            (source, f"{prefix}{i}")
            for source, prefix in (
                ("polymarket", "pm"),
                ("kalshi", "ks"),
                ("fred", "fred"),
                ("acled", "acled"),
            )
            for i in range(10)
        }
        assert _ids(result.llm_question_set) == eligible

    def test_stale_forecast_horizons_are_not_asked(self, bank):
        """Horizons the benchmark no longer asks are dropped from the bank's lists; current ones stay."""
        questions, dfmeta = bank
        questions["fred"]["forecast_horizons"] = [[7, 30, 365, 1095, 1825, 3650]] * len(
            questions["fred"]
        )
        questions["acled"]["forecast_horizons"] = [[30, 365]] * len(questions["acled"])

        result = _create(questions, dfmeta)

        def resolution_dates(horizons: list[int]) -> list[str]:
            return [
                (FORECAST_DATETIME + timedelta(days=h)).date().isoformat()
                for h in horizons
                if h in constants.FORECAST_HORIZONS_IN_DAYS
            ]

        by_source = {}
        for question in result.llm_question_set["questions"]:
            by_source.setdefault(question["source"], []).append(question["resolution_dates"])
        assert by_source["fred"] and by_source["acled"]
        assert all(dates == resolution_dates([7, 30, 365]) for dates in by_source["fred"])
        assert all(dates == resolution_dates([30, 365]) for dates in by_source["acled"])

    def test_source_without_questions_is_skipped(self, bank):
        questions, dfmeta = bank
        questions["fred"] = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)

        result = _create(questions, dfmeta)

        sources = {q["source"] for q in result.llm_question_set["questions"]}
        assert sources == {"polymarket", "kalshi", "acled"}
        assert len(result.llm_question_set["questions"]) == 10
