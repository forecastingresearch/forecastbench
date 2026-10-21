"""Sample questions.

This module samples questions for LLM and human question sets.

Sampling strategies:
- Sources: Each question type's target is split across its sources by their sampling share
  (`FREEZE_QUESTION_SOURCES` in `helpers/question_curation.py`)
- Market questions: Multi-dimensional binning across market value and time horizon
- Data questions: Even distribution across categories
- Human questions: Random sampling from the LLM question set

Market question sampling aims to achieve balanced representation across market probability values
and time horizons. The bins and their weights are `MARKET_VALUE_CONFIG` and `TIME_HORIZON_CONFIG`
in `_binning.py`; before binning, `CATEGORY_SAMPLING_WEIGHTS` in `_sampling.py` thins the pool by
category.

The module is pure: it takes the question bank data and returns the question sets. Reading the
question bank and writing the question sets is done by the orchestration layer.

The steps live in the private modules of this package: `_prepare` filters a source's question
bank down to the eligible questions, `_allocation` splits the targets across sources and
categories, `_binning` bins market questions and weights the bins, `_sampling` draws the sets and
`_report` logs and plots the result.
"""

import logging
from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from _fb_types import QuestionSetResult, QuestionSetTarget

from ._allocation import allocate_across_sources
from ._prepare import is_market_source, prepare_source_questions
from ._sampling import (
    _as_random_state,
    human_sample_questions,
    llm_sample_questions,
    process_questions,
    validate_bin_weights,
)

if TYPE_CHECKING:
    from sources._base import BaseSource

logger = logging.getLogger(__name__)


NUM_LLM_QUESTIONS = 500
NUM_HUMAN_QUESTIONS = 200

# How each question set is split between market and dataset questions. Dataset questions resolve
# at several horizons, so the human set leans toward market questions to gain power (issue #310).
NUM_LLM_QUESTIONS_BY_TYPE = {"market": 250, "dataset": 250}
NUM_HUMAN_QUESTIONS_BY_TYPE = {"market": 130, "dataset": 70}

assert sum(NUM_LLM_QUESTIONS_BY_TYPE.values()) == NUM_LLM_QUESTIONS
assert sum(NUM_HUMAN_QUESTIONS_BY_TYPE.values()) == NUM_HUMAN_QUESTIONS

# Assumed in the code: human questions are sampled from the LLM set.
assert all(
    NUM_HUMAN_QUESTIONS_BY_TYPE[t] < NUM_LLM_QUESTIONS_BY_TYPE[t] for t in NUM_LLM_QUESTIONS_BY_TYPE
)


# Column order of the questions in the published question set files.
QUESTION_SET_COLUMNS = [
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


def question_set_filename(target: QuestionSetTarget, forecast_datetime: datetime) -> str:
    """Return the filename of a question set, e.g. ``2026-10-13-llm.json``.

    Args:
        target (QuestionSetTarget): Question set target ("llm" or "human")
        forecast_datetime (datetime): The forecast due date of the question set
    """
    return f"{forecast_datetime.date().isoformat()}-{target.value}.json"


def build_question_set(
    questions: dict, question_set_target: QuestionSetTarget, forecast_datetime: datetime
) -> dict:
    """Assemble the sampled questions into the JSON-ready question set.

    Args:
        questions (dict): Source data keyed by source name, each with "dfq" DataFrame
        question_set_target (QuestionSetTarget): Question set target ("llm" or "human")
        forecast_datetime (datetime): The forecast due date, from which the resolution dates of
            dataset questions are computed

    Returns
        question_set (dict): With keys `forecast_due_date`, `question_set` (the filename) and
            `questions` (one record per question, columns `QUESTION_SET_COLUMNS`)
    """

    def forecast_horizons_to_resolution_dates(forecast_horizons: list | str) -> list | str:
        return (
            [
                (forecast_datetime + timedelta(days=day)).date().isoformat()
                for day in forecast_horizons
            ]
            if forecast_horizons != "N/A"
            else forecast_horizons
        )

    dfs = []
    for _, values in questions.items():
        df_source = values["dfq"]
        # Order columns consistently for writing
        df_source = deepcopy(
            df_source[[c for c in QUESTION_SET_COLUMNS if c != "resolution_dates"]]
        )
        df_source["resolution_dates"] = values["dfq"]["forecast_horizons"].apply(
            forecast_horizons_to_resolution_dates
        )
        dfs.append(df_source)

    df = pd.concat(dfs, ignore_index=True)

    return {
        "forecast_due_date": forecast_datetime.date().isoformat(),
        "question_set": question_set_filename(question_set_target, forecast_datetime),
        "questions": df.to_dict(orient="records"),
    }


def create_question_set(
    dfmeta: pd.DataFrame,
    questions: dict[str, pd.DataFrame],
    sources: dict[str, "BaseSource"],
    *,
    sampling_shares: dict[str, float],
    freeze_datetime: datetime,
    forecast_datetime: datetime,
    num_llm_questions_by_type: dict[str, int] = NUM_LLM_QUESTIONS_BY_TYPE,
    num_human_questions_by_type: dict[str, int] = NUM_HUMAN_QUESTIONS_BY_TYPE,
    random_state: int | np.random.RandomState | None = None,
    show_plots: bool = False,
) -> QuestionSetResult:
    """Sample the LLM and human question sets from the question bank.

    Args:
        dfmeta (pd.DataFrame): Question metadata with columns `constants.META_DATA_FILE_COLUMNS`
        questions (dict[str, pd.DataFrame]): The question bank of each source to sample from,
            keyed by source name (`QuestionFrame`). A source with no questions is skipped.
        sources (dict[str, BaseSource]): The sources to sample from, keyed by source name
        sampling_shares (dict[str, float]): Each source's share of its question type's target,
            keyed by source name. The shares of the sources of one question type sum to 1; they
            are renormalised over the sources that have questions.
        freeze_datetime (datetime): The datetime the question bank values were frozen at;
            tz-aware UTC, like the datetimes stored in the question bank
        forecast_datetime (datetime): The forecast due date of the question set; tz-aware UTC
        num_llm_questions_by_type (dict[str, int]): Number of LLM questions to sample per
            question type ("market" and "dataset")
        num_human_questions_by_type (dict[str, int]): Number of human questions to sample per
            question type; the human set is sampled from the LLM set
        random_state: Seed or ``np.random.RandomState`` for reproducibility. An int seeds one
            generator that every draw of the run shares, LLM and human pass alike, so sources
            draw independently of each other. ``None`` (default) samples without a fixed seed.
        show_plots (bool): Whether to display the sampling distribution plots (local runs only)

    Returns
        result (QuestionSetResult): The LLM and human question sets, ready to be written as JSON
    """
    validate_bin_weights()
    random_state = _as_random_state(random_state)

    QUESTIONS = {}
    for source_name, dfq in questions.items():
        if dfq.empty:
            logger.warning(f"Found 0 questions from {source_name}.")
            continue
        dfq = prepare_source_questions(
            source=sources[source_name],
            dfq=dfq,
            dfmeta=dfmeta,
            freeze_datetime=freeze_datetime,
            forecast_datetime=forecast_datetime,
        )
        num_questions = len(dfq)
        QUESTIONS[source_name] = {
            "dfq": dfq,
            "num_questions_available": num_questions,
            "sampling_share": sampling_shares[source_name],
        }
        logger.info(f"Found {num_questions:,} single questions from {source_name}.")

    # Find allocations of questions
    question_type_sources = {
        "market": [s for s in QUESTIONS if is_market_source(s)],
        "dataset": [s for s in QUESTIONS if not is_market_source(s)],
    }
    LLM_QUESTIONS, HUMAN_QUESTIONS = {}, {}
    for question_type, type_sources in question_type_sources.items():
        questions_of_question_type = {k: v for k, v in QUESTIONS.items() if k in type_sources}
        llm_questions_of_question_type = allocate_across_sources(
            questions=questions_of_question_type,
            num_questions=num_llm_questions_by_type[question_type],
        )
        LLM_QUESTIONS.update(llm_questions_of_question_type)
        human_questions_of_question_type = allocate_across_sources(
            questions=llm_questions_of_question_type,
            num_questions=num_human_questions_by_type[question_type],
        )
        HUMAN_QUESTIONS.update(human_questions_of_question_type)

    # Sample questions
    logger.info("LLM SET")
    LLM_QUESTIONS = process_questions(
        questions=QUESTIONS,
        to_questions=LLM_QUESTIONS,
        single_generation_func=partial(llm_sample_questions, forecast_datetime=forecast_datetime),
        show_plots=show_plots,
        question_set_target=QuestionSetTarget.LLM,
        forecast_datetime=forecast_datetime,
        random_state=random_state,
    )

    logger.info("HUMAN SET")
    HUMAN_QUESTIONS = process_questions(
        questions=LLM_QUESTIONS,
        to_questions=HUMAN_QUESTIONS,
        single_generation_func=human_sample_questions,
        show_plots=show_plots,
        question_set_target=QuestionSetTarget.HUMAN,
        forecast_datetime=forecast_datetime,
        random_state=random_state,
    )

    return QuestionSetResult(
        llm_question_set=build_question_set(
            LLM_QUESTIONS, QuestionSetTarget.LLM, forecast_datetime
        ),
        human_question_set=build_question_set(
            HUMAN_QUESTIONS, QuestionSetTarget.HUMAN, forecast_datetime
        ),
    )
