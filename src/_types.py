"""Shared types used across multiple modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto

import pandas as pd


class SourceType(Enum):
    """Whether a source is market-based or data-based."""

    MARKET = auto()
    DATA = auto()


@dataclass(frozen=True)
class NullifiedQuestion:
    """A question that has been permanently nullified.

    A nullified question will:
    1. No longer have its resolution value fetched/updated.
    2. No longer be sampled into question sets.
    3. Be resolved to null in all resolution sets.
    """

    id: str
    nullification_start_date: date  # nullification applies from this date onward


@dataclass
class SourceQuestionBank:
    """One source's slice of the question bank.

    Loaded by orchestration, consumed by resolve/ and baseline_forecasts/.
    dfr is ResolutionFrame for most sources, but ACLED uses a different
    shape (validated at runtime by the source class).
    """

    dfq: pd.DataFrame  # QuestionFrame
    dfr: pd.DataFrame  # ResolutionFrame or source-specific
    hash_mapping: dict[str, dict] | None = None


QuestionBank = dict[str, SourceQuestionBank]
"""Full question bank: {source_name: SourceQuestionBank}."""


@dataclass
class UpdateResult:
    """Return value of every source's update() method.

    Makes all outputs explicit so orchestration can handle IO.
    """

    dfq: pd.DataFrame  # QuestionFrame
    resolution_files: dict[str, pd.DataFrame] | None = None  # {question_id: ResolutionFrame}
    hash_mapping: dict[str, dict] | None = None  # updated mapping, if source uses one


@dataclass
class QuestionSamplingConfig:
    """Configuration for question set creation."""

    num_llm_questions: int = 500
    num_human_questions: int = 200
    freeze_window_days: int = 10
    forecast_horizons: list[int] = field(default_factory=list)


@dataclass
class QuestionSetResult:
    """Return value of question sampling."""

    llm_question_set: dict  # full JSON structure
    human_question_set: dict


@dataclass
class LeaderboardResult:
    """Return value of leaderboard computation."""

    tables: dict[str, pd.DataFrame]
    html_assets: dict[str, str]  # filename: html_content
    csv_assets: dict[str, str]  # filename: csv_content
