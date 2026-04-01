"""Shared types used across modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum, auto

import pandas as pd


class SourceType(Enum):
    """Whether a source is market-based or dataset-based."""

    MARKET = auto()
    DATASET = auto()


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
