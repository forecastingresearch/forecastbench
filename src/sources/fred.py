"""FRED question source."""

from __future__ import annotations

from datetime import date
from typing import ClassVar

from _fb_types import NullifiedQuestion, SourceType
from helpers.constants import BENCHMARK_START_DATE_DATETIME_DATE

from ._dataset import DatasetSource

NULLIFIED_QUESTIONS = {
    "AMERIBOR": BENCHMARK_START_DATE_DATETIME_DATE,
    "CURRCIR": date(2025, 11, 1),
}

NULLIFIED_IDS = list(NULLIFIED_QUESTIONS.keys())


class FredSource(DatasetSource):
    """Federal Reserve Economic Data source."""

    name: ClassVar[str] = "fred"
    display_name: ClassVar[str] = "FRED"
    source_type: ClassVar[SourceType] = SourceType.DATASET
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(id=nid, nullification_start_date=start_date)
        for nid, start_date in NULLIFIED_QUESTIONS.items()
    ]
