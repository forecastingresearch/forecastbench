"""FRED question source."""

from __future__ import annotations

from typing import ClassVar

from _types import NullifiedQuestion, SourceType
from helpers.constants import BENCHMARK_START_DATE_DATETIME_DATE

from ._data import DataSource


class FredSource(DataSource):
    """Federal Reserve Economic Data source."""

    name: ClassVar[str] = "fred"
    display_name: ClassVar[str] = "FRED"
    source_type: ClassVar[SourceType] = SourceType.DATA
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(
            id="AMERIBOR", nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE
        ),
    ]
