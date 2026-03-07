"""FRED question source."""

from __future__ import annotations

from typing import ClassVar

from _types import NullifiedQuestion, SourceType
from helpers_new.constants import BENCHMARK_START_DATE_DATETIME_DATE

from ._base import DataSource


class FredSource(DataSource):
    """Federal Reserve Economic Data source — inherits standard data resolution."""

    name: ClassVar[str] = "fred"
    display_name: ClassVar[str] = "FRED"
    source_type: ClassVar[SourceType] = SourceType.DATA
    source_intro: ClassVar[str] = (
        "The Federal Reserve Economic Data database (FRED) provides economic data from national, "
        "international, public, and private sources."
        "You're going to predict how questions based on this data will resolve."
    )
    resolution_criteria: ClassVar[str] = (
        "Resolves to the value found at {url} once the data is published."
    )
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(
            id="AMERIBOR", nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE
        ),
    ]
