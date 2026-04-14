"""FRED question source."""

from __future__ import annotations

from typing import ClassVar

from _fb_types import NullifiedQuestion
from helpers.constants import BENCHMARK_START_DATE_DATETIME_DATE

from ._dataset import DatasetSource

NULLIFIED_IDS = [
    "AMERIBOR",
]


class FredSource(DatasetSource):
    """Federal Reserve Economic Data source."""

    name: ClassVar[str] = "fred"
    display_name: ClassVar[str] = "FRED"
    nullified_questions: ClassVar[list[NullifiedQuestion]] = [
        NullifiedQuestion(id=nid, nullification_start_date=BENCHMARK_START_DATE_DATETIME_DATE)
        for nid in NULLIFIED_IDS
    ]

    def fetch(self, **kwargs):
        """Fetch FRED data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched FRED data into questions and resolution files."""
        raise NotImplementedError
