"""FRED question source."""

from __future__ import annotations

from typing import ClassVar

from ._dataset import DatasetSource
from ._metadata import SOURCE_METADATA

NULLIFIED_QUESTIONS = {
    nq.id: nq.nullification_start_date for nq in SOURCE_METADATA["fred"]["nullified_questions"]
}

NULLIFIED_IDS = [nq.id for nq in SOURCE_METADATA["fred"]["nullified_questions"]]


class FredSource(DatasetSource):
    """Federal Reserve Economic Data source."""

    name: ClassVar[str] = "fred"

    def fetch(self, **kwargs):
        """Fetch FRED data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched FRED data into questions and resolution files."""
        raise NotImplementedError
