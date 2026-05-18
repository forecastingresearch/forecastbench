"""Yahoo Finance question source."""

from __future__ import annotations

from typing import ClassVar

from ._dataset import DatasetSource


class YfinanceSource(DatasetSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"

    def fetch(self, **kwargs):
        """Fetch Yahoo Finance data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched Yahoo Finance data into questions and resolution files."""
        raise NotImplementedError
