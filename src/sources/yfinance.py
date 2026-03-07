"""Yahoo Finance question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._data import DataSource


class YfinanceSource(DataSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"
    display_name: ClassVar[str] = "Yahoo Finance"
    source_type: ClassVar[SourceType] = SourceType.DATA
