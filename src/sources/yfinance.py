"""Yahoo Finance question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._base import DataSource


class YfinanceSource(DataSource):
    """Yahoo Finance data source — inherits standard data resolution."""

    name: ClassVar[str] = "yfinance"
    display_name: ClassVar[str] = "Yahoo Finance"
    source_type: ClassVar[SourceType] = SourceType.DATA
    source_intro: ClassVar[str] = (
        "Yahoo Finance provides financial data on stocks, bonds, and currencies and also offers "
        "news, commentary and tools for personal financial management. You're going to predict how "
        "questions based on this data will resolve."
    )
    resolution_criteria: ClassVar[str] = (
        "Resolves to the market close price at {url} for the resolution date. If the resolution "
        "date coincides with a day the market is closed (weekend, holiday, etc.) the previous "
        "market close price is used."
    )
