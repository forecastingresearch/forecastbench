"""Yahoo Finance question source."""

from __future__ import annotations

from typing import ClassVar

from _fb_types import SourceType

from ._dataset import DatasetSource


class YfinanceSource(DatasetSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"
    display_name: ClassVar[str] = "Yahoo Finance"
    source_type: ClassVar[SourceType] = SourceType.DATASET
