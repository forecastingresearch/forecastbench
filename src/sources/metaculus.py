"""Metaculus question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._market import MarketSource


class MetaculusSource(MarketSource):
    """Metaculus prediction market source."""

    name: ClassVar[str] = "metaculus"
    display_name: ClassVar[str] = "Metaculus"
    source_type: ClassVar[SourceType] = SourceType.MARKET
