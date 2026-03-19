"""INFER question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._market import MarketSource


class InferSource(MarketSource):
    """INFER Public prediction market source."""

    name: ClassVar[str] = "infer"
    display_name: ClassVar[str] = "INFER"
    source_type: ClassVar[SourceType] = SourceType.MARKET
