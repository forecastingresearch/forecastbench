"""Manifold question source."""

from __future__ import annotations

from typing import ClassVar

from _types import SourceType

from ._market import MarketSource


class ManifoldSource(MarketSource):
    """Manifold prediction market source."""

    name: ClassVar[str] = "manifold"
    display_name: ClassVar[str] = "Manifold"
    source_type: ClassVar[SourceType] = SourceType.MARKET
