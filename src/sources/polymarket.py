"""Polymarket question source."""

from __future__ import annotations

from typing import ClassVar

from ._market import MarketSource


class PolymarketSource(MarketSource):
    """Polymarket prediction market source."""

    name: ClassVar[str] = "polymarket"

    def fetch(self, **kwargs):
        """Fetch Polymarket data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched Polymarket data into questions and resolution files."""
        raise NotImplementedError
