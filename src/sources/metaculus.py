"""Metaculus question source."""

from __future__ import annotations

from typing import ClassVar

from _fb_types import SourceType

from ._market import MarketSource


class MetaculusSource(MarketSource):
    """Metaculus prediction market source."""

    name: ClassVar[str] = "metaculus"
    display_name: ClassVar[str] = "Metaculus"
    source_type: ClassVar[SourceType] = SourceType.MARKET

    def fetch(self, **kwargs):
        """Fetch Metaculus data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched Metaculus data into questions and resolution files."""
        raise NotImplementedError
