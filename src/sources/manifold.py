"""Manifold question source."""

from __future__ import annotations

from typing import ClassVar

from ._market import MarketSource


class ManifoldSource(MarketSource):
    """Manifold prediction market source."""

    name: ClassVar[str] = "manifold"

    def fetch(self, **kwargs):
        """Fetch Manifold data from external API."""
        raise NotImplementedError

    def update(self, dfq, dff, **kwargs):
        """Process fetched Manifold data into questions and resolution files."""
        raise NotImplementedError
