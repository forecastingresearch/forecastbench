"""Shared pytest fixtures for ForecastBench tests (builders live in factories.py)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from sources.acled import AcledSource
from sources.dbnomics import DbnomicsSource
from sources.fred import FredSource
from sources.kalshi import KalshiSource
from sources.manifold import ManifoldSource
from sources.metaculus import MetaculusSource
from sources.polymarket import PolymarketSource
from sources.wikipedia import WikipediaSource
from sources.yfinance import YfinanceSource

# ---------------------------------------------------------------------------
# Time-freezing fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def freeze_today():
    """Patch dates.get_date_today() and get_date_yesterday() to return deterministic values.

    Usage:
        def test_something(freeze_today):
            freeze_today(date(2025, 1, 15))
            # Now dates.get_date_today() returns date(2025, 1, 15)
            # and dates.get_date_yesterday() returns date(2025, 1, 14)
    """
    patches = []

    def _freeze(target_date):
        target_datetime = datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
        )
        p1 = patch("helpers.dates.get_date_today", return_value=target_date)
        p2 = patch("helpers.dates.get_date_yesterday", return_value=target_date - timedelta(days=1))
        p3 = patch("helpers.dates.get_datetime_today", return_value=target_datetime)
        patches.extend([p1, p2, p3])
        for p in [p1, p2, p3]:
            p.start()

    yield _freeze

    # Unwind in reverse so nested freezes restore the original, not an intermediate mock.
    for p in reversed(patches):
        p.stop()


# ---------------------------------------------------------------------------
# Source instance fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def market_source():
    """Return a concrete MarketSource instance."""
    return MetaculusSource()


@pytest.fixture()
def dataset_source():
    """Return a concrete DatasetSource instance."""
    return FredSource()


@pytest.fixture()
def acled_source():
    """Return an AcledSource instance."""
    return AcledSource()


@pytest.fixture()
def acled_source_with_creds():
    """Return an AcledSource instance with fake API credentials."""
    src = AcledSource()
    src.api_email = "test@example.com"
    src.api_password = "test-password"
    return src


@pytest.fixture()
def dbnomics_source():
    """Return a DbnomicsSource instance."""
    return DbnomicsSource()


@pytest.fixture()
def fred_source():
    """Return a FredSource instance with a fake API key."""
    src = FredSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def kalshi_source():
    """Return a KalshiSource instance."""
    return KalshiSource()


@pytest.fixture()
def manifold_source():
    """Return a ManifoldSource instance."""
    return ManifoldSource()


@pytest.fixture()
def metaculus_source():
    """Return a MetaculusSource instance with a fake API key."""
    src = MetaculusSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def polymarket_source():
    """Return a PolymarketSource instance."""
    return PolymarketSource()


@pytest.fixture()
def wikipedia_source():
    """Return a WikipediaSource instance."""
    return WikipediaSource()


@pytest.fixture()
def yfinance_source():
    """Return a YfinanceSource instance with no pair questions and no rate-limit sleep.

    Tests of the pair pass set ``source.pairs`` themselves.
    """
    source = YfinanceSource()
    source.pairs = []
    with patch("sources.yfinance.time.sleep"):
        yield source
