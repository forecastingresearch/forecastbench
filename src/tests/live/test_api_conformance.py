"""Live API conformance: opt-in drift detection, never run in PR CI.

Run with ``make test ARGS="-m live"``. Each test calls the source's own request methods (never the
heavyweight ``fetch()``/``update()``), so a dropped or renamed field surfaces through the real
parser. Polymarket discovery is the one re-created request. Keyed endpoints skip when no credential
is available. Live tests only read.
"""

from datetime import date

import pytest
import requests

pytestmark = pytest.mark.live

_HTTP_TIMEOUT = 30


def _resolve_secret_or_skip(attr_name: str) -> str:
    """Resolve a credential via ``helpers.keys``, or ``pytest.skip`` when it is unavailable.

    Live tests lift the fake-secrets guard, so this is a real Secret Manager lookup.

    Args:
        attr_name (str): A ``helpers.keys`` secret attribute, e.g. ``"API_KEY_METACULUS"``.
    """
    from helpers import keys

    if attr_name not in keys._SECRET_NAMES:
        raise AssertionError(f"{attr_name!r} is not a known helpers.keys secret (test typo?)")
    try:
        value = getattr(keys, attr_name)
    except Exception as exc:
        pytest.skip(f"no credential for {attr_name} ({type(exc).__name__}: {exc})")
    if not value:
        pytest.skip(f"credential {attr_name} resolved empty")
    return value


def _keyed_source(source_cls, secret_attr: str):
    """Instantiate ``source_cls`` with its ``api_key`` resolved via keys.py (or skip)."""
    src = source_cls()
    src.api_key = _resolve_secret_or_skip(secret_attr)
    return src


def test_polymarket_gamma_markets_contract():
    """Gamma /markets output still parses through Polymarket's real field consumers.

    Discovery (``_fetch_active_markets_from_api``) paginates every active market, so this issues
    one bounded request and routes each market through ``_is_market_binary`` and
    ``_get_yes_token``, which ``json.loads`` ``outcomes`` and ``clobTokenIds``.
    """
    from sources.polymarket import PolymarketSource

    resp = requests.get("https://gamma-api.polymarket.com/markets?limit=50", timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    markets = resp.json()
    assert isinstance(markets, list) and markets, "expected a non-empty list of markets"

    saw_binary = False
    for market in markets:
        assert "conditionId" in market, "Polymarket dropped conditionId"  # read as m['conditionId']
        assert "endDate" in market or market.get("events"), "no endDate or events fallback"
        if PolymarketSource._is_market_binary(market):
            saw_binary = True
            assert PolymarketSource._get_yes_token(market)
    assert saw_binary, "no binary market in the sample to exercise _get_yes_token"


# A long-closed, heavily traded Manifold market: resolved markets never change, so the detail and
# bets endpoints must keep returning the fields update() consumes for every published question.
_MANIFOLD_RESOLVED_MARKET_ID = (
    "YTIuuSsNRn2OlA4KykRM"  # "Will Biden be the 2024 Democratic Nominee?"
)


def test_manifold_market_detail_and_bets_contract():
    """Run Manifold's real market-detail + bets request methods against live data.

    Manifold's nightly update still calls both for every published question.
    """
    from sources.manifold import ManifoldSource

    src = ManifoldSource()
    try:
        market = src._get_market(_MANIFOLD_RESOLVED_MARKET_ID)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            pytest.skip(f"Manifold market {_MANIFOLD_RESOLVED_MARKET_ID} no longer exists")
        raise
    for field in (
        "question",
        "textDescription",
        "createdTime",
        "closeTime",
        "url",
        "isResolved",
        "resolutionTime",
        "resolution",
    ):
        assert field in market, f"Manifold market detail dropped consumed field {field!r}"
    assert market["isResolved"] is True

    bets = src._get_market_bets(_MANIFOLD_RESOLVED_MARKET_ID)
    assert isinstance(bets, list) and bets
    for bet in bets[:5]:
        assert "createdTime" in bet and "probAfter" in bet, "Manifold bet dropped a consumed field"


def test_yfinance_sp500_and_price_contract():
    """Run yfinance's real Wikipedia + Yahoo fetch methods against live data.

    Both are the real source methods: a Wikipedia table change yields no tickers, and a dropped
    ``Close`` column yields an empty / mis-shaped price frame — each caught below.
    """
    from sources.yfinance import YfinanceSource

    tickers = YfinanceSource._get_sp500_tickers()
    assert tickers, "S&P 500 Wikipedia scrape returned no tickers (table structure changed?)"

    prices = YfinanceSource._fetch_historical_prices("AAPL", "1mo")
    assert not prices.empty, "Yahoo returned no AAPL prices (the 'Close' column may be gone)"
    assert list(prices.columns) == ["date", "value"], "price frame columns drifted"


def test_metaculus_search_and_detail_contract():
    """Run Metaculus's real search (auth + filter fields) + detail request methods, live.

    The search method reads nr_forecasters/question/cp_reveal_time/id internally; the detail dict
    carries the fields ``update()`` indexes.
    """
    from sources.metaculus import MetaculusSource

    src = _keyed_source(MetaculusSource, "API_KEY_METACULUS")
    ids = src._call_search_endpoint(today=date.today())
    assert isinstance(ids, set)
    if not ids:
        pytest.skip("no metaculus questions matched the search filters")

    market = src._get_market(sorted(ids)[0])
    assert "title" in market, "Metaculus detail dropped 'title'"
    assert "resolved" in market, "Metaculus detail dropped 'resolved'"
    assert isinstance(market.get("question"), dict), "Metaculus detail 'question' is not a dict"
    # open_time / actual_close_time are indexed unconditionally in update(); keys must be present.
    for field in ("open_time", "actual_close_time"):
        assert field in market["question"], f"Metaculus detail question dropped {field!r}"
