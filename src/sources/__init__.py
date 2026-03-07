"""Source registry — one instance per question source."""

from _types import SourceType

from .acled import AcledSource
from .dbnomics import DbnomicsSource
from .fred import FredSource
from .infer import InferSource
from .manifold import ManifoldSource
from .metaculus import MetaculusSource
from .polymarket import PolymarketSource
from .wikipedia import WikipediaSource
from .yfinance import YfinanceSource

# Singletons
_acled = AcledSource()
_dbnomics = DbnomicsSource()
_fred = FredSource()
_infer = InferSource()
_manifold = ManifoldSource()
_metaculus = MetaculusSource()
_polymarket = PolymarketSource()
_wikipedia = WikipediaSource()
_yfinance = YfinanceSource()

SOURCES = {
    s.name: s
    for s in [
        _acled,
        _dbnomics,
        _fred,
        _infer,
        _manifold,
        _metaculus,
        _polymarket,
        _wikipedia,
        _yfinance,
    ]
}

DATA_SOURCES = {
    name: src for name, src in sorted(SOURCES.items()) if src.source_type == SourceType.DATA
}
MARKET_SOURCES = {
    name: src for name, src in sorted(SOURCES.items()) if src.source_type == SourceType.MARKET
}

ALL_SOURCE_NAMES = sorted(SOURCES.keys())
DATA_SOURCE_NAMES = sorted(DATA_SOURCES.keys())
MARKET_SOURCE_NAMES = sorted(MARKET_SOURCES.keys())
