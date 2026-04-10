"""Source registry — imports all source modules. Heavy deps. Use sparringly.

Only import from here if you need source instances (e.g. func_resolve).
For metadata, use `from sources import xxx` or for individual sources
use `from sources.infer import InferSource`
"""

from _fb_types import SourceType

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

DATASET_SOURCES = {
    name: src for name, src in sorted(SOURCES.items()) if src.source_type == SourceType.DATASET
}
MARKET_SOURCES = {
    name: src for name, src in sorted(SOURCES.items()) if src.source_type == SourceType.MARKET
}
