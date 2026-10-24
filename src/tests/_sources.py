"""Registry-derived source lists for parametrized contract/integration tests.

Every registry source belongs to exactly one of three buckets; the partition is pinned by
``contract/test_registry_coverage.py``:

- ``IMPLEMENTED_SOURCES`` — a real ``fetch()`` **and** ``update()`` plus ``func_<name>_fetch`` /
  ``func_<name>_update`` orchestration drivers. Listed explicitly so that adding a registry source
  has to be reflected here before the parametrized contract and driver tests will pass.
- ``UPDATE_ONLY_SOURCES`` — no longer fetched (``run_fetch`` is False) but still updated nightly:
  ``fetch()`` refuses, while the real ``update()`` keeps the already-published questions' market
  values and resolution status current so they can still be resolved. Only an update driver
  exists.
- ``RETIRED_SOURCES`` — nothing fetches or writes them any more (``dfq_is_frozen`` is True): the
  update job is a no-op and they stay in the registry only so their published questions resolve.

Detection is static (metadata), never a runtime call, so importing this module never hits the
network.
"""

from sources._metadata import SOURCE_METADATA
from sources.registry import SOURCES

# Sources with a real fetch() + update() on the source base class.
IMPLEMENTED_SOURCES = [
    "acled",
    "dbnomics",
    "fred",
    "kalshi",
    "metaculus",
    "polymarket",
    "serpapi",
    "wikipedia",
    "yfinance",
]


def update_only_sources() -> list[str]:
    """Return registry sources that are still updated nightly but no longer fetched."""
    return sorted(
        name
        for name in SOURCES
        if not SOURCE_METADATA[name]["run_fetch"]
        and SOURCE_METADATA[name]["run_update"]
        and not SOURCE_METADATA[name]["dfq_is_frozen"]
    )


def retired_sources() -> list[str]:
    """Return registry sources whose question file is frozen: nothing fetches or writes them."""
    return sorted(name for name in SOURCES if SOURCE_METADATA[name]["dfq_is_frozen"])


UPDATE_ONLY_SOURCES = update_only_sources()
RETIRED_SOURCES = retired_sources()
