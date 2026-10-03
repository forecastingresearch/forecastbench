"""Registry-derived source lists for the parametrized contract and integration tests.

Every registry source is in exactly one list, pinned by ``contract/test_registry_coverage.py``:
``IMPLEMENTED_SOURCES`` (real fetch and update drivers; listed explicitly, so a new source fails
the parametrized tests until it is added), ``UPDATE_ONLY_SOURCES`` (no longer fetched, still
updated nightly) and ``RETIRED_SOURCES`` (frozen question file; the update job is a no-op). The
lists come from source metadata, so importing this module calls no source.
"""

from sources._metadata import SOURCE_METADATA
from sources.registry import SOURCES

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
