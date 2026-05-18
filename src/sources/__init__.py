"""Source metadata re-exports. Lightweight — no concrete source imports.

For source instances:
- import from ``sources.registry`` for all sources (heavyweight, use cautiously)
- import from the specific source module for a single source (lighter weight)
  e.g. ``from sources.infer import InferSource``
"""

from ._metadata import (  # noqa: F401
    ALL_SOURCE_NAMES,
    DATASET_SOURCE_NAMES,
    MARKET_SOURCE_NAMES,
    SOURCE_METADATA,
)
