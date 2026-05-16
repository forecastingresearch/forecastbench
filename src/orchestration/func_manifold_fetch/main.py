"""Manifold fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import decorator
from orchestration import _source_io
from sources.manifold import ManifoldSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "manifold"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Manifold market IDs and upload to question bank."""
    source = ManifoldSource()

    dff = source.fetch()

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
