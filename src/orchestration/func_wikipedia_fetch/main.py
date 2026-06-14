"""Wikipedia fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import decorator
from orchestration import _source_io
from sources.wikipedia import WikipediaSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "wikipedia"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Wikipedia data and store in GCP Cloud Storage."""
    source = WikipediaSource()

    fetch_result = source.fetch()
    if not fetch_result:
        logger.error("No Wikipedia data was downloaded.")
        return

    _source_io.write_wikipedia_fetch_output(fetch_result)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
