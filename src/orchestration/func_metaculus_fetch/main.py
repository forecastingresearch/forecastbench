"""Metaculus fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import decorator, keys
from orchestration import _source_io
from sources.metaculus import MetaculusSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "metaculus"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Metaculus question IDs and upload to question bank."""
    source = MetaculusSource()
    source.api_key = keys.API_KEY_METACULUS

    dff = source.fetch()

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
