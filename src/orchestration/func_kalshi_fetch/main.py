"""Kalshi fetch entry point."""

import logging
from typing import Any

from helpers import decorator
from orchestration import _source_io
from sources.kalshi import KalshiSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "kalshi"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Kalshi market tickers and upload to question bank."""
    source = KalshiSource()

    dff = source.fetch()

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
