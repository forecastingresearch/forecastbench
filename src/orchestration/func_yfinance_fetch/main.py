"""Yfinance fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.yfinance import YfinanceSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Yahoo Finance stock data and upload to question bank."""
    source = YfinanceSource()

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    dff = source.fetch(dfq=dfq)

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
