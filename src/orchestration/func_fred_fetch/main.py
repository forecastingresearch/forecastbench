"""FRED fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, keys
from orchestration import _source_io
from sources.fred import FredSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "fred"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch FRED questions and upload to question bank."""
    source = FredSource()
    source.api_key = keys.API_KEY_FRED

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    dff = source.fetch(dfq=dfq)

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
