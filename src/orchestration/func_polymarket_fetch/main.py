"""Polymarket fetch entry point."""

from __future__ import annotations

import logging
import os
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.polymarket import PolymarketSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "polymarket"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Polymarket questions and upload to question bank."""
    source = PolymarketSource()

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    # Off by default: only load resolved questions' resolution files when the completeness check
    # is enabled, to avoid downloading the whole resolved backlog on every run.
    existing_resolution_files = None
    if os.environ.get("CHECK_AND_FIX_RESOLVED_DATA"):
        resolved_ids = dfq.loc[dfq["resolved"], "id"].astype(str).tolist() if not dfq.empty else []
        existing_resolution_files = _source_io.load_existing_resolution_files(
            SOURCE, ids=resolved_ids
        )

    dff = source.fetch(dfq=dfq, existing_resolution_files=existing_resolution_files)

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
