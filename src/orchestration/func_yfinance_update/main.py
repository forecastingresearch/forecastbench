"""Yfinance update entry point."""

from __future__ import annotations

import logging
import os
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.yfinance import YfinanceSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update Yahoo Finance questions and resolution files."""
    overwrite_price_history = os.environ.get("OVERWRITE_PRICE_HISTORY", "").lower() in ("1", "true")
    if overwrite_price_history:
        logger.info("OVERWRITE_PRICE_HISTORY is set. Re-fetching all resolution data.")

    source = YfinanceSource()

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    # Load existing resolution files for fetched tickers plus the renamed-ticker originals, whose
    # files are rebuilt from their replacement symbols inside update().
    rename_originals = [entry["original_ticker"] for entry in source.ticker_renames]
    ids_to_load = sorted(set(dff["id"].astype(str)) | set(rename_originals))
    existing_resolution_files = _source_io.load_existing_resolution_files(SOURCE, ids=ids_to_load)

    result = source.update(
        dfq,
        dff,
        existing_resolution_files=existing_resolution_files,
        overwrite_price_history=overwrite_price_history,
    )

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
