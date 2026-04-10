"""INFER update entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, keys
from orchestration import _source_io
from sources import SOURCES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update INFER questions and resolution files."""
    source = SOURCES[SOURCE]
    source.api_key = keys.API_KEY_INFER

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )
    existing_resolution_files = _source_io.load_existing_resolution_files(SOURCE)

    result = source.update(dfq, dff, existing_resolution_files=existing_resolution_files)

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
