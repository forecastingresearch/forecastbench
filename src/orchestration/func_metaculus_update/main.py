"""Metaculus update entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, env, keys
from orchestration import _source_io
from sources.metaculus import MetaculusSource
from utils import gcp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "metaculus"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update Metaculus questions and resolution files."""
    source = MetaculusSource()
    source.api_key = keys.API_KEY_METACULUS

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    logger.info("Loading existing resolution files...")
    existing_resolution_files = _source_io.load_existing_resolution_files(SOURCE)
    logger.info(f"Loaded {len(existing_resolution_files)} resolution files")

    files_in_storage = gcp.storage.list_with_prefix(
        bucket_name=env.QUESTION_BANK_BUCKET, prefix=SOURCE
    )

    result = source.update(
        dfq,
        dff,
        existing_resolution_files=existing_resolution_files,
        files_in_storage=files_in_storage,
    )

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
