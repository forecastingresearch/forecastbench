"""Manifold update entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.manifold import ManifoldSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "manifold"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update Manifold questions and resolution files."""
    source = ManifoldSource()

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    logger.info("Loading existing resolution files...")
    # No ids= filter: load ALL existing resolution files so that we
    # matching the old non-refactored behaviour.
    # TODO: we can drop pre-benchmark start date history/ids.
    existing_resolution_files = _source_io.load_existing_resolution_files(SOURCE)
    logger.info(f"Loaded {len(existing_resolution_files)} resolution files")

    existing_resolution_ids = _source_io.list_existing_resolution_ids(SOURCE)

    result = source.update(
        dfq,
        dff,
        existing_resolution_files=existing_resolution_files,
        existing_resolution_ids=existing_resolution_ids,
    )

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
