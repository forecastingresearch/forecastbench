"""Wikipedia update entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator
from orchestration import _io, _source_io
from sources.wikipedia import WikipediaSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "wikipedia"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Pull in fetched data and update questions and resolution values in the question bank."""
    logger.info("Downloading previously-fetched Wikipedia data from Cloud.")
    source = WikipediaSource()
    source.populate_hash_mapping(_io.load_hash_mapping(SOURCE))

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)
    dff = _source_io.read_wikipedia_fetch_files()

    result = source.update(dfq, dff)

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    if result.hash_mapping is not None:
        _io.upload_hash_mapping(source.dump_hash_mapping(), SOURCE)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
