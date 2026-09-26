"""SerpAPI update entry point."""

import logging
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.serpapi import SerpapiSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "serpapi"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update SerpAPI questions while retaining their collected history."""
    source = SerpapiSource()
    # Before the first run, create an empty serpapi_questions.jsonl in the question-bank bucket.
    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )
    existing = _source_io.load_existing_resolution_files(
        SOURCE, ids=dff["id"].unique(), strict=True
    )
    result = source.update(dfq, dff, existing_resolution_files=existing)
    if result.resolution_files:
        _source_io.upload_resolution_files(
            SOURCE, result.resolution_files, extra_columns=("fetch_datetime",)
        )
    data_utils.upload_questions(result.dfq, SOURCE)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
