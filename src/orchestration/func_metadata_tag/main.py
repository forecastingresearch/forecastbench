"""Tag questions entry point."""

import logging
import time
from typing import Any

from helpers import data_utils, decorator, question_curation
from metadata import tag
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Tag the questions of every sampled source, uploading the metadata after each one."""
    dfmeta = _io.load_question_metadata()

    for source in question_curation.FREEZE_QUESTION_SOURCES:
        logger.info(f"Getting categories for {source} questions.")
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        dfmeta = tag(dfmeta, dfq, source)

        # Upload after every source is finished to save work
        logger.info(f"Uploading metadata for {source}.")
        _io.upload_question_metadata(dfmeta)
        # Sleep to avoid cloud storage 429 rate limit error
        # Rate is 1 write/second to the same object
        # https://cloud.google.com/storage/quotas
        time.sleep(2)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
