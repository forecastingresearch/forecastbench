"""Validate questions entry point."""

import logging
import time
from typing import Any

from helpers import data_utils, decorator, question_curation
from metadata import validate
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Validate the questions of every sampled source and update the question metadata."""
    dfmeta = _io.load_question_metadata()

    n_total_invalid = 0
    for source in question_curation.FREEZE_QUESTION_SOURCES:
        logger.info(f"Validating {source} questions.")
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )

        dfmeta = validate(dfmeta, dfq, source)
        n_false = int(dfmeta.loc[dfmeta["source"] == source, "valid_question"].eq(False).sum())
        n_total_invalid += n_false

        # Upload after every source is finished to save work
        logger.info(f"Uploading metadata for {source}. Total of {n_false} invalid questions.")
        _io.upload_question_metadata(dfmeta)
        # Sleep to avoid cloud storage 429 rate limit error
        # Rate is 1 write/second to the same object
        # https://cloud.google.com/storage/quotas
        time.sleep(2)

    logger.info(f"Total of {n_total_invalid} invalid questions.")
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
