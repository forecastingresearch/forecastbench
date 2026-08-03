"""INFER update entry point."""

import logging
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.infer import InferSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Run the (currently no-op) update for INFER.

    INFER is no longer fetched, so this does not read a fetch file. Until INFER questions
    are resolved by LLM or by hand, ``update`` returns no changes and nothing is uploaded.
    """
    source = InferSource()
    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    result = source.update(dfq)

    if result.resolution_files:
        logger.info("Uploading resolution files to GCP...")
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
