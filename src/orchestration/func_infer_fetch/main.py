"""INFER fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, env, keys
from orchestration import _source_io
from sources import SOURCES
from utils import gcp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch INFER questions and upload to question bank."""
    source = SOURCES[SOURCE]
    source.api_key = keys.API_KEY_INFER

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)
    files_in_storage = gcp.storage.list_with_prefix(
        bucket_name=env.QUESTION_BANK_BUCKET, prefix=SOURCE
    )

    dff = source.fetch(dfq=dfq, files_in_storage=files_in_storage)

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
