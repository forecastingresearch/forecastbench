"""INFER fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, keys
from orchestration import _source_io
from sources.infer import InferSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "infer"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch INFER questions and upload to question bank."""
    source = InferSource()
    source.api_key = keys.API_KEY_INFER

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)
    existing_resolution_ids = _source_io.list_existing_resolution_ids(SOURCE)

    dff = source.fetch(dfq=dfq, existing_resolution_ids=existing_resolution_ids)

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
