"""ACLED update entry point."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from helpers import data_utils, decorator
from orchestration import _io
from sources.acled import FETCH_COLUMN_DTYPE, FETCH_COLUMNS, AcledSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "acled"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Pull in fetched data and update questions in the question bank."""
    logger.info("Downloading previously-fetched ACLED data from Cloud.")
    source = AcledSource()
    source.populate_hash_mapping(_io.load_hash_mapping(SOURCE))

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    # Read the fetch file with explicit dtypes, replicating the legacy read: event_date must
    # stay a string so the year-prefix fix inside update() can apply.
    filenames = data_utils.generate_filenames(SOURCE)
    dff = data_utils.download_and_read(
        filename=filenames["jsonl_fetch"],
        local_filename=filenames["local_fetch"],
        df_tmp=pd.DataFrame(columns=FETCH_COLUMNS),
        dtype=FETCH_COLUMN_DTYPE,
    )

    result = source.update(dfq, dff)

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.hash_mapping is not None:
        _io.upload_hash_mapping(source.dump_hash_mapping(), SOURCE)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
