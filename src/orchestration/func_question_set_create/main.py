"""Create question set entry point."""

import logging
from typing import Any

import numpy as np
import pandas as pd

from helpers import constants, data_utils, decorator, env, question_curation
from orchestration import _io
from question_sampling import QuestionSetTarget, create_question_set
from sources.registry import SOURCES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Create the LLM and human question sets and upload them."""
    if not env.RUNNING_LOCALLY and not question_curation.is_today_question_curation_date():
        logger.info("Today is NOT the question set creation date.")
        return

    dfmeta = data_utils.download_and_read(
        filename=constants.META_DATA_FILENAME,
        local_filename=f"/tmp/{constants.META_DATA_FILENAME}",
        df_tmp=pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
        dtype=constants.META_DATA_FILE_COLUMN_DTYPE,
    )

    sources = {name: SOURCES[name] for name in question_curation.FREEZE_QUESTION_SOURCES}
    # ACLED reads its question types from its hash mapping, see `BaseSource.is_retired()`.
    for name, source in sources.items():
        source.populate_hash_mapping(_io.load_hash_mapping(name))
    sampling_shares = {
        name: config["sampling_share"]
        for name, config in question_curation.FREEZE_QUESTION_SOURCES.items()
    }
    questions = {
        name: data_utils.get_data_from_cloud_storage(source=name, return_question_data=True)
        for name in sources
    }

    # For testing/reproducibility only: when `env.RANDOM_SEED` is set, thread a single RandomState
    # through both sampling passes so the sampled set is deterministic. It is unset in deployment,
    # which preserves the historical unseeded behaviour.
    seed = env.RANDOM_SEED
    random_state = None if seed is None else np.random.RandomState(seed)

    result = create_question_set(
        dfmeta=dfmeta,
        questions=questions,
        sources=sources,
        sampling_shares=sampling_shares,
        freeze_datetime=question_curation.FREEZE_DATETIME,
        forecast_datetime=question_curation.FORECAST_DATETIME,
        random_state=random_state,
        show_plots=env.RUNNING_LOCALLY,
    )

    for target, question_set in (
        (QuestionSetTarget.LLM, result.llm_question_set),
        (QuestionSetTarget.HUMAN, result.human_question_set),
    ):
        filename = question_set["question_set"]
        local_filename = f"/tmp/{filename}"
        _io.write_question_set_file(local_filename, question_set)
        if not env.RUNNING_LOCALLY:
            _io.upload_question_set(local_filename, filename=filename, target=target.value)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
