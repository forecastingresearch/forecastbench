"""Generate meta data for questions to filter."""

import asyncio
import logging
import os
import sys
import time

import pandas as pd
from tqdm.asyncio import tqdm_asyncio

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    llm_prompts,
    model_eval,
    question_curation,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _validate_single_question(index, question, semaphore):
    """Validate a single question asynchronously."""
    async with semaphore:
        prompt = llm_prompts.VALIDATE_QUESTION_PROMPT.format(question=question)
        try:
            response = await asyncio.to_thread(
                model_eval.get_response_from_model,
                model_name=question_curation.METADATA_MODEL_NAME,
                prompt=prompt,
                max_tokens=500,
            )
            if "Classification:" not in response:
                logger.error(f"'Classification:' not in response for: {question}")
                return (index, None, question)

            end_resp = response.split("Classification:")[1]
            if "ok" in end_resp:
                return (index, True, question)
            elif "flag" in end_resp:
                logger.info(f"Ill-defined question: {question}")
                return (index, False, question)
            else:
                logger.error(f"Ambiguous response for: {question}")
                return (index, True, question)
        except Exception as e:
            logger.error(f"Error validating question: {e}")
            return (index, True, question)


async def _validate_questions_async(dfq):
    """Run all validations concurrently."""
    semaphore = asyncio.Semaphore(50)

    tasks = [
        _validate_single_question(index, row["question"], semaphore)
        for index, row in dfq[dfq["valid_question"] == ""].iterrows()
    ]

    return await tqdm_asyncio.gather(*tasks, desc="Validating questions")


def validate_questions(dfq):
    """Validate questions using concurrent API calls."""
    n_to_validate = len(dfq[dfq["valid_question"] == ""])
    logger.info(f"Validating {n_to_validate} questions.")
    results = asyncio.run(_validate_questions_async(dfq))

    invalid_questions = []
    for index, is_valid, question in results:
        if is_valid is None:
            dfq.loc[index, "valid_question"] = None
        else:
            dfq.loc[index, "valid_question"] = is_valid
            if not is_valid:
                invalid_questions.append(question)

    dfq.loc[:, "valid_question"] = dfq["valid_question"].apply(
        lambda x: x if x in [True, False] else ""
    )
    if invalid_questions:
        logger.warning(f"Invalid questions found:\n\n{'\n'.join(invalid_questions)}\n")
    return dfq


@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update question metadata in question bank."""
    local_filename = f"/tmp/{constants.META_DATA_FILENAME}"
    dfmeta = data_utils.download_and_read(
        filename=constants.META_DATA_FILENAME,
        local_filename=local_filename,
        df_tmp=pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
        dtype={},
    )
    if "valid_question" not in dfmeta.columns:
        dfmeta["valid_question"] = ""

    n_total_invalid = 0
    for source, _ in question_curation.FREEZE_QUESTION_SOURCES.items():
        logger.info(f"Validating {source} questions.")
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        dfq["source"] = source

        dfq = dfq.merge(dfmeta, on=["source", "id"], how="left").fillna("")
        dfmeta = dfmeta[dfmeta["source"] != source]

        if source in question_curation.DATA_SOURCES + [
            "infer",
            "metaculus",
        ]:
            dfq["valid_question"] = True
        else:
            dfq = validate_questions(dfq)

        dfq = dfq[constants.META_DATA_FILE_COLUMNS]
        dfq = dfq[dfq["valid_question"] != ""]
        dfq["valid_question"] = dfq["valid_question"].astype(bool)
        dfmeta = pd.concat(
            [
                dfmeta,
                dfq,
            ],
            ignore_index=True,
        )
        n_false = len(dfq[~dfq["valid_question"]])
        n_total_invalid += n_false

        # Upload after every source is finished to save work
        dfmeta.sort_values(by=["source", "id"], ignore_index=True, inplace=True)
        dfmeta.to_json(local_filename, lines=True, orient="records")
        logger.info(f"Uploading metadata for {source}. Total of {n_false} invalid questions.")
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
        )
        # Sleep to avoid cloud storage 429 rate limit error
        # Rate is 1 write/second to the same object
        # https://cloud.google.com/storage/quotas
        time.sleep(2)

    logger.info(f"Total of {n_total_invalid} invalid questions.")
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
