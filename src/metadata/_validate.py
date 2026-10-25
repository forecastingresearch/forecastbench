"""Validate questions: mark each question in the question bank as valid or invalid.

Dataset sources and Metaculus are valid without a call; the other sources are judged by the
metadata LLM. Rows that already hold a verdict keep it. Pure of GCS: frames in, frames out.
"""

import asyncio
import logging

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from tqdm.asyncio import tqdm_asyncio

from _schemas import MetadataFrame, QuestionFrame
from helpers import metadata_llm, metadata_prompts, question_curation

logger = logging.getLogger(__name__)

# Sources whose questions are valid by construction, so no LLM call is made for them.
AUTO_VALID_SOURCES: list[str] = question_curation.DATA_SOURCES + ["metaculus"]


async def _validate_single_question(
    index: int, question: str, semaphore: asyncio.Semaphore
) -> tuple[int, bool | None, str]:
    """Validate a single question asynchronously."""
    async with semaphore:
        prompt = metadata_prompts.VALIDATE_QUESTION_PROMPT.format(question=question)
        try:
            response = await asyncio.to_thread(
                metadata_llm.get_metadata_model_response,
                prompt=prompt,
                max_output_tokens=1024,
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


async def _validate_questions_async(dfq: pd.DataFrame) -> list[tuple[int, bool | None, str]]:
    """Run all validations concurrently."""
    semaphore = asyncio.Semaphore(50)

    tasks = [
        _validate_single_question(index, row["question"], semaphore)
        for index, row in dfq[dfq["valid_question"] == ""].iterrows()
    ]

    return await tqdm_asyncio.gather(*tasks, desc="Validating questions")


def _validate_via_llm(dfq: pd.DataFrame) -> pd.DataFrame:
    """Fill the empty `valid_question` values of `dfq` with the metadata LLM's verdicts.

    A response without the "Classification:" marker leaves the row empty, so it is dropped
    by `validate()` and retried on the next run.

    Args:
      dfq (pd.DataFrame): One source's questions merged with their metadata.
    """
    n_to_validate = len(dfq[dfq["valid_question"] == ""])
    logger.info(f"Validating {n_to_validate} questions.")
    # Fetch the API keys once here; the cache has no lock, so the first batch of
    # concurrent calls would each fetch every secret from Secret Manager.
    metadata_llm._get_metadata_model_run()
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


@pa.check_types
def validate(
    dfmeta: DataFrame[MetadataFrame], dfq: DataFrame[QuestionFrame], source: str
) -> DataFrame[MetadataFrame]:
    """Validate the questions of `source` and merge them into the question metadata.

    Every question in `dfq` gets a verdict. A row that already holds one in `dfmeta` keeps
    it. Questions of an auto-valid source are valid; the rest are judged by the metadata LLM,
    and a question the LLM gave no verdict for is left out so the next run retries it.
    Questions `dfmeta` has not seen yet carry the empty-string placeholder in the columns
    the tag job fills. The rows `dfmeta` holds for `source` are replaced by the validated
    rows of `dfq`, so a question that left the question file leaves the metadata too. Rows
    of other sources are untouched.

    Args:
      dfmeta (DataFrame[MetadataFrame]): The question metadata, any sources.
      dfq (DataFrame[QuestionFrame]): The source's question file.
      source (str): The source name of the questions in `dfq`.

    Returns:
      DataFrame[MetadataFrame]: The question metadata with the rows of `source` validated,
        sorted by source and id.
    """
    dfq = dfq.assign(source=source)

    dfq = dfq.merge(dfmeta, on=["source", "id"], how="left").fillna("")
    dfmeta = dfmeta[dfmeta["source"] != source]

    if source in AUTO_VALID_SOURCES:
        dfq["valid_question"] = True
    else:
        dfq = _validate_via_llm(dfq)

    dfq = dfq[list(MetadataFrame.to_schema().columns)]
    dfq = dfq[dfq["valid_question"] != ""]
    dfmeta = pd.concat(
        [
            dfmeta,
            dfq,
        ],
        ignore_index=True,
    )
    dfmeta.sort_values(by=["source", "id"], ignore_index=True, inplace=True)
    return dfmeta
