"""Tag questions: assign a category to every question of a source and classify its language."""

import asyncio
import logging
import ssl

import httpx2
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from typesafe_sdk import AsyncTypeSafeClient, Choice
from typesafe_sdk.constants import DEFAULT_TIMEOUT as JEV_DEFAULT_TIMEOUT

from _schemas import MetadataFrame, QuestionFrame
from helpers import constants, keys, metadata_llm, metadata_prompts, question_curation

logger = logging.getLogger(__name__)


async def _get_category_single(
    index: int, row: pd.Series, semaphore: asyncio.Semaphore
) -> tuple[int, str]:
    """Get category for a single question asynchronously."""
    async with semaphore:
        prompt = metadata_prompts.ASSIGN_CATEGORY_PROMPT.format(
            question=row["question"], background=row["background"]
        )
        try:
            response = await asyncio.to_thread(
                metadata_llm.get_metadata_model_response,
                prompt=prompt,
                max_output_tokens=512,
            )
            category = response.strip('"').strip("'").strip(" ").strip(".")
            logger.info(
                f"Got category for {row['source']}: {row['id']} {row['question']}--> {category}"
            )
            valid_category = category if category in constants.QUESTION_CATEGORIES else "Other"
            return (index, valid_category)
        except Exception as e:
            logger.error(f"Error in assign_category: {e}")
            return (index, "Other")


async def _get_categories_async(dfq: pd.DataFrame) -> list[tuple[int, str]]:
    """Run all category lookups concurrently."""
    semaphore = asyncio.Semaphore(50)

    tasks = [
        _get_category_single(index, row, semaphore)
        for index, row in dfq[dfq["category"] == ""].iterrows()
    ]

    return await asyncio.gather(*tasks)


def get_categories_from_llm(dfq: pd.DataFrame) -> pd.DataFrame:
    """Get category for questions using concurrent API calls.

    Args:
      dfq (pd.DataFrame): Questions with `category` == "" where a category is still needed.
    """
    n_to_tag = len(dfq[dfq["category"] == ""])
    logger.info(f"Tagging {n_to_tag} questions.")
    # Fetch the API keys once here; the cache has no lock, so the first batch of
    # concurrent calls would each fetch every secret from Secret Manager.
    metadata_llm._get_metadata_model_run()
    results = asyncio.run(_get_categories_async(dfq))

    for index, category in results:
        dfq.at[index, "category"] = category

    return dfq


LANGUAGE_QUESTION = Choice(
    instructions=(
        "What language is the text written in? Judge the sentences themselves. Ignore names,"
        " titles, and quoted words in other languages that appear inside them."
    ),
    criteria={
        "english": (
            "The sentences are written in English, even if they mention a name or a title in"
            " another language."
        ),
        "not_english": "The sentences are written in a language other than English.",
    },
)


def get_client() -> AsyncTypeSafeClient:
    """Return a Jev client that authenticates with the key from Secret Manager.

    The client gets an httpx2 client built on Python's default SSL context. httpx2 would
    otherwise use truststore, whose async handshake path is not thread-safe on Linux in
    0.10.4 and corrupts the OpenSSL heap when many connections open at once
    (sethmlarson/truststore#221). The SDK closes this httpx2 client with its own.
    """
    http_client = httpx2.AsyncClient(
        verify=ssl.create_default_context(), timeout=JEV_DEFAULT_TIMEOUT
    )
    return AsyncTypeSafeClient(api_key=keys.API_KEY_TYPESAFE, http_client=http_client)


async def _classify_single(
    client: AsyncTypeSafeClient, index: int, row: pd.Series, semaphore: asyncio.Semaphore
) -> tuple[int, bool | str]:
    """Classify one question. Return "" when the call fails so the next run retries it."""
    async with semaphore:
        try:
            response = await client.system_one(
                state={"question": row["question"], "background": row["background"]},
                questions={"language": LANGUAGE_QUESTION},
            )
            answer = response.choices["language"]
            if answer.choice != "english":
                logger.info(
                    f"Not English (p={answer.probabilities['not_english']:.2f}) for"
                    f" {row['source']}: {row['id']} {row['question']}"
                )
            return (index, answer.choice == "english")
        except Exception as e:
            logger.error(f"Error classifying language for {row['source']}: {row['id']}: {e}")
            return (index, "")


async def _classify_unfilled(dfq: pd.DataFrame) -> list[tuple[int, bool | str]]:
    """Run one Jev call per row that has no english value yet."""
    semaphore = asyncio.Semaphore(50)
    async with get_client() as client:
        tasks = [
            _classify_single(client, index, row, semaphore)
            for index, row in dfq[dfq["english"] == ""].iterrows()
        ]
        return await asyncio.gather(*tasks)


def assign_english(dfq: pd.DataFrame, source: str) -> pd.DataFrame:
    """Fill the `english` column of `dfq`.

    Dataset questions are written in English, so they get True with no call. Market
    questions with an empty `english` value are classified by Jev; when the call fails the
    value stays "" and the next run asks again. Rows that already hold True or False are
    left alone.

    Args:
      dfq (pd.DataFrame): Questions for `source`, with `english` == "" where unknown.
      source (str): The question source.
    """
    if source in question_curation.DATA_SOURCES:
        dfq["english"] = True
        return dfq

    n_to_classify = len(dfq[dfq["english"] == ""])
    logger.info(f"Classifying language of {n_to_classify} questions.")
    for index, english in asyncio.run(_classify_unfilled(dfq)):
        dfq.at[index, "english"] = english
    return dfq


@pa.check_types
def tag(
    dfmeta: DataFrame[MetadataFrame], dfq: DataFrame[QuestionFrame], source: str
) -> DataFrame[MetadataFrame]:
    """Tag the questions of `source` and merge them into the question metadata.

    Every question in `dfq` gets a category. Its English flag is True for a dataset source
    and Jev's answer otherwise; when the Jev call fails the flag stays "" and the next run
    asks again. A row already tagged in `dfmeta` keeps its values; a category that is not
    in `constants.QUESTION_CATEGORIES` is assigned again. The rows `dfmeta` holds for
    `source` are replaced by the tagged rows of `dfq`, so a question that left the question
    file leaves the metadata too. Rows of other sources are untouched. The category of
    acled, fred, yfinance and serpapi questions is fixed; the other sources are tagged by
    the metadata LLM.

    Both frames are validated against their schema on the way in, and the result on the way
    out, so a malformed metadata file or question file fails the job instead of being written.

    Args:
      dfmeta (DataFrame[MetadataFrame]): The question metadata.
      dfq (DataFrame[QuestionFrame]): The question file of `source`.
      source (str): The question source.

    Returns:
      DataFrame[MetadataFrame]: The question metadata with the rows of `source` tagged, sorted
        by source and id.
    """
    dfq = dfq.assign(source=source)

    dfq = dfq.merge(dfmeta, on=["source", "id"], how="left").fillna("")
    dfq["category"] = dfq["category"].apply(
        lambda x: x if x in constants.QUESTION_CATEGORIES else ""
    )
    dfmeta = dfmeta[dfmeta["source"] != source]

    if source == "acled":
        dfq["category"] = "Security & Defense"
    elif source == "fred":
        dfq["category"] = "Economics & Business"
    elif source == "yfinance":
        dfq["category"] = "Economics & Business"
    elif source == "serpapi":
        dfq["category"] = "Economics & Business"
    else:
        dfq = get_categories_from_llm(dfq)

    dfq = assign_english(dfq, source)

    dfq = dfq[list(MetadataFrame.to_schema().columns)]
    dfq = dfq[dfq["category"] != ""]
    dfmeta = pd.concat(
        [
            dfmeta,
            dfq,
        ],
        ignore_index=True,
    )
    dfmeta.sort_values(by=["source", "id"], ignore_index=True, inplace=True)
    return dfmeta
