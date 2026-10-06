"""Generate meta data for questions."""

import asyncio
import logging
import os
import ssl
import sys
import time

import functions_framework
import httpx2
import pandas as pd
from typesafe_sdk import AsyncTypeSafeClient, Choice
from typesafe_sdk.constants import DEFAULT_TIMEOUT as JEV_DEFAULT_TIMEOUT
from utils import gcp

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    keys,
    metadata_llm,
    metadata_prompts,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _get_category_single(index, row, semaphore):
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


async def _get_categories_async(dfq):
    """Run all category lookups concurrently."""
    semaphore = asyncio.Semaphore(50)

    tasks = [
        _get_category_single(index, row, semaphore)
        for index, row in dfq[dfq["category"] == ""].iterrows()
    ]

    return await asyncio.gather(*tasks)


def get_categories_from_llm(dfq):
    """Get category for questions using concurrent API calls."""
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


def get_client():
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


async def _classify_single(client, index, row, semaphore):
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


async def _classify_unfilled(dfq):
    """Run one Jev call per row that has no english value yet."""
    semaphore = asyncio.Semaphore(50)
    async with get_client() as client:
        tasks = [
            _classify_single(client, index, row, semaphore)
            for index, row in dfq[dfq["english"] == ""].iterrows()
        ]
        return await asyncio.gather(*tasks)


def assign_english(dfq, source):
    """Fill the `english` column of `dfq`.

    Dataset questions are written in English, so they get True with no call. Market
    questions with an empty `english` value are classified by Jev. Rows that already
    hold True or False are left alone.

    Args:
        dfq (pd.DataFrame): Questions for `source`, with `english` == "" where unknown.
        source (str): The question source.
    """
    from helpers import question_curation

    if source in question_curation.DATA_SOURCES:
        dfq["english"] = True
        return dfq

    n_to_classify = len(dfq[dfq["english"] == ""])
    logger.info(f"Classifying language of {n_to_classify} questions.")
    for index, english in asyncio.run(_classify_unfilled(dfq)):
        dfq.at[index, "english"] = english
    return dfq


@functions_framework.http
@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
    from helpers import question_curation

    local_filename = f"/tmp/{constants.META_DATA_FILENAME}"
    dfmeta = data_utils.download_and_read(
        filename=constants.META_DATA_FILENAME,
        local_filename=local_filename,
        df_tmp=pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
        dtype={},
    )
    if "category" not in dfmeta.columns:
        dfmeta["category"] = ""
    if "english" not in dfmeta.columns:
        dfmeta["english"] = ""

    for source, _ in question_curation.FREEZE_QUESTION_SOURCES.items():
        logger.info(f"Getting categories for {source} questions.")
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        dfq["source"] = source

        dfq = dfq.merge(dfmeta, on=["source", "id"], how="left").fillna("")
        dfq["category"] = dfq["category"].apply(
            lambda x: x if x in constants.QUESTION_CATEGORIES else ""
        )
        dfmeta = dfmeta[dfmeta["source"] != source]

        # Asign categories to some sources
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

        dfq = dfq[constants.META_DATA_FILE_COLUMNS]
        dfq = dfq[dfq["category"] != ""]
        dfmeta = pd.concat(
            [
                dfmeta,
                dfq,
            ],
            ignore_index=True,
        )

        # Upload after every source is finished to save work
        dfmeta.sort_values(by=["source", "id"], ignore_index=True, inplace=True)
        dfmeta.to_json(local_filename, lines=True, orient="records")
        logger.info(f"Uploading metadata for {source}.")
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
        )
        # Sleep to avoid cloud storage 429 rate limit error
        # Rate is 1 write/second to the same object
        # https://cloud.google.com/storage/quotas
        time.sleep(2)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
