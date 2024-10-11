"""Generate meta data for questions."""

import logging
import os
import sys
import time

import functions_framework
import pandas as pd

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


def get_categories_from_llm(dfq):
    """Get category for question from gpt 3.5-turbo."""
    for index, row in dfq[dfq["category"] == ""].iterrows():
        prompt = llm_prompts.ASSIGN_CATEGORY_PROMPT.format(
            question=row["question"], background=row["background"]
        )
        try:
            response = model_eval.get_response_from_model(
                model_name="gpt-3.5-turbo-0125", prompt=prompt, max_tokens=50, temperature=0
            )
            category = response.strip('"').strip("'").strip(" ").strip(".")
            logger.info(f"Got category for {row['source']}: {row['id']} --> {category}")
            dfq.at[index, "category"] = (
                category if category in constants.QUESTION_CATEGORIES else "Other"
            )
        except Exception as e:
            logger.error(f"Error in assign_category: {e}")
            dfq.at[index, "category"] = "Other"
    return dfq


@functions_framework.http
@decorator.log_runtime
def driver(_):
    """Pull in fetched data and update questions and resolved values in question bank."""
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
        elif source in ["yfinance", "fred"]:
            dfq["category"] = "Economics & Business"
        else:
            dfq = get_categories_from_llm(dfq)

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
        time.sleep(1)

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
