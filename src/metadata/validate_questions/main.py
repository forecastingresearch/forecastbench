"""Generate meta data for questions to filter."""

import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    llm_prompts,
    model_eval,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))  # noqa: E402
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_questions(dfq):
    """Validate question with gpt 3.5-turbo."""
    invalid_questions = []
    for index, row in dfq[dfq["valid_question"] == ""].iterrows():
        question = row["question"]
        prompt = llm_prompts.VALIDATE_QUESTION_PROMPT.format(question=question)
        try:
            response = model_eval.get_response_from_model(
                model_name="gpt-3.5-turbo-0125", prompt=prompt, max_tokens=500, temperature=0
            )
            if "Classification:" not in response:
                logger.error(f"'Classification:' is not in the response for question: {question}")
                dfq.loc[index, "valid_question"] = None
            else:
                end_resp = response.split("Classification:")[1]
                if "ok" in end_resp:
                    dfq.loc[index, "valid_question"] = True
                elif "flag" in end_resp:
                    dfq.loc[index, "valid_question"] = False
                    invalid_questions += [question]
                    logger.info(f"The following question is ill-defined: {question}")
                else:
                    dfq.loc[index, "valid_question"] = True
                    logger.error(f"Ambiguous response for question: {question}")
        except Exception as e:
            logger.error(f"Error in assign_category: {e}")
            dfq.loc[index, "valid_question"] = True
    dfq.loc[:, "valid_question"] = dfq["valid_question"].apply(
        lambda x: x if x in [True, False] else ""
    )
    if invalid_questions:
        logger.warning(f"Invalid_questions found:\n\n{'\n'.join(invalid_questions)}\n")
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
    for source, _ in constants.FREEZE_QUESTION_SOURCES.items():
        logger.info(f"Validating {source} questions.")
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        dfq["source"] = source

        dfq = dfq.merge(dfmeta, on=["source", "id"], how="left").fillna("")
        dfmeta = dfmeta[dfmeta["source"] != source]

        if source in constants.DATA_SOURCES + [
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
    logger.info(f"Total of {n_total_invalid} invalid questions.")
    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
