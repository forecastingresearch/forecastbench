"""Freeze forecasting questions."""

import json
import logging
import math
import os
import random
import sys
from copy import deepcopy
from itertools import combinations

import numpy as np
import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    question_prompts,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fill_questions(QUESTIONS, PROCESSED_QUESTIONS, num_found, num_questions_needed):
    """Ensure we select `num_questions_needed` questions.

    If we were unable to get the number of questions the first time through in `process_questions`,
    it means that some source does not have enough questions. This function will choose random
    single questions from sources until it has obtained 'num_questions_needed`.
    """
    logger.error("Should not end up in this function.")
    giveup_count = 0
    keys = list(QUESTIONS.keys())
    while num_found < num_questions_needed and giveup_count < 50:
        giveup_count += 1
        source = random.choice(keys)
        if len(PROCESSED_QUESTIONS[source]["dfq"]) < len(QUESTIONS[source]["dfq"]):
            df_tmp = QUESTIONS[source]["dfq"].drop(PROCESSED_QUESTIONS[source]["dfq"].index)
            if not df_tmp.empty:
                PROCESSED_QUESTIONS[source]["dfq"] = pd.concat(
                    [PROCESSED_QUESTIONS[source]["dfq"], df_tmp.sample(1)]
                )


def process_questions(QUESTIONS, TO_QUESTIONS, single_generation_func, combo_generation_func):
    """Sample from `QUESTIONS` to get the number of questions needed.

    This function works for both the LLM question set and the human forecaster question set.
    """
    num_found = 0
    PROCESSED_QUESTIONS = deepcopy(QUESTIONS)
    for source, values in PROCESSED_QUESTIONS.items():
        num_single = math.ceil(TO_QUESTIONS[source]["num_questions_to_sample"] / 2)
        num_combo = math.floor(TO_QUESTIONS[source]["num_questions_to_sample"] / 2)
        values["dfq"] = single_generation_func(values, num_single)
        values["combos"] = combo_generation_func(values, num_combo)
        num_found += len(values["dfq"]) + len(values["combos"])
        logger.info(
            f"Got {num_single} single questions and {num_combo} combo questions from {source}."
        )
    logger.info(f"Found {num_found} questions.")
    return PROCESSED_QUESTIONS


def generate_N_combos(df, N, remove_combos=None):
    """Generate `N` combinations of the indices in `df`."""
    indices = df.index.tolist()
    possible_pairs = list(combinations(indices, 2))
    if remove_combos:
        possible_pairs = [tup for tup in possible_pairs if tup not in remove_combos]
    if len(possible_pairs) < N:
        logger.warning(
            f"Not enough combinations available: Requested {N}, but only {len(possible_pairs)} are possible."
        )
        return possible_pairs
    selected_pairs = np.random.choice(len(possible_pairs), size=N, replace=False)
    return [possible_pairs[i] for i in selected_pairs]


def llm_sample_single_questions(values, n_single):
    """Generate single questions for the LLM question set."""
    return values["dfq"].sample(n_single)


def llm_sample_combo_questions(values, n_combo):
    """Generate combination questions for the LLM question set."""
    return generate_N_combos(values["dfq"], n_combo)


def human_sample_single_questions(values, n_single):
    """Get single questions for the human question set by sampling from the LLM combos question set.

    Take indices from the combos question set first to allow us to get the same combos for humans
    as we do llms.
    """
    indices = [num for tup in values["combos"] for num in tup]

    seen = set()
    unique_indices = []
    for item in indices:
        if item not in seen:
            seen.add(item)
            unique_indices.append(item)

    # Instead of sampling from combos, take the first n_single unique combos. This ensures we can
    # get combos that are seen by the LLM too.
    indices = unique_indices[:n_single]
    if len(unique_indices) < n_single:
        remaining = n_single - len(indices)
        all_indices = set(values["dfq"].index.tolist())
        remaining_indices = list(all_indices - set(indices))
        additional_indices = random.sample(
            remaining_indices, min(remaining, len(remaining_indices))
        )
        indices.extend(additional_indices)

    return values["dfq"].loc[indices]


def human_sample_combo_questions(values, n_combo):
    """Generate combination questions for the human question set by sampling from the LLM question set.

    Ensure that the combos sampled are from questions that are in the human question set.
    """
    human_indices = values["dfq"].index.tolist()
    combos = [
        (q1, q2) for q1, q2 in values["combos"] if q1 in human_indices and q2 in human_indices
    ]
    if len(combos) >= n_combo:
        return combos[:n_combo]

    combos.extend(
        generate_N_combos(df=values["dfq"], N=n_combo - len(combos), remove_combos=set(combos))
    )
    return combos


def allocate_evenly(num_questions, QUESTIONS):
    """Allocates the number of questions evenly among sources.

    `num_questions` is divided evenly among the question sources. It handles remainders and the
    constraint of trying to allocate too many questions to a source, by allocating what's leftover
    to the other sources.
    """
    sources = deepcopy(QUESTIONS)
    allocation = {
        key: min(num_questions // len(sources), source["num_questions_available"])
        for key, source in sources.items()
    }
    allocated_num_questions = sum(allocation.values())

    while allocated_num_questions < num_questions:
        remaining = num_questions - allocated_num_questions
        under_allocated = {
            key: sources[key]["num_questions_available"] - allocation[key]
            for key in sources
            if allocation[key] < sources[key]["num_questions_available"]
        }

        if not under_allocated:
            # Break if no category can take more
            break

        # Amount to add in this iteration
        to_allocate = max(remaining // len(under_allocated), 1)
        additional_alloc = {}
        for key in under_allocated:
            if under_allocated[key] > 0:
                add_amount = min(to_allocate, under_allocated[key], remaining)
                additional_alloc[key] = add_amount
                remaining -= add_amount
                if remaining <= 0:
                    break

        # Update allocation and num_questions
        for key, value in additional_alloc.items():
            allocation[key] += value
        allocated_num_questions = sum(allocation.values())

    for source in sources:
        sources[source]["num_questions_to_sample"] = allocation[source]

    num_allocated = sum(allocation.values())
    if num_allocated != num_questions:
        logger.error("*** Problem allocating questions. ***")
    logger.info(f"Allocated {num_allocated}/{num_questions}.")
    return sources


def write_questions(questions, filename):
    """Write single and combo questions to file and upload."""

    def get_id(combo_rows):
        id1 = combo_rows.at[0, "id"]
        id2 = combo_rows.at[1, "id"]
        return [id1, id2]

    def get_forecast_horizon(combo_rows):
        fh1 = combo_rows.at[0, "forecast_horizons"]
        fh2 = combo_rows.at[1, "forecast_horizons"]
        return fh1 if len(fh1) < len(fh2) else fh2

    df = pd.DataFrame()
    for source, values in tqdm(questions.items(), "Processing questions"):
        df_source = values["dfq"]
        df = pd.concat([df, df_source], ignore_index=True)
        for q1, q2 in values["combos"]:
            combo_rows = df_source.loc[[q1, q2]].reset_index(drop=True)
            df_combo = pd.DataFrame(
                [
                    {
                        "id": get_id(combo_rows),
                        "source": source,
                        "combination_of": combo_rows.to_dict(orient="records"),
                        "question": question_prompts.combination,
                        "background": "N/A",
                        "market_info_resolution_criteria": "N/A",
                        "market_info_open_datetime": "N/A",
                        "market_info_close_datetime": "N/A",
                        "url": "N/A",
                        "resolution_criteria": "N/A",
                        "freeze_datetime_value": "N/A",
                        "freeze_datetime_value_explanation": "N/A",
                        "freeze_datetime": constants.FREEZE_DATETIME.isoformat(),
                        "human_prompt": question_prompts.combination,
                        "forecast_horizons": get_forecast_horizon(combo_rows),
                    }
                ]
            )
            df = pd.concat([df, df_combo], ignore_index=True)

    local_filename = f"/tmp/{filename}"
    with open(local_filename, "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            jsonl_str = json.dumps(record, ensure_ascii=False)
            f.write(jsonl_str + "\n")

    gcp.storage.upload(
        bucket_name=env.QUESTION_SETS_BUCKET,
        local_filename=local_filename,
    )


def drop_invalid_questions(dfq, dfmeta):
    """Drop invalid questions from dfq."""
    if dfmeta.empty:
        return dfq
    dfq = pd.merge(
        dfq,
        dfmeta,
        how="inner",
        on=["id", "source"],
    )
    return dfq[dfq["valid_question"]].drop(columns="valid_question")


@decorator.log_runtime
def driver(_):
    """Curate questions for forecasting."""

    def format_string_field(row, template, field):
        return template.format(f_string_value=row[field])

    def format_string_value(row, template, value):
        return template.format(f_string_value=value)

    dfmeta = data_utils.download_and_read(
        filename=constants.META_DATA_FILENAME,
        local_filename=f"/tmp/{constants.META_DATA_FILENAME}",
        df_tmp=pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
        dtype=constants.META_DATA_FILE_COLUMN_DTYPE,
    )

    # Get the latest questions
    QUESTIONS = constants.FREEZE_QUESTION_SOURCES
    sources_to_remove = []
    for source, _ in constants.FREEZE_QUESTION_SOURCES.items():
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        if dfq.empty:
            sources_to_remove.extend([source])
            logger.info(f"Found 0 questions from {source}.")
        else:
            dfq["source"] = source
            dfq = drop_invalid_questions(dfq=dfq, dfmeta=dfmeta)
            dfq = dfq[~dfq["resolved"]]
            dfq = dfq[dfq["forecast_horizons"].map(len) > 0]
            dfq["human_prompt"] = dfq.apply(
                format_string_value,
                args=(QUESTIONS[source]["human_prompt"], QUESTIONS[source]["name"]),
                axis=1,
            )
            dfq["resolution_criteria"] = dfq.apply(
                format_string_field, args=(QUESTIONS[source]["resolution_criteria"], "url"), axis=1
            )
            dfq["freeze_datetime"] = constants.FREEZE_DATETIME.isoformat()
            dfq["combination_of"] = "N/A"
            if source == "acled":
                # Drop Acled-specific columns
                dfq.drop(
                    list(
                        set(constants.ACLED_QUESTION_FILE_COLUMNS)
                        - set(constants.QUESTION_FILE_COLUMNS)
                    ),
                    axis=1,
                    inplace=True,
                )
            dfq.drop(
                [
                    "market_info_resolution_datetime",
                    "resolved",
                ],
                axis=1,
                inplace=True,
            )
            num_questions = len(dfq)
            QUESTIONS[source]["dfq"] = dfq.reset_index(drop=True)
            QUESTIONS[source]["num_questions_available"] = num_questions
            logger.info(f"Found {num_questions} questions from {source}.")

    QUESTIONS = {key: value for key, value in QUESTIONS.items() if key not in sources_to_remove}

    # Find allocations of questions
    LLM_QUESTIONS = allocate_evenly(
        constants.FREEZE_NUM_LLM_QUESTIONS,
        QUESTIONS,
    )
    HUMAN_QUESTIONS = allocate_evenly(
        constants.FREEZE_NUM_HUMAN_QUESTIONS,
        LLM_QUESTIONS,
    )

    # Sample questions
    LLM_QUESTIONS = process_questions(
        QUESTIONS,
        LLM_QUESTIONS,
        llm_sample_single_questions,
        llm_sample_combo_questions,
    )
    HUMAN_QUESTIONS = process_questions(
        LLM_QUESTIONS,
        HUMAN_QUESTIONS,
        human_sample_single_questions,
        human_sample_combo_questions,
    )

    def _log_questions_found(questions, for_whom):
        running_sum = 0
        for source, values in questions.items():
            num_single = len(values["dfq"])
            num_combo = len(values["combos"])
            num_total = num_single + num_combo
            running_sum += num_total
            logger.info(f"* {source}: Single: {num_single}. Combo: {num_combo}. Total: {num_total}")
        logger.info(f"Found {running_sum} questions total for {for_whom}.")

    _log_questions_found(LLM_QUESTIONS, "LLMs")
    _log_questions_found(HUMAN_QUESTIONS, "Humans")

    forecast_date_str = constants.FORECAST_DATE.isoformat()
    llm_filename = f"{forecast_date_str}-llm.jsonl"
    human_filename = f"{forecast_date_str}-human.jsonl"

    write_questions(LLM_QUESTIONS, llm_filename)
    write_questions(HUMAN_QUESTIONS, human_filename)

    logger.info("Done.")

    return "OK", 200


if __name__ == "__main__":
    driver(None)
