"""Freeze forecasting questions."""

import json
import logging
import os
import random
import sys
from copy import deepcopy
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    question_curation,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_questions(questions, to_questions, single_generation_func):
    """Sample from `questions` to get the number of questions needed.

    This function works for both the LLM question set and the human forecaster question set.
    """
    num_found = 0
    processed_questions = deepcopy(questions)
    for source, values in processed_questions.items():
        num_single = to_questions[source]["num_questions_to_sample"]
        values["dfq"] = single_generation_func(values, num_single)
        num_found += len(values["dfq"])
    logger.info(f"Found {num_found:,} questions.")
    return processed_questions


def human_sample_questions(values, n_single):
    """Get questions for the human question set by sampling from LLM questions."""
    dfq = values["dfq"].copy()
    indices_to_sample_from = dfq.index.tolist()
    indices = random.sample(indices_to_sample_from, min(n_single, len(indices_to_sample_from)))
    return dfq.loc[indices]


def llm_sample_questions(values, n_single):
    """Generate questions for the LLM question set.

    Sample evenly across categories.
    """
    dfq = values["dfq"].copy()
    allocation, underrepresented_categories = allocate_across_categories(
        num_questions=n_single, dfq=dfq
    )

    df = pd.DataFrame()
    for key, value in allocation.items():
        df_tmp = dfq[dfq["category"] == key].sample(value)
        df = pd.concat([df, df_tmp], ignore_index=True)

    df["underrepresented_category"] = df["category"].apply(
        lambda x: True if x in underrepresented_categories else False
    )
    return df


def allocate_evenly(data: dict, n: int):
    """Allocates the number of questions evenly given `data`.

    `n` is the total number of items we want to allocate.

    `data` is a dict that has the items to allocate across as keys and the number of possible items
    to allocate as values. So, if we're allocating across sources, it would look like:
    {'source1': 30, 'source2': 50, ...} and if we're allocating across categories within a source
    it would look like: {'category1': 30, 'category2': 50, ...}.

    The function returns a dict with the same keys as `data` but with the allocation. The allocated
    values are guaranteed to be <= the original values provided in `data`.

    If `sum(data.values()) <= n` it returns `data`.
    """

    def print_info_message(num_allocated, n):
        if num_allocated != n:
            logger.error(f"*** Problem allocating evenly... Allocated {num_allocated:,}/{n}. ***")
            sys.exit(1)
        else:
            logger.info(f"Successfully allocated {num_allocated:,}/{n}.")

    sum_n_items = sum(data.values())
    if sum_n_items <= n:
        print_info_message(sum_n_items, n)
        return data, sorted([key for key, value in data.items()])

    # initial allocation
    allocation = {key: min(n // len(data), value) for key, value in data.items()}
    allocated_num = sum(allocation.values())
    underrepresented_items = sorted(
        [key for key, value in data.items() if allocation[key] == value]
    )

    while allocated_num < n:
        remaining = n - allocated_num
        under_allocated = {
            key: value - allocation[key] for key, value in data.items() if allocation[key] < value
        }

        if not under_allocated:
            # Break if nothing more to allocate
            break

        # Amount to add in this iteration
        to_allocate = max(remaining // len(under_allocated), 1)
        for key in under_allocated:
            if under_allocated[key] > 0:
                add_amount = min(to_allocate, under_allocated[key], remaining)
                allocation[key] += add_amount
                remaining -= add_amount
                if remaining <= 0:
                    break
        allocated_num = sum(allocation.values())

    num_allocated = sum(allocation.values())
    print_info_message(num_allocated, n)
    return allocation, underrepresented_items


def allocate_across_categories(num_questions, dfq):
    """Allocates the number of questions evenly among categories."""
    categories = dfq["category"].unique()
    data = {category: sum(dfq["category"] == category) for category in categories}
    return allocate_evenly(data=data, n=num_questions)


def allocate_across_sources(questions, num_questions):
    """Allocates the number of questions evenly among sources."""
    sources = deepcopy(questions)
    data = {key: source["num_questions_available"] for key, source in sources.items()}

    allocation, _ = allocate_evenly(data=data, n=num_questions)

    for source in sources:
        sources[source]["num_questions_to_sample"] = allocation[source]

    num_allocated = sum(allocation.values())
    if num_allocated != num_questions:
        logger.error("*** Problem allocating questions. ***")
        sys.exit(1)

    logger.info(f"Allocated {num_allocated:,}/{num_questions:,}.")
    return sources


def write_questions(questions, for_whom):
    """Write questions to file and upload."""

    def forecast_horizons_to_resolution_dates(forecast_horizons):
        return (
            [
                (question_curation.FORECAST_DATETIME + timedelta(days=day)).date().isoformat()
                for day in forecast_horizons
            ]
            if forecast_horizons != "N/A"
            else forecast_horizons
        )

    df = pd.DataFrame()
    for _, values in tqdm(questions.items(), "Writing questions"):
        df_source = values["dfq"]
        # Order columns consistently for writing
        df_source = deepcopy(
            df_source[
                [
                    "id",
                    "source",
                    "question",
                    "resolution_criteria",
                    "background",
                    "market_info_open_datetime",
                    "market_info_close_datetime",
                    "market_info_resolution_criteria",
                    "url",
                    "freeze_datetime",
                    "freeze_datetime_value",
                    "freeze_datetime_value_explanation",
                    "source_intro",
                    "forecast_horizons",
                ]
            ]
        )
        df_source["resolution_dates"] = df_source["forecast_horizons"].apply(
            forecast_horizons_to_resolution_dates
        )
        df_source = df_source.drop(columns="forecast_horizons")

        df = pd.concat(
            [
                df,
                df_source,
            ],
            ignore_index=True,
        )

    forecast_date_str = question_curation.FORECAST_DATE.isoformat()
    filename = f"{forecast_date_str}-{for_whom}.json"
    latest_filename = f"latest-{for_whom}.json"
    local_filename = f"/tmp/{filename}"

    json_data = {
        "forecast_due_date": forecast_date_str,
        "question_set": filename,
        "questions": df.to_dict(orient="records"),
    }

    with open(local_filename, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

    if not env.RUNNING_LOCALLY:
        gcp.storage.upload(
            bucket_name=env.QUESTION_SETS_BUCKET,
            local_filename=local_filename,
            filename=filename,
        )

        gcp.storage.upload(
            bucket_name=env.QUESTION_SETS_BUCKET,
            local_filename=local_filename,
            filename=latest_filename,
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


def drop_missing_freeze_datetime(dfq):
    """Drop questions with missing values in the `freeze_datetime_value` column."""
    col = "freeze_datetime_value"
    dfq = dfq.dropna(subset=col, ignore_index=True)
    dfq = dfq[dfq[col] != "N/A"]
    dfq = dfq[dfq[col] != "nan"]
    return dfq


def market_resolves_before_forecast_due_date(dt):
    """Determine whether or not the market resolves before the forecast due date.

    Parameters:
    - dt (datetime): a datetime that represents the market close time.
    """
    llm_forecast_release_datetime = question_curation.FREEZE_DATETIME + timedelta(
        days=question_curation.FREEZE_WINDOW_IN_DAYS
    )
    all_forecasts_due = llm_forecast_release_datetime.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    ndays = dt - all_forecasts_due
    ndays = ndays.days + (1 if ndays.total_seconds() > 0 else 0)
    return ndays <= 0


def drop_questions_that_resolve_too_soon(source, dfq):
    """Drop questions that resolve too soon.

    Given the freeze date:
    * for market questions determine whether or not the market will close before at least the first
      forecasting horizon. If it does, then do not use this question.
    * for data questions if forecast_horizons is empty, don't use the question
    """
    if source in question_curation.DATA_SOURCES:
        empty_horizons = dfq["forecast_horizons"].apply(lambda x: len(x) == 0)
        mask = empty_horizons | dfq["forecast_horizons"] == "N/A"
        return dfq.drop(labels=dfq[mask].index.tolist())

    empty_horizons = dfq["market_info_close_datetime"].apply(
        lambda x: market_resolves_before_forecast_due_date(datetime.fromisoformat(x))
    )
    indices_to_drop = empty_horizons[empty_horizons].index.tolist()
    return dfq.drop(labels=indices_to_drop)


@decorator.log_runtime
def driver(_):
    """Create question set."""
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

    # Get the latest questions
    QUESTIONS = question_curation.FREEZE_QUESTION_SOURCES
    sources_to_remove = []
    for source, _ in question_curation.FREEZE_QUESTION_SOURCES.items():
        dfq = data_utils.get_data_from_cloud_storage(
            source=source,
            return_question_data=True,
        )
        if dfq.empty:
            sources_to_remove.extend([source])
            logger.warning(f"Found 0 questions from {source}.")
        else:
            dfq["source"] = source
            dfq = drop_invalid_questions(dfq=dfq, dfmeta=dfmeta)
            dfq = drop_missing_freeze_datetime(dfq)
            dfq = dfq[dfq["category"] != "Other"]
            dfq = dfq[~dfq["resolved"]]
            dfq = drop_questions_that_resolve_too_soon(source=source, dfq=dfq)
            dfq["source_intro"] = dfq.apply(
                lambda row, source=source: QUESTIONS[source]["source_intro"], axis=1
            )
            dfq["resolution_criteria"] = dfq.apply(
                lambda row, source=source: QUESTIONS[source]["resolution_criteria"].format(
                    url=row["url"]
                ),
                axis=1,
            )
            dfq["freeze_datetime"] = question_curation.FREEZE_DATETIME.isoformat()
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
            logger.info(f"Found {num_questions:,} single questions from {source}.\n")

    QUESTIONS = {key: value for key, value in QUESTIONS.items() if key not in sources_to_remove}

    # Find allocations of questions
    LLM_QUESTIONS, HUMAN_QUESTIONS = {}, {}
    for question_type in [question_curation.MARKET_SOURCES, question_curation.DATA_SOURCES]:
        questions_of_question_type = {k: v for k, v in QUESTIONS.items() if k in question_type}
        llm_questions_of_question_type = allocate_across_sources(
            questions=questions_of_question_type,
            num_questions=question_curation.FREEZE_NUM_LLM_QUESTIONS // 2,
        )
        LLM_QUESTIONS.update(llm_questions_of_question_type)
        human_questions_of_question_type = allocate_across_sources(
            questions=llm_questions_of_question_type,
            num_questions=question_curation.FREEZE_NUM_HUMAN_QUESTIONS // 2,
        )
        HUMAN_QUESTIONS.update(human_questions_of_question_type)

    # Sample questions
    LLM_QUESTIONS = process_questions(
        questions=QUESTIONS,
        to_questions=LLM_QUESTIONS,
        single_generation_func=llm_sample_questions,
    )
    HUMAN_QUESTIONS = process_questions(
        questions=LLM_QUESTIONS,
        to_questions=HUMAN_QUESTIONS,
        single_generation_func=human_sample_questions,
    )

    def _log_questions_found(questions, for_humans):
        for_whom = "Humans" if for_humans else "LLMs"
        logger.info("\n\n")
        logger.info("*" * 50)
        logger.info(f"Printing info for {for_whom} question set.")
        running_sum = 0
        for source, values in questions.items():
            logger.info("\n")
            # Overall
            dfq = values["dfq"]
            num_total = len(dfq)
            running_sum += num_total
            logger.info(f"* {source}: Single: {num_total}.")

            # Categories
            category_counts = (
                dfq.groupby("category")
                .agg(
                    count=("category", "size"),
                    underrepresented=("underrepresented_category", "any"),
                )
                .reset_index()
            )

            max_category_length = max(
                len(row["category"]) + (4 if row["underrepresented"] else 0)
                for index, row in category_counts.iterrows()
            )

            logger.info(f'    {"".ljust(max_category_length)}  N')
            for _, row in category_counts.iterrows():
                category = row["category"]
                count = row["count"]
                if row["underrepresented"]:
                    category += " (*)"
                logger.info(f"  - {category.ljust(max_category_length)}: {count}")

        logger.info("")
        logger.info("Quick summary...")
        n_dataset = 0
        n_market = 0
        for label, question_type_sources in [
            ("Dataset Sources", question_curation.DATA_SOURCES),
            ("Market Sources", question_curation.MARKET_SOURCES),
        ]:
            logger.info("")
            logger.info(f"{label}")
            for source in question_type_sources:
                n_questions = len(questions.get(source)["dfq"])
                n_question_bank = len(QUESTIONS.get(source)["dfq"])
                logger.info(f"* {source}: {n_questions}/{n_question_bank:,}")
                if source in question_curation.DATA_SOURCES:
                    n_dataset += n_questions
                elif source in question_curation.MARKET_SOURCES:
                    n_market += n_questions

        logger.info("")
        logger.info(f"Found {n_dataset} DATASET questions and {n_market} MARKET questions.")
        logger.info(f"Found {running_sum} questions total for {for_whom}.\n")

    _log_questions_found(HUMAN_QUESTIONS, for_humans=True)
    _log_questions_found(LLM_QUESTIONS, for_humans=False)

    write_questions(LLM_QUESTIONS, "llm")
    write_questions(HUMAN_QUESTIONS, "human")

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
