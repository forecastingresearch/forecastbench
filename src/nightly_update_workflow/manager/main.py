"""Manage nightly update."""

import logging
import os
import sys

import pandas as pd
from tabulate import tabulate

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import cloud_run, constants, env, question_curation, slack  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def call_worker(dict_to_use, task_count, timeout=cloud_run.timeout_1h):
    """Make main() easier to read."""
    return cloud_run.call_worker(
        job_name="nightly-worker",
        env_vars={
            "DICT_TO_USE": dict_to_use,
        },
        task_count=task_count,
        timeout=timeout,
    )


def summarize_question_bank():
    """Send a message to Slack with the updated status of the question bank."""
    dfmeta = pd.read_json(
        f"gs://{env.QUESTION_BANK_BUCKET}/{constants.META_DATA_FILENAME}",
        lines=True,
    )
    df = pd.DataFrame()
    for source in sorted(question_curation.ALL_SOURCES):
        logger.info(f"downloading {source} question file.")
        dfq = pd.read_json(
            f"gs://{env.QUESTION_BANK_BUCKET}/{source}_questions.jsonl",
            lines=True,
            convert_dates=False,
        )
        dfq = dfq[~dfq["resolved"]].reset_index(drop=True)
        dfq["source"] = source
        dfq["id"] = dfq["id"].astype(str)

        dfq_valid = pd.merge(
            dfq,
            dfmeta,
            how="inner",
            on=["id", "source"],
        )
        dfq_valid = dfq_valid[dfq_valid["valid_question"]]

        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    [
                        {
                            "source": source,
                            "N unresolved": len(dfq),
                            "N valid unresolved": len(dfq_valid),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    sum_row = {
        col: df[col].sum() if pd.api.types.is_numeric_dtype(df[col]) else "Total"
        for col in df.columns
    }
    df = pd.concat([df, pd.DataFrame([sum_row])], ignore_index=True)
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].map(lambda x: f"{x:,}")
    df_str = tabulate(df, headers="keys", tablefmt="psql", showindex=False)
    slack.send_message(message=f"```{df_str}```")


def main():
    """Manage nightly run."""
    dict_to_use_publish_question_set = "publish_question_set_make_llm_baseline"
    timeout_publish_question_set = cloud_run.timeout_1h * 8
    operation_publish_question_set = call_worker(
        dict_to_use=dict_to_use_publish_question_set,
        task_count=1,
        timeout=timeout_publish_question_set,
    )

    dict_to_use = "fetch_and_update"
    task_count = len(question_curation.FREEZE_QUESTION_DATA_SOURCES) + len(
        question_curation.FREEZE_QUESTION_MARKET_SOURCES
    )
    operation = call_worker(dict_to_use=dict_to_use, task_count=task_count)
    cloud_run.block_and_check_job_result(
        operation=operation,
        name=dict_to_use,
        exit_on_error=True,
        additional_slack_message_on_error=(
            "‼️ IMPORTANT: TODAY THE QUESTION SET MUST BE CREATED ‼️"
            if question_curation.is_today_question_curation_date()
            else ""
        ),
    )

    if question_curation.is_today_question_curation_date():
        slack.send_message(message="Question set successfully created 😊")

    dict_to_use_resolve_and_leaderboard = "resolve_and_leaderboard"
    timeout_resolve = cloud_run.timeout_1h * 2
    operation_resolve_and_leaderboard = call_worker(
        dict_to_use=dict_to_use_resolve_and_leaderboard,
        task_count=1,
        timeout=timeout_resolve,
    )

    dict_to_use_metadata = "metadata"
    operation_metadata = call_worker(dict_to_use=dict_to_use_metadata, task_count=1)
    cloud_run.block_and_check_job_result(
        operation=operation_metadata,
        name=dict_to_use_metadata,
        exit_on_error=False,
    )

    summarize_question_bank()

    dict_to_use_create_question_set = "create_question_set"
    operation_create_question_set = call_worker(
        dict_to_use=dict_to_use_create_question_set, task_count=1
    )

    dict_to_use_naive_and_dummy_forecasters = "naive_and_dummy_forecasters"
    operation_naive_and_dummy_forecasters = call_worker(
        dict_to_use=dict_to_use_naive_and_dummy_forecasters, task_count=1
    )

    cloud_run.block_and_check_job_result(
        operation=operation_resolve_and_leaderboard,
        name=dict_to_use_resolve_and_leaderboard,
        exit_on_error=True,
    )

    cloud_run.block_and_check_job_result(
        operation=operation_create_question_set,
        name=dict_to_use_create_question_set,
        exit_on_error=True,
    )

    dict_to_use_website = "website"
    operation_website = call_worker(dict_to_use=dict_to_use_website, task_count=1)
    cloud_run.block_and_check_job_result(
        operation=operation_website,
        name=dict_to_use_website,
        exit_on_error=True,
    )

    cloud_run.block_and_check_job_result(
        operation=operation_publish_question_set,
        name=dict_to_use_publish_question_set,
        exit_on_error=True,
        timeout=timeout_publish_question_set,
        additional_slack_message_on_error=(
            "‼️ IMPORTANT: TODAY IS THE DAY LLMS ARE FORECASTING ‼️"
            if question_curation.is_today_question_set_publication_date()
            else ""
        ),
    )

    cloud_run.block_and_check_job_result(
        operation=operation_naive_and_dummy_forecasters,
        name=dict_to_use_naive_and_dummy_forecasters,
        exit_on_error=True,
        additional_slack_message_on_error=(
            "‼️ IMPORTANT: TODAY IS THE DAY LLMS ARE FORECASTING ‼️"
            if question_curation.is_today_question_set_publication_date()
            else ""
        ),
    )

    slack.send_message(message="Nightly update succeeded 😊")


if __name__ == "__main__":
    main()
