"""Manage nightly update."""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import cloud_run, question_curation, slack  # noqa: E402

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
    )

    dict_to_use_resolve_and_leaderboard = "resolve_and_leaderboard"
    operation_resolve_and_leaderboard = call_worker(
        dict_to_use=dict_to_use_resolve_and_leaderboard, task_count=1
    )

    dict_to_use_metadata = "metadata"
    operation_metadata = call_worker(dict_to_use=dict_to_use_metadata, task_count=1)
    cloud_run.block_and_check_job_result(
        operation=operation_metadata,
        name=dict_to_use_metadata,
        exit_on_error=False,
    )

    dict_to_use_create_question_set = "create_question_set"
    operation_create_question_set = call_worker(
        dict_to_use=dict_to_use_create_question_set, task_count=1
    )

    dict_to_use_naive_forecaster = "naive_forecaster"
    operation_naive_forecaster = call_worker(dict_to_use=dict_to_use_naive_forecaster, task_count=1)

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
    )

    cloud_run.block_and_check_job_result(
        operation=operation_naive_forecaster,
        name=dict_to_use_naive_forecaster,
        exit_on_error=True,
    )

    slack.send_message(message="Nightly update succeeded.")


if __name__ == "__main__":
    main()
