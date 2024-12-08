"""Run zero-shot and scratchpad evaluations for LLM models."""

import argparse
import json
import logging
import os
import shutil
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import cloud_run, decorator, git, keys  # noqa: E402


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run LLM evaluations.")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["TEST", "PROD"],
        default="TEST",
        help="Run mode: TEST for specific models and 2 questions, PROD for all models and questions",
    )
    return parser.parse_args()


def get_latest_forecast_due_date():
    """Get the forecast due date from latest-llm.json in the datasets git repo."""
    _, local_repo_dir, _ = git.clone(repo_url=keys.API_GITHUB_DATASET_REPO_URL)
    latest_json_filename = f"{local_repo_dir}/datasets/question_sets/latest-llm.json"
    with open(latest_json_filename, "r") as file:
        questions_data = json.load(file)
    shutil.rmtree(local_repo_dir)
    return questions_data["forecast_due_date"]


@decorator.log_runtime
def main():
    """Launch worker processes to generate LLM baselines."""
    args = parse_arguments()

    forecast_due_date = get_latest_forecast_due_date()

    logger.info(f"Running LLM baselines for: {forecast_due_date}-llm.json")

    timeout = cloud_run.timeout_1h * 8
    operation = cloud_run.call_worker(
        job_name="func-baseline-llm-forecasts-worker",
        env_vars={
            "FORECAST_DUE_DATE": forecast_due_date,
            "TEST_OR_PROD": args.mode,
        },
        task_count=9,
        timeout=timeout,
    )
    cloud_run.block_and_check_job_result(
        operation=operation,
        name="llm-baselines",
        exit_on_error=True,
        timeout=timeout,
    )


if __name__ == "__main__":
    main()
