"""Run zero-shot and scratchpad evaluations for LLM models."""

import argparse
import logging
import os
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import cloud_run, constants, decorator, question_sets  # noqa: E402


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the LLM baselines manager.

    Args:
        None

    Returns:
        argparse.Namespace: Parsed arguments with:
            - mode (constants.RunMode): Run mode ("TEST" or "PROD"), case-insensitive; defaults to TEST.
    """
    parser = argparse.ArgumentParser(description="Run LLM evaluations.")
    parser.add_argument(
        "mode",
        nargs="?",
        type=constants.RunMode,
        choices=list(constants.RunMode),
        default=constants.RunMode.TEST,
        help="Run mode: TEST for specific models and 2 questions, PROD for all models and questions",
    )
    return parser.parse_args()


@decorator.log_runtime
def main() -> None:
    """
    Run zero-shot and scratchpad evaluations for LLM models.

    Args:
        None

    Returns:
        None
    """
    args = parse_arguments()

    forecast_due_date = question_sets.get_field_from_latest_question_set_file("forecast_due_date")

    logger.info(f"Running {args.mode.value} run of LLM baselines for {forecast_due_date}-llm.json")

    timeout = cloud_run.timeout_1h * 24
    task_count = len(constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS.keys()) * len(constants.PROMPT_TYPES)
    operation = cloud_run.call_worker(
        job_name="func-baseline-llm-forecasts-worker",
        env_vars={
            "FORECAST_DUE_DATE": forecast_due_date,
            "TEST_OR_PROD": args.mode.value,
        },
        task_count=task_count,
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
