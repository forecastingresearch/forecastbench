"""Cloud Run manager for ForecastBench LLM forecasts."""

import logging
import os

from helpers import cloud_run, decorator
from helpers.run_mode import RunMode
from llm_forecaster import fb_model_runs
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_manager(run_mode: RunMode) -> None:
    """Launch one LLM forecaster worker task per model run."""
    question_set_metadata = _io.get_latest_llm_question_set_metadata()
    forecast_due_date = question_set_metadata["forecast_due_date"]
    timeout = cloud_run.timeout_1h * 24

    logger.info(f"Found forecast due date of {forecast_due_date}.")
    operation = cloud_run.call_worker(
        job_name="func-llm-forecaster-worker",
        env_vars={
            "FORECAST_DUE_DATE": forecast_due_date,
            "TEST_OR_PROD": run_mode.value,
        },
        task_count=len(fb_model_runs.FB_MODEL_RUNS),
        timeout=timeout,
    )
    cloud_run.block_and_check_job_result(
        operation=operation,
        name="llm-forecaster",
        exit_on_error=True,
        timeout=timeout,
    )


@decorator.log_runtime
def main() -> None:
    """Run the LLM forecaster manager."""
    run_mode = RunMode.from_string(os.getenv("TEST_OR_PROD"))
    logger.info(f"Running {run_mode.value} LLM forecaster manager.")
    run_manager(run_mode)


if __name__ == "__main__":
    main()
