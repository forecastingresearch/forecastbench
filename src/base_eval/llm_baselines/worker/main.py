"""Run zero-shot and scratchpad evaluations for LLM models."""

import logging
import os
import shutil
import sys
import time

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from helpers import (  # noqa: E402
    constants,
    data_utils,
    decorator,
    env,
    git,
    keys,
    model_eval,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
from utils import gcp  # noqa: E402


def get_questions(forecast_due_date: str, num_questions_per_question_type: int | None) -> list:
    """
    Load and prepare questions for evaluation.

    Args:
        forecast_due_date (str): Forecast due date used to locate the questions file.
        num_questions_per_question_type (int | None): Limit per question type (None for all).

    Returns:
        questions (list): Ordered list of question sets: [dataset_questions, market_questions].
    """
    _, local_repo_dir, _ = git.clone(repo_url=keys.API_GITHUB_DATASET_REPO_URL)
    LOCAL_QUESTIONS_FILE = f"{local_repo_dir}/datasets/question_sets/{forecast_due_date}-llm.json"

    market_questions, dataset_questions = model_eval.process_questions(
        LOCAL_QUESTIONS_FILE,
        num_questions_per_question_type=num_questions_per_question_type,
    )
    questions = [
        dataset_questions,
        market_questions,
    ]

    shutil.rmtree(local_repo_dir)
    return questions


def upload_forecast_files(
    base_file_path: str,
    prompt_type: str,
    forecast_due_date: str,
    run_mode: constants.RunMode,
) -> None:
    """
    Upload local forecast artifacts to GCP.

    Args:
        base_file_path (str): GCS base path prefix for storing intermediate records.
        prompt_type (str): Prompt variant used ("zero_shot" or "scratchpad").
        forecast_due_date (str): Forecast due date used in remote path.
        run_mode (constants.RunMode): Execution mode controlling paths and behavior.

    Returns:
        None: Uploads forecast files
    """
    local_submit_dir = model_eval.get_local_final_submit_directory(
        prompt_type=prompt_type,
        run_mode=run_mode,
    )

    forecast_filenames = data_utils.list_files(local_submit_dir)
    for forecast_filename in forecast_filenames:
        local_filename = local_submit_dir + "/" + forecast_filename
        gcp.storage.upload(
            bucket_name=env.FORECAST_SETS_BUCKET,
            local_filename=local_filename,
            filename=f"{forecast_due_date}/{forecast_filename}",
        )


def parse_env_vars() -> tuple[str, constants.RunMode, str, str]:
    """
    Parse and validate environment variables, and derive model and prompt selection.

    Environment variables:
        FORECAST_DUE_DATE (str): Required. Date string used by downstream processes.
        TEST_OR_PROD (str): Required. Must be valid constants.RunMode
        CLOUD_RUN_TASK_INDEX (int): Required. Zero-based task index provided by Cloud Run.
    Args:
        None

    Returns:
        result (tuple[str, constants.RunMode, str, str]):
            (forecast_due_date, run_mode, model_to_test, prompt_type).
    """
    forecast_due_date = os.getenv("FORECAST_DUE_DATE")
    if not forecast_due_date:
        logger.error(f"`forecast_due_date` was not set: {forecast_due_date}.")
        sys.exit(1)

    try:
        run_mode = constants.RunMode(os.getenv("TEST_OR_PROD", ""))
    except ValueError:
        logger.error("`TEST_OR_PROD` must be one of TEST or PROD.")
        sys.exit(1)

    try:
        task_num = int(os.getenv("CLOUD_RUN_TASK_INDEX"))
    except Exception as e:
        logger.error("ERROR: Unexpected error. Should not arrive here. Error message...")
        logger.error(e)
        sys.exit(1)

    all_models = list(constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS.keys())
    n_prompts = len(constants.PROMPT_TYPES)
    max_tasks = len(all_models) * n_prompts if run_mode == constants.RunMode.PROD else 1
    if task_num >= max_tasks:
        logger.info(f"task number {task_num} not needed, winding down.")
        sys.exit(0)

    model_idx = task_num // n_prompts
    model_to_test = all_models[model_idx]

    prompt_type_idx = task_num % n_prompts
    prompt_type = constants.PROMPT_TYPES[prompt_type_idx]

    # Sleep `task_num` seconds to avoid cloning the same git repo too quickly
    time.sleep(task_num)

    return forecast_due_date, run_mode, model_to_test, prompt_type


@decorator.log_runtime
def main() -> None:
    """
    Orchestrate evaluation: load questions, run models, and persist results.

    Args:
        None

    Returns:
        None
    """
    forecast_due_date, run_mode, model_to_test, prompt_type = parse_env_vars()
    logger.info(f"TESTING: {model_to_test} {prompt_type}")

    model_dict = {model_to_test: constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS.get(model_to_test)}
    num_questions_per_question_type = 2 if run_mode == constants.RunMode.TEST else None

    base_file_path = f"individual_forecast_records/{forecast_due_date}"

    results = {}
    questions = get_questions(
        forecast_due_date=forecast_due_date,
        num_questions_per_question_type=num_questions_per_question_type,
    )
    for question_set in questions:
        for market_use_freeze_value in [False, True]:
            test_type = model_eval.determine_test_type(
                question_set,
                prompt_type,
                market_use_freeze_value,
                run_mode,
            )
            questions_to_eval = question_set

            gcp_file_path = f"{base_file_path}/{test_type}/{model_to_test}.jsonl"

            results[model_to_test] = model_eval.download_and_read_saved_forecasts(
                filename=gcp_file_path,
                base_file_path=base_file_path,
            )

            if results[model_to_test]:
                logger.info(f"Downloaded {gcp_file_path}. Skipping.")
            else:
                logger.info(
                    f"No results loaded for {gcp_file_path}. {model_to_test} is running inference..."
                )
                results[model_to_test] = {i: "" for i in range(len(questions_to_eval))}
                model_eval.process_model(
                    model=model_to_test,
                    models=model_dict,
                    test_type=test_type,
                    results=results,
                    questions_to_eval=questions_to_eval,
                    forecast_due_date=forecast_due_date,
                    prompt_type=prompt_type,
                    market_use_freeze_value=market_use_freeze_value,
                    base_file_path=base_file_path,
                )

    model_eval.generate_final_forecast_files(
        forecast_due_date=forecast_due_date,
        prompt_type=prompt_type,
        models=model_dict,
        run_mode=run_mode,
    )
    upload_forecast_files(
        base_file_path=base_file_path,
        prompt_type=prompt_type,
        forecast_due_date=forecast_due_date,
        run_mode=run_mode,
    )

    logger.info(f"Done for {model_to_test}")
    logger.info(f"Model info {model_dict}")


if __name__ == "__main__":
    main()
