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


def get_questions(forecast_due_date, num_questions):
    """Load the questions file from the git repo."""
    _, local_repo_dir, _ = git.clone(repo_url=keys.API_GITHUB_DATASET_REPO_URL)
    LOCAL_QUESTIONS_FILE = f"{local_repo_dir}/datasets/question_sets/{forecast_due_date}-llm.json"

    single_market, single_non_market = model_eval.process_questions(
        LOCAL_QUESTIONS_FILE, num_per_source=num_questions
    )
    questions = [
        single_non_market,
        single_market,
    ]

    shutil.rmtree(local_repo_dir)
    return questions


def delete_and_upload_to_the_cloud(
    base_file_path, prompt_type, question_types, forecast_due_date, run_mode
):
    """Upload local forecast files to GCP and then delete them."""
    # submits the final forecasts to forecastbench-forecast-sets-dev
    local_directory = f"/tmp/{prompt_type}/final_submit"
    if run_mode == constants.RunMode.TEST:
        local_directory += "_test"

    forecast_filenames = data_utils.list_files(local_directory)
    for forecast_filename in forecast_filenames:
        local_filename = local_directory + "/" + forecast_filename
        gcp.storage.upload(
            bucket_name=env.FORECAST_SETS_BUCKET,
            local_filename=local_filename,
            filename=f"{forecast_due_date}/{forecast_filename}",
        )
        os.remove(local_filename)
        print(f"deleted... {local_filename}")

    # save intermediate results to forecastbench-forecast-sets-dev/individual_forecast_records
    # in case the notebook is interrupted, it would pick up where it left off and continue running.
    for question_type in question_types:
        local_directory = f"/tmp/{prompt_type}/{question_type}"
        if run_mode == constants.RunMode.TEST:
            local_directory += "_test"
        if os.path.exists(local_directory):
            forecast_filenames = data_utils.list_files(local_directory)
            for forecast_filename in forecast_filenames:
                local_filename = local_directory + f"/{forecast_filename}"
                remote_filename = local_directory.replace("/tmp/", "") + f"/{forecast_filename}"
                gcp.storage.upload(
                    bucket_name=env.FORECAST_SETS_BUCKET,
                    local_filename=local_filename,
                    filename=f"{base_file_path}/{remote_filename}",
                )

                os.remove(local_filename)
                print(f"{local_filename} is deleted.")

        # delete freeze values files in local location
        if "non_market" not in question_type and question_type not in [
            "final",
            "final_with_freeze",
        ]:
            local_directory = f"/tmp/{prompt_type}/{question_type}/with_freeze_values"
            if run_mode == constants.RunMode.TEST:
                local_directory += "_test"

            if os.path.exists(local_directory):
                forecast_filenames = data_utils.list_files(local_directory)
                for forecast_filename in forecast_filenames:
                    local_filename = os.path.join(local_directory, forecast_filename)
                    if os.path.exists(local_filename):
                        os.remove(local_filename)
                        print(f"{local_filename} is deleted.")
                    else:
                        print(f"Warning: {local_filename} does not exist.")
            else:
                print(f"Directory {local_directory} does not exist. Skipping deletion.")


@decorator.log_runtime
def main():
    """
    Process questions for different models and prompt types.

    Steps:
    1. Determine test type for each question set.
    2. Load existing results or initialize new ones.
    3. Process each model for each question set.
    4. Save and upload results.
    """
    forecast_due_date = os.getenv("FORECAST_DUE_DATE")
    if not forecast_due_date:
        logger.error(f"`forecast_due_date` was not set: {forecast_due_date}.")
        sys.exit(1)

    try:
        run_mode = constants.RunMode(os.getenv("TEST_OR_PROD"))
    except ValueError:
        logger.error("`run_mode` must be one of TEST or PROD.")
        sys.exit(1)

    try:
        task_num = int(os.getenv("CLOUD_RUN_TASK_INDEX"))
    except Exception as e:
        logger.error("ERROR: Unexpected error. Should not arrive here. Error message...")
        logger.error(e)
        sys.exit(1)

    sources = list(constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS_BY_SOURCE.keys())
    max_tasks = len(sources) if run_mode == constants.RunMode.PROD else 1
    if task_num >= max_tasks:
        logger.info(f"task number {task_num} not needed, winding down.")
        sys.exit(0)

    source = sources[task_num]
    models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS_BY_SOURCE[source]
    market_use_freeze_values = [False, True]
    prompt_types = ["zero_shot", "scratchpad"]
    num_questions = None
    if run_mode == constants.RunMode.TEST:
        num_questions = 1
        first_key = list(models.keys())[0]
        models = {first_key: models[first_key]}
        prompt_types = prompt_types[:1]

    base_file_path = f"individual_forecast_records/{forecast_due_date}"
    question_types = [
        "market",
        "non_market",
        "final",
        "final_with_freeze",
    ]

    results = {}
    models_to_test = list(models.keys())
    model_result_loaded = {model: False for model in models_to_test}

    # Sleep `task_num` seconds to avoid cloning the same git repo too quickly
    time.sleep(task_num)
    questions = get_questions(forecast_due_date=forecast_due_date, num_questions=num_questions)

    for prompt_type in prompt_types:
        for question_set in questions:
            for market_use_freeze_value in market_use_freeze_values:
                test_type = model_eval.determine_test_type(
                    question_set, prompt_type, market_use_freeze_value, run_mode
                )
                questions_to_eval = question_set
                for model in models_to_test:
                    gcp_file_path = f"{base_file_path}/{test_type}/{model}.jsonl"

                    results[model] = model_eval.download_and_read_saved_forecasts(
                        gcp_file_path, base_file_path
                    )

                    if results[model]:
                        model_result_loaded[model] = True
                        logger.info(f"Downloaded {gcp_file_path}.")
                    else:
                        logger.info(f"No results loaded for {gcp_file_path}.")
                        model_result_loaded[model] = False
                        results[model] = {i: "" for i in range(len(questions_to_eval))}

                for model in models_to_test:
                    if not model_result_loaded[model]:
                        logger.info(f"{model} is running inference...")
                        model_eval.process_model(
                            model,
                            models,
                            test_type,
                            results,
                            questions_to_eval,
                            forecast_due_date,
                            prompt_type,
                            market_use_freeze_value,
                            base_file_path,
                        )

        model_eval.generate_final_forecast_files(
            forecast_due_date=forecast_due_date,
            prompt_type=prompt_type,
            models=models,
            run_mode=run_mode,
        )
        delete_and_upload_to_the_cloud(
            base_file_path=base_file_path,
            prompt_type=prompt_type,
            question_types=question_types,
            forecast_due_date=forecast_due_date,
            run_mode=run_mode,
        )


if __name__ == "__main__":
    main()
