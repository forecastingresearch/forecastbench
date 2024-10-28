"""Run zero-shot and scratchpad evaluations for LLM models."""

import argparse
import json
import logging
import os
import pickle
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add necessary paths
current_path = os.getcwd()
sys.path.append(os.path.join(current_path, "../.."))

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import constants, data_utils, decorator, env, model_eval  # noqa: E402

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from utils import gcp  # noqa: E402

QUESTIONS_FILE = "latest-llm.json"
LOCAL_QUESTIONS_FILE = f"/tmp/{QUESTIONS_FILE}"


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


def load_questions_file():
    """Load the questions file."""
    logger.info(f"Downloading {QUESTIONS_FILE}...")
    gcp.storage.download_no_error_message_on_404(
        bucket_name=env.QUESTION_SETS_BUCKET,
        filename=QUESTIONS_FILE,
        local_filename=LOCAL_QUESTIONS_FILE,
    )

    with open(LOCAL_QUESTIONS_FILE, "r") as file:
        questions_data = json.load(file)
    return questions_data["forecast_due_date"]


def download_and_map_news(
    single_market, single_non_market, combo_market, combo_non_market, forecast_due_date
):
    """Download and map retrieved summaries back into questions."""
    # Don't provide baselines with news for now.
    return

    def load_news(question_id):
        """Download news for a given question ID."""
        news_filename = f"{forecast_due_date}/{question_id}.pickle"
        local_news_filname = "news/" + news_filename

        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_news_filname), exist_ok=True)
        if not os.path.exists(local_news_filname):
            gcp.storage.download_no_error_message_on_404(
                bucket_name=env.LLM_BASELINE_NEWS_BUCKET,
                filename=news_filename,
                local_filename=local_news_filname,
            )

            if not os.path.exists(local_news_filname):
                logger.info(f"Warning: News file not found for question ID {question_id}")
                return {}
        else:
            logger.info(f"Local news exist for {question_id}")

        try:
            with open(local_news_filname, "rb") as file:
                return pickle.load(file)
        except Exception as e:
            logger.info(f"Error loading news for question ID {question_id}: {str(e)}")
            return {}

    # Process single questions
    for question in single_market + single_non_market:
        question["news"] = load_news(question["id"])

    # Process combination questions
    for question in combo_market + combo_non_market:
        for sub_question in question.get("combination_of", []):
            sub_question["news"] = load_news(sub_question["id"])


def delete_and_upload_to_the_cloud(
    base_file_path, prompt_type, question_types, forecast_due_date, test_or_prod
):
    """Upload local forecast files to GCP and then delete them."""
    # submits the final forecasts to forecastbench-forecast-sets-dev
    local_directory = f"/tmp/{prompt_type}/final_submit"
    if test_or_prod == "TEST":
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
        if test_or_prod == "TEST":
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
            if test_or_prod == "TEST":
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


def run_evaluations(questions, models, forecast_due_date, test_or_prod):
    """Run zero-shot and scratchpad evaluations for given questions and models."""
    base_file_path = f"individual_forecast_records/{forecast_due_date}"
    question_types = [
        "market",
        "non_market",
        "combo_market",
        "combo_non_market",
        "final",
        "final_with_freeze",
    ]

    for prompt_type in ["zero_shot", "scratchpad"]:  # , "scratchpad_with_news"]:
        # to run superforecaster prompts
        # add 'superforecaster_with_news_1', 'superforecaster_with_news_2', 'superforecaster_with_news_3'
        # if test_or_prod == "PROD":
        #     if prompt_type == "scratchpad_with_news":
        #         models = constants.SCRATCHPAD_WITH_NEWS_MODELS
        #     elif "superforecaster" in prompt_type:
        #         models = constants.SUPERFORECASTER_WITH_NEWS_MODELS
        #     else:
        #         models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS

        logger.info(f"Running evaluation for models: {', '.join(models.keys())}")

        model_eval.process_questions_and_models(
            questions,
            models,
            prompt_type,
            base_file_path,
            forecast_due_date=forecast_due_date,
            market_use_freeze_value=False,
            test_or_prod=test_or_prod,
        )

        if "superforecaster" not in prompt_type:
            # Don't run market with freeze values for superforecaster prompts
            model_eval.process_questions_and_models(
                questions,
                models,
                prompt_type,
                base_file_path,
                forecast_due_date=forecast_due_date,
                market_use_freeze_value=True,
                test_or_prod=test_or_prod,
            )

        model_eval.generate_final_forecast_files(
            deadline=forecast_due_date,
            prompt_type=prompt_type,
            models=models,
            test_or_prod=test_or_prod,
        )
        delete_and_upload_to_the_cloud(
            base_file_path,
            prompt_type,
            question_types,
            forecast_due_date=forecast_due_date,
            test_or_prod=test_or_prod,
        )

@decorator.log_runtime
def main():
    """Execute the main evaluation process."""
    args = parse_arguments()

    forecast_due_date = load_questions_file()

    if args.mode == "TEST":
        selected_models = {
            model: constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS[model]
            for model in [
                "claude_3p5_sonnet",
            ]
            if model in constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        }
        num_questions = 1
    elif args.mode == "PROD":
        selected_models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        num_questions = None
    else:
        logger.error("Must pass one of TEST or PROD.")
        sys.exit(1)

    single_market, single_non_market, combo_market, combo_non_market = model_eval.process_questions(
        LOCAL_QUESTIONS_FILE, num_per_source=num_questions
    )

    # map news back to each question
    # download_and_map_news(
    #     single_market, single_non_market, combo_market, combo_non_market, forecast_due_date
    # )

    questions = [single_non_market, single_market, combo_market, combo_non_market]

    logger.info(
        f"Number of questions per question category: {'all' if num_questions is None else num_questions}"
    )

    run_evaluations(questions, selected_models, forecast_due_date, args.mode)

    os.remove(LOCAL_QUESTIONS_FILE)


if __name__ == "__main__":
    main()
