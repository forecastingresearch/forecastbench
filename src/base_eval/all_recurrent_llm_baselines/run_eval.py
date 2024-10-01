"""Run zero-shot and scratchpad evaluations for LLM models."""

import argparse
import json
import logging
import os
import pickle
import subprocess
import sys

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add necessary paths
current_path = os.getcwd()
sys.path.append(os.path.join(current_path, "../.."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from helpers import constants, data_utils, env, model_eval  # noqa: E402
from utils import gcp  # noqa: E402

QUESTIONS_FILE = "latest-llm.json"
QUESTION_SET_BUCKET = env.QUESTION_SETS_BUCKET
LLM_BASELINE_NEWS_BUCKET = env.LLM_BASELINE_NEWS_BUCKET


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


def install_dependencies():
    """Install required packages from requirements.txt."""
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "pip", "--root-user-action=ignore"]
    )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            "requirements.txt",
            "--root-user-action=ignore",
        ]
    )


def download_questions_file():
    """Download the questions file if it doesn't exist locally."""
    if not os.path.exists(QUESTIONS_FILE):
        gcp.storage.download_no_error_message_on_404(
            bucket_name=QUESTION_SET_BUCKET,
            filename=QUESTIONS_FILE,
            local_filename=QUESTIONS_FILE,
        )
    else:
        print(f"{QUESTIONS_FILE} already exists.")


def load_questions_data():
    """Load and return questions and forecast due date from the JSON file."""
    with open(QUESTIONS_FILE, "r") as file:
        questions_data = json.load(file)
    return questions_data["questions"], questions_data["forecast_due_date"]


def download_and_map_news(
    single_market, single_non_market, combo_market, combo_non_market, forecast_due_date
):
    """Download and map retrieved summaries back into questions."""

    def load_news(question_id):
        """Download news for a given question ID."""
        news_filename = f"{forecast_due_date}/{question_id}.pickle"
        local_news_filname = "news/" + news_filename

        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_news_filname), exist_ok=True)
        if not os.path.exists(local_news_filname):
            gcp.storage.download_no_error_message_on_404(
                bucket_name=LLM_BASELINE_NEWS_BUCKET,
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

    for prompt_type in ["zero_shot", "scratchpad", "scratchpad_with_news"]:
        # to run superforecaster prompts
        # add 'superforecaster_with_news_1', 'superforecaster_with_news_2', 'superforecaster_with_news_3'
        if test_or_prod != "TEST":
            if prompt_type == "scratchpad_with_news":
                models = constants.SCRATCHPAD_WITH_NEWS_MODELS
            elif "superforecaster" in prompt_type:
                models = constants.SUPERFORECASTER_WITH_NEWS_MODELS
            else:
                models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS

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
        data_utils.delete_and_upload_to_the_cloud(
            base_file_path, prompt_type, question_types, test_or_prod=test_or_prod
        )


def main():
    """Execute the main evaluation process."""
    args = parse_arguments()

    install_dependencies()
    download_questions_file()
    _, forecast_due_date = load_questions_data()

    if args.mode == "TEST":
        selected_models = {
            model: constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS[model]
            for model in ["claude_3p5_sonnet"]  # Add more models to test here
            if model in constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        }
        num_questions = 2
    elif args.mode == "PROD":  # ALL mode
        selected_models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        num_questions = None

    single_market, single_non_market, combo_market, combo_non_market = model_eval.process_questions(
        QUESTIONS_FILE, num_per_source=num_questions
    )

    # map news back to each question
    download_and_map_news(
        single_market, single_non_market, combo_market, combo_non_market, forecast_due_date
    )

    questions = [single_non_market, single_market, combo_market, combo_non_market]

    logger.info(
        f"Number of questions per question category: {'all' if num_questions is None else num_questions}"
    )

    run_evaluations(questions, selected_models, forecast_due_date, args.mode)

    # remove the local question file
    os.remove(QUESTIONS_FILE)


if __name__ == "__main__":
    main()
