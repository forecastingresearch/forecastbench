"""Run zero-shot and scratchpad evaluations for LLM models."""

import argparse
import json
import logging
import os
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

# Import required modules
from helpers import constants, data_utils, env, model_eval  # noqa: E402
from utils import gcp  # noqa: E402

# Constants
QUESTIONS_FILE = "latest-llm.json"
REMOTE_BUCKET = env.QUESTION_SETS_BUCKET


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
            bucket_name=REMOTE_BUCKET,
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


def run_evaluations(questions, models, forecast_due_date):
    """Run zero-shot and scratchpad evaluations for given questions and models."""
    base_file_path = f"individual_forecast_records/{forecast_due_date}"
    question_types = ["market", "non_market", "combo_market", "combo_non_market", "final"]

    for prompt_type in ["zero_shot", "scratchpad"]:
        model_eval.process_questions_and_models(
            questions,
            models,
            prompt_type,
            base_file_path,
            forecast_due_date=forecast_due_date,
            market_use_freeze_value=False,
        )
        model_eval.process_questions_and_models(
            questions,
            models,
            prompt_type,
            base_file_path,
            forecast_due_date=forecast_due_date,
            market_use_freeze_value=True,
        )
        model_eval.generate_final_forecast_files(
            deadline=forecast_due_date, prompt_type=prompt_type, models=models
        )
        data_utils.delete_and_upload_to_the_cloud(base_file_path, prompt_type, question_types)


def main():
    """Execute the main evaluation process."""
    args = parse_arguments()

    install_dependencies()
    download_questions_file()
    _, forecast_due_date = load_questions_data()

    if args.mode == "TEST":
        selected_models = {
            model: constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS[model]
            for model in ["gpt_3p5_turbo_0125", "claude_3p5_sonnet", "gemini_1p5_flash"]
            if model in constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        }
        num_questions = 2
    elif args.mode == "PROD":  # ALL mode
        selected_models = constants.ZERO_SHOT_AND_SCRATCHPAD_MODELS
        num_questions = (
            3  # Will change it to None, which will process all questions when this code is ready.
        )

    single_market, single_non_market, combo_market, combo_non_market = model_eval.process_questions(
        QUESTIONS_FILE, num_per_source=num_questions
    )
    questions = [single_non_market, single_market, combo_market, combo_non_market]

    logger.info(f"Running evaluation for models: {', '.join(selected_models.keys())}")
    logger.info(
        f"Number of questions per question category: {'all' if num_questions is None else num_questions}"
    )

    run_evaluations(questions, selected_models, forecast_due_date)

    # remove local files
    os.remove(QUESTIONS_FILE)


if __name__ == "__main__":
    main()
