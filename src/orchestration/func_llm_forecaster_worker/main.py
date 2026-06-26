"""Cloud Run worker for ForecastBench LLM forecasts."""

import logging
import os

from helpers import dates, decorator
from helpers.run_mode import RunMode
from llm_forecaster import fb_model_runs, runner
from llm_forecaster.question_set import (
    QuestionSet,
    limit_questions_for_test_mode,
    split_questions,
)
from orchestration import _io, _llm_forecaster_io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_TEST_QUESTIONS_PER_TYPE = 2
LOCAL_OUTPUT_DIR = "/tmp/forecasts/llm_forecaster"


def parse_env_vars() -> tuple[str, RunMode, fb_model_runs.ModelRun]:
    """Parse Cloud Run worker environment variables."""
    forecast_due_date = os.getenv("FORECAST_DUE_DATE")
    if not forecast_due_date:
        raise ValueError("FORECAST_DUE_DATE must be set.")

    task_num_raw = os.getenv("CLOUD_RUN_TASK_INDEX")
    if task_num_raw is None:
        raise ValueError("CLOUD_RUN_TASK_INDEX must be set.")

    task_num = int(task_num_raw)
    if task_num < 0 or task_num >= len(fb_model_runs.FB_MODEL_RUNS):
        raise ValueError(
            f"CLOUD_RUN_TASK_INDEX must be between 0 and {len(fb_model_runs.FB_MODEL_RUNS) - 1}; "
            f"got {task_num}."
        )
    model_run = fb_model_runs.FB_MODEL_RUNS[task_num]

    for index, available_model_run in enumerate(fb_model_runs.FB_MODEL_RUNS):
        marker = "🌟️ running 🌟️ " if index == task_num else ""
        logger.info(f"{index}: {marker}{available_model_run}")

    run_mode = RunMode.from_string(os.getenv("TEST_OR_PROD"))

    return forecast_due_date, run_mode, model_run


def _limit_question_set_for_test_mode(
    question_set: QuestionSet,
) -> QuestionSet:
    dataset_questions, market_questions = split_questions(question_set.questions)
    dataset_questions, market_questions = limit_questions_for_test_mode(
        dataset_questions,
        market_questions,
        DEFAULT_TEST_QUESTIONS_PER_TYPE,
    )
    return QuestionSet(
        forecast_due_date=question_set.forecast_due_date,
        question_set_filename=question_set.question_set_filename,
        questions=dataset_questions + market_questions,
    )


def _written_forecast_file_summary(
    written_file: _llm_forecaster_io.WrittenForecastFile,
) -> str:
    return (
        f"{written_file.variant.key} -> {written_file.local_filename} "
        f"({len(written_file.rows)} rows)"
    )


def load_question_set(forecast_due_date: str) -> QuestionSet:
    """Load one LLM question set through the orchestration IO boundary."""
    filename = f"{forecast_due_date}-llm.json"
    data = _io.read_question_set_json(filename)
    return QuestionSet.from_question_set_json(data)


def _all_remote_final_files_exist_message(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    is_test: bool,
) -> str | None:
    """Return a warning message only if every remote target final forecast file exists."""
    existing_file_messages = []
    for destination_blob_name in _llm_forecaster_io.final_forecast_set_destination_blob_names(
        model_run=model_run,
        question_set=question_set,
        is_test=is_test,
    ):
        if not _io.forecast_file_exists(destination_blob_name):
            return None
        existing_file_messages.append(
            f"Remote forecast file already exists: {destination_blob_name}"
        )

    return f"All final forecast files already exist: {'; '.join(existing_file_messages)}"


def _upload_written_forecast_file(
    written_file: _llm_forecaster_io.WrittenForecastFile,
) -> None:
    _llm_forecaster_io.upload_written_forecast_file(written_file)


def _write_and_upload_forecast_result(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    output_dir: str,
    forecast_result: runner.ForecastResult,
    is_test: bool,
) -> _llm_forecaster_io.WrittenForecastFile:
    written_file = _llm_forecaster_io.write_final_forecast_file(
        model_run=model_run,
        question_set=question_set,
        output_dir=output_dir,
        forecast_result=forecast_result,
        is_test=is_test,
    )
    logger.info(
        f"Wrote LLM forecast file variant={written_file.variant.key} "
        f"rows={len(written_file.rows)} forecast_file={written_file.local_filename}"
    )
    _upload_written_forecast_file(written_file)
    return written_file


def _upload_llm_call_transcripts(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    output_dir: str,
    is_test: bool,
) -> None:
    """Upload existing transcript files without failing the forecast run."""
    for target in runner.llm_call_transcript_upload_targets(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        output_dir=output_dir,
        is_test=is_test,
    ):
        if not target.local_filename.exists():
            continue
        try:
            _llm_forecaster_io.upload_llm_call_transcript(
                local_filename=target.local_filename,
                filename=target.destination_blob_name,
            )
        except Exception:
            logger.exception(
                f"Failed to upload LLM call transcript file={target.local_filename} "
                f"destination={target.destination_blob_name}. "
                "Transcript contents were not logged."
            )
            continue

        logger.info(f"Uploaded LLM call transcript file={target.destination_blob_name}")


def _log_written_forecast_files(
    written_files: list[_llm_forecaster_io.WrittenForecastFile],
) -> None:
    file_summaries = [
        _written_forecast_file_summary(written_file) for written_file in written_files
    ]
    logger.info(
        f"Wrote and uploaded {len(written_files)} LLM forecast files: "
        f"{'; '.join(file_summaries)}"
    )


def run_worker(
    forecast_due_date: str,
    run_mode: RunMode,
    model_run: fb_model_runs.ModelRun,
) -> None:
    """Run one ForecastBench LLM model task."""
    logger.info(f"Loading LLM question set for forecast due date {forecast_due_date}.")
    question_set = load_question_set(forecast_due_date)
    logger.info(
        f"Loaded {len(question_set.questions)} questions from {question_set.question_set_filename}."
    )

    if run_mode.is_test:
        logger.info(
            f"Limiting TEST run to {DEFAULT_TEST_QUESTIONS_PER_TYPE} dataset questions and "
            f"{DEFAULT_TEST_QUESTIONS_PER_TYPE} market questions."
        )
        question_set = _limit_question_set_for_test_mode(question_set)
        logger.info(f"TEST run will forecast {len(question_set.questions)} questions.")

    remote_existing_file_message = _all_remote_final_files_exist_message(
        model_run=model_run,
        question_set=question_set,
        is_test=run_mode.is_test,
    )
    if remote_existing_file_message is not None:
        logger.warning(
            f"{remote_existing_file_message}. Exiting LLM forecast run without overwriting."
        )
        return

    fb_model_runs.configure_and_validate_provider_keys([model_run])
    logger.info("Starting LLM forecast runner.")
    written_files = []
    try:
        for forecast_result in runner.iter_model_forecasts(
            model_run=model_run,
            question_set=question_set,
            output_dir=LOCAL_OUTPUT_DIR,
            is_test=run_mode.is_test,
            today_date=dates.get_date_today_as_iso(),
        ):
            written_file = _write_and_upload_forecast_result(
                model_run=model_run,
                question_set=question_set,
                output_dir=LOCAL_OUTPUT_DIR,
                forecast_result=forecast_result,
                is_test=run_mode.is_test,
            )
            written_files.append(written_file)
    finally:
        _upload_llm_call_transcripts(
            forecast_due_date=question_set.forecast_due_date,
            model_run=model_run,
            output_dir=LOCAL_OUTPUT_DIR,
            is_test=run_mode.is_test,
        )
    _log_written_forecast_files(written_files)


@decorator.log_runtime
def main() -> None:
    """Parse environment and run the selected LLM forecaster task."""
    forecast_due_date, run_mode, model_run = parse_env_vars()
    logger.info(f"Running {run_mode.value} LLM forecaster worker for {model_run.slug}.")
    run_worker(
        forecast_due_date=forecast_due_date,
        run_mode=run_mode,
        model_run=model_run,
    )


if __name__ == "__main__":
    main()
