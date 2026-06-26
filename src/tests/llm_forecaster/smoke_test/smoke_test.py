"""Run a full-path smoke test for configured ForecastBench LLM model runs.

Run from the repository root:

    UTILS_REPO="../utils"  # Change this if your utils checkout is elsewhere.
    PYTHONPATH="${UTILS_REPO}:src" \
        python -m src.tests.llm_forecaster.smoke_test.smoke_test \
        --forecast-due-date 2026-06-07 \
        --sample-size 2 \
        --model-run gpt-5-mini-2025-08-07-run-variant-01

Omit --model-run to test all configured ForecastBench model runs. Omit
--forecast-due-date to use the latest LLM question-set metadata.
"""

import argparse
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

logger = logging.getLogger(__name__)

PASS = "PASS"
FAIL = "FAIL"
SMOKE_OUTPUT_DIR = "/tmp/forecasts/llm_smoke_test"


@dataclass(frozen=True)
class SmokeResult:
    """Result for one model smoke check."""

    model_name: str
    lab: str
    provider: str
    status: str
    error_type: str
    error_message: str
    runtime_seconds: float | None = None


@dataclass(frozen=True)
class SmokeRun:
    """Smoke-test results and local forecast rows."""

    results: list[SmokeResult]
    forecast_file_paths: list[str]


def select_questions(questions: Sequence[dict], sample_size: int) -> list[dict]:
    """Return deterministic dataset and market prefixes for smoke coverage."""
    if sample_size < 1:
        raise ValueError("sample_size must be at least 1.")

    from sources import DATASET_SOURCE_NAMES, MARKET_SOURCE_NAMES

    dataset_source_names = set(DATASET_SOURCE_NAMES)
    market_source_names = set(MARKET_SOURCE_NAMES)
    dataset_questions = []
    market_questions = []
    unknown_sources = set()

    for question in questions:
        source = question.get("source")
        if source in dataset_source_names:
            dataset_questions.append(question)
        elif source in market_source_names:
            market_questions.append(question)
        else:
            unknown_sources.add(source)

    if unknown_sources:
        sources = ", ".join(sorted(str(source) for source in unknown_sources))
        raise ValueError(f"Unknown question sources: {sources}")

    selected_questions = (
        sorted(dataset_questions, key=lambda question: question["id"])[:sample_size]
        + sorted(market_questions, key=lambda question: question["id"])[:sample_size]
    )
    if not selected_questions:
        raise ValueError("No questions selected for smoke test.")
    return selected_questions


def select_model_runs(
    model_runs: Sequence[Any],
    model_run_keys: Sequence[str] | None,
    available_model_runs: Sequence[Any] | None = None,
) -> list[Any]:
    """Return configured runs by default or exact-key requested runs."""
    if model_run_keys is None:
        return list(model_runs)

    candidate_model_runs = model_runs if available_model_runs is None else available_model_runs
    requested_model_run_keys = set(model_run_keys)
    selected_model_runs = [
        model_run
        for model_run in candidate_model_runs
        if model_run.model_run_key in requested_model_run_keys
    ]
    selected_model_run_keys = {model_run.model_run_key for model_run in selected_model_runs}
    missing_model_run_keys = sorted(requested_model_run_keys - selected_model_run_keys)
    if not missing_model_run_keys:
        return selected_model_runs

    available_model_run_keys = ", ".join(
        model_run.model_run_key for model_run in candidate_model_runs
    )
    raise ValueError(
        f"Unknown model run key(s) {missing_model_run_keys}. Available model run keys: "
        f"{available_model_run_keys}"
    )


def exit_code_for_results(results: Sequence[SmokeResult]) -> int:
    """Return a process exit code for smoke-test results."""
    if not results:
        return 1
    return 1 if any(result.status == FAIL for result in results) else 0


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the smoke test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--forecast-due-date",
        default=os.getenv("FORECAST_DUE_DATE"),
        help="Forecast due date to smoke test. Defaults to FORECAST_DUE_DATE or latest LLM set.",
    )
    parser.add_argument(
        "--sample-size",
        default=2,
        type=int,
        help="Number of deterministic questions to test.",
    )
    parser.add_argument(
        "--model-run",
        nargs="+",
        default=None,
        help=(
            "One or more exact ModelRun.model_run_key values to test. "
            "Defaults to all configured runs."
        ),
    )
    parser.add_argument(
        "--run-locally",
        action="store_true",
        help="Read question-set metadata and questions from local files.",
    )
    return parser.parse_args()


def _get_io_module() -> Any:
    """Return orchestration IO module without importing it at smoke_test import time."""
    from orchestration import _io

    return _io


def _get_fb_model_runs_module() -> Any:
    """Return model-run declarations without importing them at smoke_test import time."""
    from llm_forecaster import fb_model_runs

    return fb_model_runs


def _get_shared_model_runs_module() -> Any:
    """Return shared model-run registry without importing it at smoke_test import time."""
    from utils.llm import model_runs

    return model_runs


def _get_question_set_module() -> Any:
    """Return question-set helpers without importing them at smoke_test import time."""
    from llm_forecaster import question_set

    return question_set


def _get_runner() -> Any:
    """Return runner module without importing it at smoke_test import time."""
    from llm_forecaster import runner

    return runner


def _get_llm_forecaster_io_module() -> Any:
    """Return LLM forecast IO helpers without importing them at smoke_test import time."""
    from orchestration import _llm_forecaster_io

    return _llm_forecaster_io


def _new_output_dir() -> Path:
    """Return a unique smoke output directory for one CLI invocation."""
    return Path(SMOKE_OUTPUT_DIR) / uuid.uuid4().hex


def _result_for(
    model_run: Any,
    status: str,
    error_type: str = "",
    error_message: str = "",
    runtime_seconds: float | None = None,
) -> SmokeResult:
    """Build a normalized smoke-test result row."""
    return SmokeResult(
        model_name=model_run.slug,
        lab=model_run.lab.name,
        provider=model_run.provider.name,
        status=status,
        error_type=error_type,
        error_message=error_message,
        runtime_seconds=runtime_seconds,
    )


def _expected_rows_per_forecast_file(question_set: Any) -> int:
    """Return expected forecast row count for one written forecast file."""
    from sources import DATASET_SOURCE_NAMES

    expected_rows = 0
    for question in question_set.questions:
        if question["source"] in DATASET_SOURCE_NAMES:
            expected_rows += len(question["resolution_dates"])
        else:
            expected_rows += 1
    return expected_rows


def _partial_forecast_error_message(written_files: Sequence[Any], expected_rows: int) -> str:
    """Return a compact row-count mismatch message for partial smoke outputs."""
    row_counts = [
        f"{written_file.local_filename} has {len(written_file.rows)}"
        for written_file in written_files
        if len(written_file.rows) != expected_rows
    ]
    return f"Expected {expected_rows} rows per forecast file; {'; '.join(row_counts)}."


def run_smoke_test(
    model_runs: Sequence[Any],
    question_set: Any,
    output_dir: str | Path = SMOKE_OUTPUT_DIR,
) -> SmokeRun:
    """Run every selected model through the ForecastBench LLM runner path."""
    results = []
    forecast_file_paths = []
    runner = _get_runner()
    forecast_io = _get_llm_forecaster_io_module()
    expected_rows = _expected_rows_per_forecast_file(question_set)

    for model_run in model_runs:
        start_time = perf_counter()

        try:
            forecast_results = runner.run_model(
                model_run=model_run,
                question_set=question_set,
                output_dir=output_dir,
                is_test=True,
                raise_on_question_error=False,
            )
            written_files = [
                forecast_io.write_final_forecast_file(
                    model_run=model_run,
                    question_set=question_set,
                    output_dir=output_dir,
                    forecast_result=forecast_result,
                    is_test=True,
                )
                for forecast_result in forecast_results
            ]
            runtime_seconds = perf_counter() - start_time
            forecast_file_paths.extend(
                str(written_file.local_filename) for written_file in written_files
            )
            row_count = sum(len(written_file.rows) for written_file in written_files)
            partial_files = [
                written_file
                for written_file in written_files
                if len(written_file.rows) != expected_rows
            ]
            if row_count and not partial_files:
                results.append(
                    _result_for(
                        model_run=model_run,
                        status=PASS,
                        runtime_seconds=runtime_seconds,
                    )
                )
            elif not row_count:
                results.append(
                    _result_for(
                        model_run=model_run,
                        status=FAIL,
                        error_type="EmptyForecast",
                        error_message="No forecast rows returned by runner.",
                        runtime_seconds=runtime_seconds,
                    )
                )
            else:
                results.append(
                    _result_for(
                        model_run=model_run,
                        status=FAIL,
                        error_type="PartialForecast",
                        error_message=_partial_forecast_error_message(
                            written_files,
                            expected_rows,
                        ),
                        runtime_seconds=runtime_seconds,
                    )
                )
        except Exception as exc:
            runtime_seconds = perf_counter() - start_time
            results.append(
                _result_for(
                    model_run=model_run,
                    status=FAIL,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    runtime_seconds=runtime_seconds,
                )
            )

    return SmokeRun(results=results, forecast_file_paths=forecast_file_paths)


def _sample_question_set(question_set: Any, sample_size: int) -> Any:
    """Return a question set containing only sampled smoke questions."""
    question_set_module = _get_question_set_module()
    return question_set_module.QuestionSet(
        forecast_due_date=question_set.forecast_due_date,
        question_set_filename=question_set.question_set_filename,
        questions=select_questions(question_set.questions, sample_size=sample_size),
    )


def _log_results(smoke_run: SmokeRun) -> None:
    """Log compact smoke-test result rows and runtime timing."""
    for result in smoke_run.results:
        logger.info(
            f"model={result.model_name} lab={result.lab} provider={result.provider} "
            f"status={result.status} error_type={result.error_type} "
            f"error={result.error_message}"
        )

    for result in smoke_run.results:
        logger.info(
            f"timing model={result.model_name} "
            f"runtime_seconds={_format_duration(result.runtime_seconds)}"
        )


def _format_duration(seconds: float | None) -> str:
    """Return compact seconds text for smoke-test logs."""
    if seconds is None:
        return "NA"
    return f"{seconds:.3f}"


def main() -> None:
    """Load questions, validate providers, run smoke checks, and exit."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    forecast_due_date = args.forecast_due_date
    _io = _get_io_module()
    if forecast_due_date is None:
        metadata = _io.get_latest_llm_question_set_metadata(run_locally=args.run_locally)
        forecast_due_date = metadata["forecast_due_date"]

    question_set_module = _get_question_set_module()
    question_set_data = _io.read_question_set_json(
        f"{forecast_due_date}-llm.json",
        run_locally=args.run_locally,
    )
    question_set = question_set_module.QuestionSet.from_question_set_json(question_set_data)
    sampled_question_set = _sample_question_set(question_set, sample_size=args.sample_size)
    fb_model_runs_module = _get_fb_model_runs_module()
    shared_model_runs_module = _get_shared_model_runs_module()
    selected_model_runs = select_model_runs(
        model_runs=fb_model_runs_module.FB_MODEL_RUNS,
        model_run_keys=args.model_run,
        available_model_runs=shared_model_runs_module.MODEL_RUNS,
    )

    fb_model_runs_module.configure_and_validate_provider_keys(selected_model_runs)
    smoke_run = run_smoke_test(
        model_runs=selected_model_runs,
        question_set=sampled_question_set,
        output_dir=_new_output_dir(),
    )
    _log_results(smoke_run)
    sys.exit(exit_code_for_results(smoke_run.results))


if __name__ == "__main__":
    main()
