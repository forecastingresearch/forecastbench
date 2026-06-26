"""Run ForecastBench LLM forecast generation."""

import logging
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Final

import pandas as pd
from pandera.typing import DataFrame

from _schemas import ForecastFrame
from helpers import dates
from llm_forecaster import fb_model_runs, output, parsing, prompts
from llm_forecaster.forecast_variants import (
    DATASET_FORECAST_SHARING_VARIANT_GROUPS,
    ZERO_SHOT,
    ZERO_SHOT_WITH_FREEZE_VALUES,
    ForecastVariant,
)
from llm_forecaster.model_run_transcripts import (
    LLMCallTranscript,
    TranscriptRecordingModelRun,
    TranscriptUploadTarget,
)
from llm_forecaster.question_set import QuestionSet, split_questions
from sources import DATASET_SOURCE_NAMES

logger = logging.getLogger(__name__)

DATASET_PROMPTS_BY_VARIANT: Final[dict[ForecastVariant, str]] = {
    ZERO_SHOT: prompts.ZERO_SHOT_DATASET_PROMPT,
}
MARKET_PROMPTS_BY_VARIANT: Final[dict[ForecastVariant, str]] = {
    ZERO_SHOT: prompts.ZERO_SHOT_MARKET_PROMPT,
    ZERO_SHOT_WITH_FREEZE_VALUES: prompts.ZERO_SHOT_MARKET_WITH_FREEZE_VALUE_PROMPT,
}


QuestionForecastFn = Callable[[dict[str, Any]], list[dict[str, Any]]]


@dataclass(frozen=True)
class ForecastResult:
    """Validated forecast rows for one output variant."""

    variant: ForecastVariant
    rows: DataFrame[ForecastFrame]


def _llm_call_transcript_base_filename(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    is_test: bool,
) -> str:
    """Return the local transcript filename stem shared by all transcript formats."""
    return output.llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )


def _formatted_question(question: dict[str, Any], forecast_due_date: str) -> str:
    # Only format dataset questions here, as market questions don't need extra formatting as they
    # come directly from the market source.
    if question["source"] not in DATASET_SOURCE_NAMES:
        return question["question"]

    return question["question"].format(
        forecast_due_date=forecast_due_date,
        resolution_date="each of the resolution dates provided below",
    )


def _background(question: dict[str, Any]) -> str:
    background = question["background"]
    if question["market_info_resolution_criteria"] != "N/A":
        background += "\n" + question["market_info_resolution_criteria"]
    return background


def _prompt_params(
    question: dict[str, Any],
    forecast_due_date: str,
    today_date: str,
    variant: ForecastVariant,
) -> dict[str, Any]:
    params = {
        "question": _formatted_question(
            question=question,
            forecast_due_date=forecast_due_date,
        ),
        "background": _background(question=question),
        "resolution_criteria": question["resolution_criteria"],
        "today_date": today_date,
    }

    if question["source"] in DATASET_SOURCE_NAMES:
        params.update(
            {
                "freeze_datetime": question["freeze_datetime"],
                "freeze_datetime_value": question["freeze_datetime_value"],
                "freeze_datetime_value_explanation": question["freeze_datetime_value_explanation"],
                "list_of_resolution_dates": question["resolution_dates"],
            }
        )
        return params

    params["resolution_date"] = question["market_info_close_datetime"]
    if variant.market_prompt_uses_freeze_values:
        params.update(
            {
                "freeze_datetime": question["freeze_datetime"],
                "freeze_datetime_value": question["freeze_datetime_value"],
            }
        )
    return params


def _prompt_template(question: dict[str, Any], variant: ForecastVariant) -> str:
    prompt_templates = (
        DATASET_PROMPTS_BY_VARIANT
        if question["source"] in DATASET_SOURCE_NAMES
        else MARKET_PROMPTS_BY_VARIANT
    )
    try:
        return prompt_templates[variant]
    except KeyError as exc:
        question_type = "dataset" if question["source"] in DATASET_SOURCE_NAMES else "market"
        raise KeyError(f"No {question_type} prompt template for {variant.key}") from exc


def render_prompt(
    question: dict[str, Any],
    forecast_due_date: str,
    today_date: str,
    variant: ForecastVariant,
) -> str:
    """Render the prompt for a question and variant."""
    prompt_template = _prompt_template(
        question=question,
        variant=variant,
    )
    params = _prompt_params(
        question=question,
        forecast_due_date=forecast_due_date,
        today_date=today_date,
        variant=variant,
    )

    return prompts.render_template(
        template=prompt_template,
        params=params,
    )


def _handle_question_error(
    question: dict[str, Any],
    raise_on_question_error: bool,
    response: str | None = None,
) -> None:
    """Raise or log an exception from an individual question forecast."""
    question_id = question.get("id")
    if response is None:
        logger.exception(f"Skipping LLM forecast question after error: {question_id}")
    else:
        response_for_log = response if response.strip() else repr(response)
        logger.exception(
            f"Skipping LLM forecast question after error: {question_id}\n"
            f"LLM response before error for {question_id}:\n{response_for_log}"
        )
    if raise_on_question_error:
        raise


def _max_workers_for_questions(model_run: fb_model_runs.ModelRun, question_count: int) -> int:
    """Return the bounded question-level worker count for one model run."""
    provider_max_workers = fb_model_runs.PROVIDER_MAX_WORKERS.get(
        model_run.provider,
        fb_model_runs.DEFAULT_PROVIDER_MAX_WORKERS,
    )
    return max(1, min(question_count, provider_max_workers))


def _forecast_questions(
    model_run: fb_model_runs.ModelRun,
    questions_to_forecast: list[dict[str, Any]],
    forecast_question: QuestionForecastFn,
    progress_label: str,
) -> DataFrame[ForecastFrame] | None:
    """Forecast questions concurrently while preserving input question order."""
    total_questions = len(questions_to_forecast)
    logger.info(f"LLM forecast phase {progress_label} starting: {total_questions} question(s).")
    completed_questions = 0
    progress_lock = Lock()

    def forecast_question_with_progress(question: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal completed_questions
        try:
            return forecast_question(question)
        finally:
            with progress_lock:
                completed_questions += 1
                logger.info(
                    f"LLM forecast progress phase={progress_label} "
                    f"completed={completed_questions}/{total_questions} "
                    f"question_id={question.get('id')}"
                )

    max_workers = _max_workers_for_questions(
        model_run=model_run,
        question_count=total_questions,
    )
    if max_workers == 1:
        question_rows = [
            forecast_question_with_progress(question) for question in questions_to_forecast
        ]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            question_rows = list(
                executor.map(forecast_question_with_progress, questions_to_forecast)
            )
    return _validated_forecast_frame_from_rows(rows=[row for rows in question_rows for row in rows])


def _forecast_dataset_questions(
    model_run: fb_model_runs.ModelRun,
    dataset_questions: list[dict[str, Any]],
    forecast_due_date: str,
    today_date: str,
    variant: ForecastVariant,
    transcript: LLMCallTranscript,
    raise_on_question_error: bool = False,
) -> DataFrame[ForecastFrame] | None:
    """Forecast dataset questions once and return one row per resolution date."""

    def forecast_question(question: dict[str, Any]) -> list[dict[str, Any]]:
        question_id = question["id"]
        source = question["source"]
        resolution_dates = question["resolution_dates"]
        expected_forecasts = len(resolution_dates)
        response = None
        try:
            prompt = render_prompt(
                question=question,
                forecast_due_date=forecast_due_date,
                today_date=today_date,
                variant=variant,
            )
            response = TranscriptRecordingModelRun(
                model_run=model_run,
                transcript=transcript,
                question=question,
                variant=variant,
                role="forecast",
                expected_forecasts=expected_forecasts,
            ).get_response(prompt)
            forecasts = parsing.parse_dataset_forecast(
                response=response,
                question=question,
                forecast_extraction_model=TranscriptRecordingModelRun(
                    model_run=fb_model_runs.FORECAST_EXTRACTION_MODEL,
                    transcript=transcript,
                    question=question,
                    variant=variant,
                    role="extract",
                    expected_forecasts=expected_forecasts,
                ),
            )
            if len(forecasts) != len(resolution_dates):
                raise ValueError(
                    f"Expected {len(resolution_dates)} dataset forecasts for "
                    f"{question_id}, got {len(forecasts)}"
                )
        except Exception:
            _handle_question_error(
                question=question,
                raise_on_question_error=raise_on_question_error,
                response=response,
            )
            return []
        return [
            {
                "id": question_id,
                "source": source,
                "forecast": forecast,
                "resolution_date": resolution_date,
                "reasoning": "",
            }
            for forecast, resolution_date in zip(forecasts, resolution_dates)
        ]

    return _forecast_questions(
        model_run=model_run,
        questions_to_forecast=dataset_questions,
        forecast_question=forecast_question,
        progress_label="dataset",
    )


def _forecast_market_questions(
    model_run: fb_model_runs.ModelRun,
    market_questions: list[dict[str, Any]],
    forecast_due_date: str,
    today_date: str,
    variant: ForecastVariant,
    transcript: LLMCallTranscript,
    raise_on_question_error: bool = False,
) -> DataFrame[ForecastFrame] | None:
    """Forecast market questions for one variant."""

    def forecast_question(question: dict[str, Any]) -> list[dict[str, Any]]:
        question_id = question["id"]
        source = question["source"]
        expected_forecasts = 1
        response = None
        try:
            prompt = render_prompt(
                question=question,
                forecast_due_date=forecast_due_date,
                today_date=today_date,
                variant=variant,
            )
            response = TranscriptRecordingModelRun(
                model_run=model_run,
                transcript=transcript,
                question=question,
                variant=variant,
                role="forecast",
                expected_forecasts=expected_forecasts,
            ).get_response(prompt)
            forecast = parsing.parse_market_forecast(
                response=response,
                forecast_extraction_model=TranscriptRecordingModelRun(
                    model_run=fb_model_runs.FORECAST_EXTRACTION_MODEL,
                    transcript=transcript,
                    question=question,
                    variant=variant,
                    role="extract",
                    expected_forecasts=expected_forecasts,
                ),
            )
        except Exception:
            _handle_question_error(
                question=question,
                raise_on_question_error=raise_on_question_error,
                response=response,
            )
            return []
        return [
            {
                "id": question_id,
                "source": source,
                "forecast": forecast,
                "resolution_date": None,
                "reasoning": "",
            }
        ]

    return _forecast_questions(
        model_run=model_run,
        questions_to_forecast=market_questions,
        forecast_question=forecast_question,
        progress_label=variant.key,
    )


def _validated_forecast_frame_from_rows(
    rows: Sequence[dict[str, Any]],
) -> DataFrame[ForecastFrame] | None:
    if not rows:
        return None
    frame = pd.DataFrame(rows, columns=list(ForecastFrame.to_schema().columns))
    return ForecastFrame.validate(frame)


def _sorted_forecast_rows(rows: DataFrame[ForecastFrame]) -> DataFrame[ForecastFrame]:
    sorted_frame = (
        rows.assign(_resolution_date_sort=rows["resolution_date"].fillna(""))
        .sort_values(
            by=["source", "id", "_resolution_date_sort"],
            kind="mergesort",
        )
        .drop(columns=["_resolution_date_sort"])
        .reset_index(drop=True)
    )
    return sorted_frame


def _expected_dataset_forecast_count(dataset_questions: list[dict[str, Any]]) -> int:
    """Return the total dataset forecast rows expected across all horizons."""
    return sum(len(question["resolution_dates"]) for question in dataset_questions)


def _forecast_summary_label(label: str) -> str:
    """Return the compact human-readable label used in final runner logs."""
    return label.replace("-", " ")


def _log_forecast_success_counts(
    market_success_counts: dict[ForecastVariant, int],
    market_expected_count: int,
    dataset_success_count: int,
    dataset_expected_count: int,
) -> None:
    """Log final successful forecast counts by market variant and dataset rows."""
    for variant_group in DATASET_FORECAST_SHARING_VARIANT_GROUPS:
        for variant in variant_group.output_variants:
            summary_label = _forecast_summary_label(label=variant.key)
            logger.info(
                f"LLM forecast success {summary_label}: "
                f"{market_success_counts[variant]}/{market_expected_count}"
            )

    logger.info(f"LLM forecast success dataset: {dataset_success_count}/{dataset_expected_count}")


def _llm_call_transcript_local_filenames(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    output_dir: str | Path,
    is_test: bool,
) -> tuple[Path, Path]:
    base_filename = Path(output_dir) / _llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return (
        Path(f"{base_filename}.llm-calls.md"),
        Path(f"{base_filename}.llm-calls.jsonl"),
    )


def _new_llm_call_transcript(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    output_dir: str | Path,
    is_test: bool,
) -> LLMCallTranscript:
    base_filename = Path(output_dir) / _llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return LLMCallTranscript(local_filename=base_filename)


def _llm_call_transcript_upload_targets_for_files(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    local_markdown_filename: Path,
    local_jsonl_filename: Path,
    is_test: bool,
) -> list[TranscriptUploadTarget]:
    transcript_files = (
        (
            local_markdown_filename,
            output.llm_call_transcript_markdown_destination_blob_name,
        ),
        (local_jsonl_filename, output.llm_call_transcript_jsonl_destination_blob_name),
    )
    return [
        TranscriptUploadTarget(
            local_filename=local_filename,
            destination_blob_name=destination_blob_name_for(
                forecast_due_date=forecast_due_date,
                model_run=model_run,
                is_test=is_test,
            ),
        )
        for local_filename, destination_blob_name_for in transcript_files
    ]


def llm_call_transcript_upload_targets(
    forecast_due_date: str,
    model_run: fb_model_runs.ModelRun,
    output_dir: str | Path,
    is_test: bool,
) -> list[TranscriptUploadTarget]:
    """Return local transcript files and destination names for boundary uploads."""
    local_markdown_filename, local_jsonl_filename = _llm_call_transcript_local_filenames(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        output_dir=output_dir,
        is_test=is_test,
    )
    return _llm_call_transcript_upload_targets_for_files(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        local_markdown_filename=local_markdown_filename,
        local_jsonl_filename=local_jsonl_filename,
        is_test=is_test,
    )


def iter_model_forecasts(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    output_dir: str | Path,
    is_test: bool = False,
    today_date: str | None = None,
    raise_on_question_error: bool = False,
) -> Iterator[ForecastResult]:
    """Run one model and yield forecast rows in variant order."""
    if today_date is None:
        today_date = dates.get_date_today_as_iso()

    dataset_questions, market_questions = split_questions(question_set.questions)
    dataset_expected_count = _expected_dataset_forecast_count(dataset_questions=dataset_questions)
    market_expected_count = len(market_questions)
    market_success_counts = {}
    dataset_success_count = 0
    transcript = _new_llm_call_transcript(
        forecast_due_date=question_set.forecast_due_date,
        model_run=model_run,
        output_dir=output_dir,
        is_test=is_test,
    )
    logger.info(f"Writing LLM call transcript to {transcript.local_filename}.")

    for variant_group in DATASET_FORECAST_SHARING_VARIANT_GROUPS:
        dataset_rows = _forecast_dataset_questions(
            model_run=model_run,
            dataset_questions=dataset_questions,
            forecast_due_date=question_set.forecast_due_date,
            today_date=today_date,
            variant=variant_group.dataset_prompt_variant,
            raise_on_question_error=raise_on_question_error,
            transcript=transcript,
        )
        if dataset_rows is None or dataset_rows.empty:
            raise RuntimeError("No dataset forecasts were produced.")
        dataset_success_count = len(dataset_rows)

        for variant in variant_group.output_variants:
            market_rows = _forecast_market_questions(
                model_run=model_run,
                market_questions=market_questions,
                forecast_due_date=question_set.forecast_due_date,
                today_date=today_date,
                variant=variant,
                raise_on_question_error=raise_on_question_error,
                transcript=transcript,
            )
            if market_rows is None or market_rows.empty:
                raise RuntimeError(f"No market forecasts were produced for variant {variant.key}.")
            market_success_counts[variant] = len(market_rows)
            rows = _sorted_forecast_rows(
                rows=pd.concat([dataset_rows, market_rows], ignore_index=True)
            )
            forecast_result = ForecastResult(variant=variant, rows=rows)
            logger.info(
                f"Generated LLM forecasts variant={variant.key} rows={len(forecast_result.rows)}"
            )
            yield forecast_result

    _log_forecast_success_counts(
        market_success_counts=market_success_counts,
        market_expected_count=market_expected_count,
        dataset_success_count=dataset_success_count,
        dataset_expected_count=dataset_expected_count,
    )


def run_model(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    output_dir: str | Path,
    is_test: bool = False,
    today_date: str | None = None,
    raise_on_question_error: bool = False,
) -> list[ForecastResult]:
    """Run one model and return forecast rows in variant order."""
    return list(
        iter_model_forecasts(
            model_run=model_run,
            question_set=question_set,
            output_dir=output_dir,
            is_test=is_test,
            today_date=today_date,
            raise_on_question_error=raise_on_question_error,
        )
    )
