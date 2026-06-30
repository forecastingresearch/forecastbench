"""ForecastBench LLM forecast output helpers."""

from functools import lru_cache
from typing import Any, Mapping, Sequence

from helpers.constants import BENCHMARK_NAME
from helpers.run_mode import RunMode
from llm_forecaster.fb_model_runs import FB_MODEL_RUNS, ModelRun
from llm_forecaster.forecast_variants import (
    FORECAST_VARIANTS,
    ZERO_SHOT,
    ZERO_SHOT_WITH_FREEZE_VALUES,
    ForecastVariant,
)


def display_model_name(model_run: ModelRun, variant: ForecastVariant) -> str:
    """Return the displayed model name for a model run and forecast variant."""
    if variant == ZERO_SHOT:
        return model_run.slug
    if variant == ZERO_SHOT_WITH_FREEZE_VALUES:
        return f"{model_run.slug}†"
    return f"{model_run.slug}-{variant.key}"


def forecast_file_model_name(model_run: ModelRun, variant: ForecastVariant) -> str:
    """Return the model name stored in forecast files and filenames."""
    if variant == ZERO_SHOT:
        return model_run.slug
    return f"{model_run.slug}-{variant.key}"


@lru_cache
def _display_model_name_map() -> dict[str, tuple[ModelRun, ForecastVariant]]:
    """Return active display model names keyed to model runs and variants."""
    return {
        display_model_name(model_run, variant): (model_run, variant)
        for model_run in FB_MODEL_RUNS
        for variant in FORECAST_VARIANTS
    }


def parse_display_model_name(model_name: str) -> tuple[ModelRun, ForecastVariant]:
    """Return the active model run and forecast variant for a displayed model name."""
    try:
        return _display_model_name_map()[model_name]
    except KeyError as exc:
        raise KeyError(f"Unknown ForecastBench LLM display model name: {model_name}") from exc


def final_filename(
    forecast_due_date: str,
    model_run: ModelRun,
    variant: ForecastVariant,
    is_test: bool,
) -> str:
    """Return the final forecast filename."""
    filename = (
        f"{forecast_due_date}.{BENCHMARK_NAME}."
        f"{model_run.provider.name}.{model_run.lab.name}."
        f"{model_run.model_run_key}-{variant.key}.json"
    )
    if is_test:
        return f"{RunMode.TEST.output_file_prefix}{filename}"
    return filename


def llm_call_transcript_base_filename(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the local transcript filename stem shared by all transcript formats."""
    filename = (
        f"{forecast_due_date}.{BENCHMARK_NAME}."
        f"{model_run.provider.name}.{model_run.lab.name}."
        f"{model_run.model_run_key}"
    )
    if is_test:
        return f"{RunMode.TEST.output_file_prefix}{filename}"
    return filename


def llm_call_transcript_markdown_filename(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the local Markdown transcript filename."""
    base_filename = llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return f"{base_filename}.llm-calls.md"


def llm_call_transcript_jsonl_filename(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the local structured JSONL transcript filename."""
    base_filename = llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return f"{base_filename}.llm-calls.jsonl"


def llm_call_transcript_base_destination_blob_name(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the private transcript bucket blob stem shared by all transcript formats."""
    run_mode = (RunMode.TEST if is_test else RunMode.PROD).value.lower()
    base_filename = llm_call_transcript_base_filename(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return f"{forecast_due_date}/{run_mode}/{base_filename}"


def llm_call_transcript_markdown_destination_blob_name(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the private transcript bucket destination blob name for Markdown."""
    base_blob_name = llm_call_transcript_base_destination_blob_name(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return f"{base_blob_name}.llm-calls.md"


def llm_call_transcript_jsonl_destination_blob_name(
    forecast_due_date: str,
    model_run: ModelRun,
    is_test: bool,
) -> str:
    """Return the private transcript bucket destination blob name for JSONL."""
    base_blob_name = llm_call_transcript_base_destination_blob_name(
        forecast_due_date=forecast_due_date,
        model_run=model_run,
        is_test=is_test,
    )
    return f"{base_blob_name}.llm-calls.jsonl"


def destination_blob_name(
    forecast_due_date: str,
    model_run: ModelRun,
    variant: ForecastVariant,
    is_test: bool,
) -> str:
    """Return the destination blob name for a final forecast file."""
    return f"{forecast_due_date}/{final_filename(forecast_due_date, model_run, variant, is_test)}"


def forecast_file_data(
    forecast_due_date: str,
    question_set_filename: str,
    model_run: ModelRun,
    variant: ForecastVariant,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return a ForecastBench LLM forecast file using the current identity schema."""
    market_prompt_uses_freeze_values = variant.market_prompt_uses_freeze_values
    return {
        "organization": BENCHMARK_NAME,
        "model": forecast_file_model_name(model_run, variant),
        "model_organization": model_run.lab.name,
        "model_run_key": model_run.model_run_key,
        "model_run_slug": model_run.slug,
        "forecast_variant_key": variant.key,
        "market_prompt_uses_freeze_values": market_prompt_uses_freeze_values,
        "question_set": question_set_filename,
        "forecast_due_date": forecast_due_date,
        "forecasts": rows,
    }
