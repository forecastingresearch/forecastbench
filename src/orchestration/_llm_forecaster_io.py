"""Final forecast file IO for ForecastBench LLM forecaster orchestration."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pandera.typing import DataFrame
from utils import gcp

from _schemas import ForecastFrame
from helpers import env
from llm_forecaster import fb_model_runs, output, runner
from llm_forecaster.forecast_variants import (
    DATASET_FORECAST_SHARING_VARIANT_GROUPS,
    ForecastVariant,
)
from llm_forecaster.question_set import QuestionSet
from orchestration import _io


@dataclass(frozen=True)
class WrittenForecastFile:
    """A final forecast file written by orchestration."""

    variant: ForecastVariant
    local_filename: Path
    destination_blob_name: str
    rows: list[dict[str, Any]]


def final_forecast_set_destination_blob_names(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    is_test: bool,
) -> list[str]:
    """Return destination names for all final forecast files in this run."""
    destination_blob_names = []
    for variant_group in DATASET_FORECAST_SHARING_VARIANT_GROUPS:
        for variant in variant_group.output_variants:
            destination_blob_names.append(
                output.destination_blob_name(
                    forecast_due_date=question_set.forecast_due_date,
                    model_run=model_run,
                    variant=variant,
                    is_test=is_test,
                )
            )
    return destination_blob_names


def _forecast_rows_to_records(rows: DataFrame[ForecastFrame]) -> list[dict[str, Any]]:
    validated_rows = ForecastFrame.validate(rows)
    object_rows = validated_rows.astype(object)
    return object_rows.where(pd.notnull(object_rows), None).to_dict(orient="records")


def write_final_forecast_file(
    model_run: fb_model_runs.ModelRun,
    question_set: QuestionSet,
    output_dir: str | Path,
    forecast_result: runner.ForecastResult,
    is_test: bool,
) -> WrittenForecastFile:
    """Write one final forecast result as the current ForecastBench JSON file."""
    variant = forecast_result.variant
    local_filename = Path(output_dir) / output.final_filename(
        forecast_due_date=question_set.forecast_due_date,
        model_run=model_run,
        variant=variant,
        is_test=is_test,
    )
    destination_blob_name = output.destination_blob_name(
        forecast_due_date=question_set.forecast_due_date,
        model_run=model_run,
        variant=variant,
        is_test=is_test,
    )
    row_records = _forecast_rows_to_records(forecast_result.rows)
    forecast_data = output.forecast_file_data(
        forecast_due_date=question_set.forecast_due_date,
        question_set_filename=question_set.question_set_filename,
        model_run=model_run,
        variant=variant,
        rows=row_records,
    )
    _io.write_forecast_file(local_filename, forecast_data)
    return WrittenForecastFile(
        variant=variant,
        local_filename=local_filename,
        destination_blob_name=destination_blob_name,
        rows=row_records,
    )


def upload_written_forecast_file(written_file: WrittenForecastFile) -> None:
    """Upload one written final forecast file."""
    _io.upload_forecast_file(
        local_filename=written_file.local_filename,
        filename=written_file.destination_blob_name,
    )


def upload_llm_call_transcript(local_filename: str | Path, filename: str) -> None:
    """Upload a local LLM call transcript to the private transcript bucket."""
    gcp.storage.upload(
        bucket_name=env.FORECAST_SETS_TRANSCRIPTS_BUCKET,
        local_filename=str(local_filename),
        filename=filename,
    )
