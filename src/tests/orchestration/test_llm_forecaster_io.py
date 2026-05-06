import json
from types import SimpleNamespace

import pandera.pandas as pa
import pytest
from utils.llm.provider_registry import PROVIDERS

from helpers import env
from llm_forecaster import runner
from llm_forecaster.forecast_variants import ZERO_SHOT
from llm_forecaster.question_set import QuestionSet
from orchestration import _llm_forecaster_io


class FakeRun:
    model_run_key = "test-model-run-variant-01"
    slug = "test-model"
    provider_model_id = "test-provider-model-id"
    lab = SimpleNamespace(name="Test Lab")
    provider = PROVIDERS["OpenAI"]


def _question_set():
    return QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[],
    )


def _forecast_frame(rows):
    return runner._validated_forecast_frame_from_rows(rows)


def test_write_final_forecast_file_converts_forecast_frame_to_json_records(tmp_path):
    forecast_result = runner.ForecastResult(
        variant=ZERO_SHOT,
        rows=_forecast_frame(
            [
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "forecast": 0.2,
                    "resolution_date": "2026-06-01",
                    "reasoning": "",
                },
                {
                    "id": "market-1",
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                },
            ]
        ),
    )

    written_file = _llm_forecaster_io.write_final_forecast_file(
        model_run=FakeRun(),
        question_set=_question_set(),
        output_dir=tmp_path,
        forecast_result=forecast_result,
        is_test=True,
    )
    written_data = json.loads(written_file.local_filename.read_text(encoding="utf-8"))

    assert written_file.rows == [
        {
            "id": "dataset-1",
            "source": "fred",
            "forecast": 0.2,
            "resolution_date": "2026-06-01",
            "reasoning": "",
        },
        {
            "id": "market-1",
            "source": "metaculus",
            "forecast": 0.4,
            "resolution_date": None,
            "reasoning": "",
        },
    ]
    assert written_data["forecasts"] == written_file.rows


def test_write_final_forecast_file_rejects_raw_forecast_records(tmp_path):
    forecast_result = SimpleNamespace(
        variant=ZERO_SHOT,
        rows=[
            {
                "id": "dataset-1",
                "source": "fred",
                "forecast": 0.2,
                "resolution_date": "2026-06-01",
                "reasoning": "",
            }
        ],
    )

    with pytest.raises(pa.errors.BackendNotFoundError, match="Backend not found"):
        _llm_forecaster_io.write_final_forecast_file(
            model_run=FakeRun(),
            question_set=_question_set(),
            output_dir=tmp_path,
            forecast_result=forecast_result,
            is_test=True,
        )


def test_final_forecast_set_destination_blob_names_match_output_variants():
    destination_blob_names = _llm_forecaster_io.final_forecast_set_destination_blob_names(
        model_run=FakeRun(),
        question_set=_question_set(),
        is_test=True,
    )

    assert destination_blob_names == [
        (
            "2026-05-10/"
            "TEST.2026-05-10.ForecastBench.OpenAI.Test Lab."
            "test-model-run-variant-01-zero-shot.json"
        ),
        (
            "2026-05-10/"
            "TEST.2026-05-10.ForecastBench.OpenAI.Test Lab."
            "test-model-run-variant-01-zero-shot-with-freeze-values.json"
        ),
    ]


def test_write_final_forecast_file_overwrites_existing_local_file(tmp_path):
    forecast_result = runner.ForecastResult(
        variant=ZERO_SHOT,
        rows=_forecast_frame(
            [
                {
                    "id": "market-1",
                    "source": "metaculus",
                    "forecast": 0.4,
                    "resolution_date": None,
                    "reasoning": "",
                }
            ]
        ),
    )
    existing_path = tmp_path / (
        "TEST.2026-05-10.ForecastBench.OpenAI.Test Lab." "test-model-run-variant-01-zero-shot.json"
    )
    existing_path.write_text("existing", encoding="utf-8")

    written_file = _llm_forecaster_io.write_final_forecast_file(
        model_run=FakeRun(),
        question_set=_question_set(),
        output_dir=tmp_path,
        forecast_result=forecast_result,
        is_test=True,
    )

    assert written_file.local_filename == existing_path
    assert existing_path.read_text(encoding="utf-8") != "existing"


def test_transcript_storage_helper_uses_transcripts_bucket(monkeypatch, tmp_path):
    calls = {}
    path = tmp_path / "transcript.md"

    def fake_upload(**kwargs):
        calls["upload"] = kwargs

    monkeypatch.setattr(_llm_forecaster_io.gcp.storage, "upload", fake_upload)

    _llm_forecaster_io.upload_llm_call_transcript(path, "2026-05-10/prod/transcript.md")

    assert calls["upload"] == {
        "bucket_name": env.FORECAST_SETS_TRANSCRIPTS_BUCKET,
        "local_filename": str(path),
        "filename": "2026-05-10/prod/transcript.md",
    }
