import inspect
import json
from types import SimpleNamespace

from llm_forecaster import output
from llm_forecaster.forecast_variants import ZERO_SHOT, ZERO_SHOT_WITH_FREEZE_VALUES


class FakeRun:
    model_run_key = "test-model-run-variant-01"
    slug = "test-model"
    provider = SimpleNamespace(name="TestProvider")
    lab = SimpleNamespace(name="OpenAI")
    options = {}


class FakeSearchToolRun(FakeRun):
    options = {"tools": [{"type": "web_search_preview"}]}


def _run() -> FakeRun:
    return FakeRun()


def test_display_model_name_uses_variant_display_suffix():
    assert output.display_model_name(_run(), ZERO_SHOT) == "test-model"
    assert output.display_model_name(_run(), ZERO_SHOT_WITH_FREEZE_VALUES) == "test-model†"


def test_destination_blob_name_uses_marker_free_file_name():
    assert (
        output.destination_blob_name("2026-05-10", _run(), ZERO_SHOT, is_test=False)
        == "2026-05-10/"
        "2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01-zero-shot.json"
    )
    assert (
        output.destination_blob_name(
            "2026-05-10", _run(), ZERO_SHOT_WITH_FREEZE_VALUES, is_test=True
        )
        == "2026-05-10/"
        "TEST.2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01-zero-shot-with-freeze-values.json"
    )


def test_llm_call_transcript_filenames_add_format_suffix_to_shared_base_name():
    assert (
        output.llm_call_transcript_base_filename("2026-05-10", _run(), is_test=False)
        == "2026-05-10.ForecastBench.TestProvider.OpenAI.test-model-run-variant-01"
    )
    assert (
        output.llm_call_transcript_markdown_filename("2026-05-10", _run(), is_test=False)
        == "2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01.llm-calls.md"
    )
    assert (
        output.llm_call_transcript_jsonl_filename("2026-05-10", _run(), is_test=False)
        == "2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01.llm-calls.jsonl"
    )


def test_llm_call_transcript_destinations_use_private_transcript_folder():
    assert (
        output.llm_call_transcript_markdown_destination_blob_name(
            "2026-05-10",
            _run(),
            is_test=False,
        )
        == "2026-05-10/prod/"
        "2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01.llm-calls.md"
    )
    assert (
        output.llm_call_transcript_markdown_destination_blob_name(
            "2026-05-10",
            _run(),
            is_test=True,
        )
        == "2026-05-10/test/"
        "TEST.2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01.llm-calls.md"
    )
    assert (
        output.llm_call_transcript_base_destination_blob_name(
            "2026-05-10",
            _run(),
            is_test=True,
        )
        == "2026-05-10/test/"
        "TEST.2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01"
    )
    assert (
        output.llm_call_transcript_jsonl_destination_blob_name(
            "2026-05-10",
            _run(),
            is_test=True,
        )
        == "2026-05-10/test/"
        "TEST.2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01.llm-calls.jsonl"
    )


def test_final_filename_uses_marker_free_file_name():
    assert (
        output.final_filename("2026-05-10", _run(), ZERO_SHOT, is_test=True)
        == "TEST.2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01-zero-shot.json"
    )
    assert (
        output.final_filename("2026-05-10", _run(), ZERO_SHOT_WITH_FREEZE_VALUES, is_test=False)
        == "2026-05-10.ForecastBench.TestProvider.OpenAI."
        "test-model-run-variant-01-zero-shot-with-freeze-values.json"
    )


def test_output_module_builds_data_but_does_not_write_files():
    source = inspect.getsource(output)

    assert "write_text" not in source
    assert "open(" not in source


def test_forecast_file_data_includes_stable_model_run_identity():
    rows = [
        {
            "id": "q1",
            "source": "fred",
            "forecast": 0.61,
            "resolution_date": "2026-06-01",
            "reasoning": None,
        }
    ]

    data = output.forecast_file_data(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        model_run=_run(),
        variant=ZERO_SHOT,
        rows=rows,
    )

    assert data == {
        "organization": "ForecastBench",
        "model": "test-model",
        "model_organization": "OpenAI",
        "model_run_key": "test-model-run-variant-01",
        "model_run_slug": "test-model",
        "forecast_variant_key": "zero-shot",
        "market_prompt_uses_freeze_values": False,
        "question_set": "2026-05-10-llm.json",
        "forecast_due_date": "2026-05-10",
        "forecasts": rows,
    }


def test_forecast_file_data_does_not_persist_tool_use():
    data = output.forecast_file_data(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        model_run=FakeSearchToolRun(),
        variant=ZERO_SHOT,
        rows=[],
    )

    assert "uses_tools" not in data


def test_forecast_file_data_does_not_write_display_footnote_marker():
    data = output.forecast_file_data(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        model_run=_run(),
        variant=ZERO_SHOT_WITH_FREEZE_VALUES,
        rows=[],
    )

    raw_file = json.dumps(data)

    assert "†" not in raw_file
    assert "\\u2020" not in raw_file
    assert data["model"] == "test-model-zero-shot-with-freeze-values"
    assert data["model_run_slug"] == "test-model"
    assert data["forecast_variant_key"] == ZERO_SHOT_WITH_FREEZE_VALUES.key
    assert data["market_prompt_uses_freeze_values"] is True
