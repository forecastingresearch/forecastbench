import json
from types import SimpleNamespace

import pytest

from llm_forecaster import model_run_transcripts
from llm_forecaster.forecast_variants import ZERO_SHOT


class FakeRun:
    model_run_key = "test-model-run-variant-01"
    slug = "test-model"
    provider_model_id = "test-provider-model-id"
    provider = SimpleNamespace(name="OpenAI")
    lab = SimpleNamespace(name="Test Lab")


def test_llm_call_transcript_writes_local_markdown_and_jsonl_files(tmp_path):
    path = tmp_path / "calls"

    transcript = model_run_transcripts.LLMCallTranscript(path)
    transcript.record(
        role="forecast",
        model_run=FakeRun(),
        question={"id": "q1", "source": "fred", "url": "https://example.com/q1"},
        variant=ZERO_SHOT,
        prompt="Prompt",
        response="*0.4*",
        expected_forecasts=1,
    )

    markdown_path = tmp_path / "calls.llm-calls.md"
    jsonl_path = tmp_path / "calls.llm-calls.jsonl"
    markdown = markdown_path.read_text(encoding="utf-8")
    jsonl = jsonl_path.read_text(encoding="utf-8")
    assert markdown.startswith("# LLM Call Transcript\n")
    assert "## Call 1: forecast (zero-shot)" in markdown
    assert "- Question URL: https://example.com/q1" in markdown
    assert "- Expected forecasts: 1" in markdown
    assert json.loads(jsonl)["response"] == "*0.4*"
    assert json.loads(jsonl)["question_url"] == "https://example.com/q1"
    assert json.loads(jsonl)["expected_forecasts"] == 1


def test_llm_call_transcript_requires_question_url(tmp_path):
    transcript = model_run_transcripts.LLMCallTranscript(tmp_path / "calls")

    with pytest.raises(KeyError, match="url"):
        transcript.record(
            role="forecast",
            model_run=FakeRun(),
            question={"id": "q1", "source": "fred"},
            variant=ZERO_SHOT,
            prompt="Prompt",
            response="*0.4*",
            expected_forecasts=1,
        )
