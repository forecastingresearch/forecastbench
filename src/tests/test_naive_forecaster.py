"""Tests for the naive forecaster's question set loading."""

import importlib
import io
import json
import types

from orchestration import _io
from tests._module_stubs import reset_modules, stubbed_modules

NAIVE_MAIN = "base_eval.naive_and_dummy_forecasters.main"
STUBS = {
    "prophet": types.SimpleNamespace(Prophet=object),
    "pandas_market_calendars": types.SimpleNamespace(),
}


def import_naive_main():
    """Import the naive forecaster without its heavy time-series dependencies."""
    with stubbed_modules(STUBS), reset_modules(NAIVE_MAIN):
        return importlib.import_module(NAIVE_MAIN)


def test_load_latest_question_set_reads_published_utf8_question_set(monkeypatch):
    question_text = "Will the “Südmo – décor” index rise?"
    latest_url = f"{_io.DATASETS_QUESTION_SETS_RAW_BASE_URL}/latest-llm.json"
    question_set_url = f"{_io.DATASETS_QUESTION_SETS_RAW_BASE_URL}/2026-09-27-llm.json"
    question_set = {
        "forecast_due_date": "2026-09-27",
        "question_set": "2026-09-27-llm.json",
        "questions": [{"id": "q1", "source": "dbnomics", "question": question_text}],
    }
    responses = {
        latest_url: b"2026-09-27-llm.json",
        question_set_url: json.dumps(question_set, ensure_ascii=False).encode("utf-8"),
    }
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        return io.BytesIO(responses[url])

    monkeypatch.setattr(_io, "urlopen", fake_urlopen)
    naive_main = import_naive_main()

    df, forecast_due_date, question_set_filename = naive_main.load_latest_question_set()

    assert forecast_due_date == "2026-09-27"
    assert question_set_filename == "2026-09-27-llm.json"
    assert df["question"].tolist() == [question_text]
    assert set(calls) == {latest_url, question_set_url}
