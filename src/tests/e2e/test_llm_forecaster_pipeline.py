"""E2E: the LLM-forecaster flow — question set → ``run_model`` → written forecast files.

Runs the real ``llm_forecaster.runner`` end to end with the model boundary mocked (a deterministic
``get_response`` — never a provider call), then writes the final forecast files through
``orchestration._llm_forecaster_io`` and asserts:

- **anchors** — the forecast-file schema, the two active variants in order, the exact per-variant row
  set (dataset rows exploded per resolution date; market rows once with ``resolution_date=None``),
  dataset rows shared across variants, and no display-only ``†`` marker leaking into the JSON; and
- a **golden snapshot** of the combined forecast set (both variants), the catch-all net.

The offline test builds a question set from the **real published record shape** (14 keys, captured
from a live ``*-llm.json`` set) so the runner is proven to accept it, with no network. A separate
``@live`` companion fetches the *actual* published set to catch upstream shape drift.
"""

import ast
import json
import re
from types import SimpleNamespace

import pandas as pd
import pytest
from utils.llm.provider_registry import PROVIDERS

from llm_forecaster import output
from llm_forecaster import question_set as question_set_module
from llm_forecaster import runner
from llm_forecaster.forecast_variants import ZERO_SHOT, ZERO_SHOT_WITH_FREEZE_VALUES
from orchestration import _llm_forecaster_io
from sources import DATASET_SOURCE_NAMES
from tests._golden import check_golden

_FORECAST_DUE_DATE = "2026-05-10"
_TODAY = "2026-05-06"
_QUESTION_SET_FILENAME = "2026-05-10-llm.json"


class FixedRun:
    """A stable model-run identity so the golden's model columns never drift."""

    model_run_key = "test-model-run-variant-01"
    slug = "test-model"
    provider = PROVIDERS["OpenAI"]
    lab = SimpleNamespace(name="TestLab")
    provider_model_id = "test-provider-model-id"
    options: dict = {}

    calls: list[str] = []

    def get_response(self, prompt: str) -> str:
        """Return deterministic, cleanly-parseable forecasts (no extraction-model fallback)."""
        FixedRun.calls.append(prompt)
        match = re.search(r"^Question resolution dates: (.+)$", prompt, flags=re.MULTILINE)
        if match is not None:
            resolution_dates = ast.literal_eval(match.group(1))
            return " ".join(f"*0.{index + 1:02d}*" for index, _ in enumerate(resolution_dates))
        return "*0.42*"


def _dataset_question(question_id: str) -> dict:
    """A dataset question carrying the full published-record key set (14 keys)."""
    return {
        "id": question_id,
        "source": "fred",
        "url": f"https://example.com/{question_id}",
        "question": "Will value rise after {forecast_due_date} by {resolution_date}?",
        "background": "Dataset background.",
        "resolution_criteria": "Dataset resolution criteria.",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "freeze_datetime": "2026-05-05",
        "freeze_datetime_value": "100",
        "freeze_datetime_value_explanation": "Latest observed value.",
        "source_intro": "This dataset tracks a published series.",
        "resolution_dates": ["2026-06-01", "2026-07-01"],
    }


def _market_question(question_id: str, source: str) -> dict:
    """A market question carrying the full published-record key set (14 keys)."""
    return {
        "id": question_id,
        "source": source,
        "url": f"https://example.com/{question_id}",
        "question": "Will the market question resolve true?",
        "background": "Market background.",
        "resolution_criteria": "Market resolution criteria.",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "2026-05-01",
        "market_info_close_datetime": "2026-06-15",
        "freeze_datetime": "2026-05-05",
        "freeze_datetime_value": "0.33",
        "freeze_datetime_value_explanation": "Latest market price.",
        "source_intro": "This market trades on a public platform.",
        "resolution_dates": ["2026-06-15"],
    }


def _question_set() -> question_set_module.QuestionSet:
    return question_set_module.QuestionSet(
        forecast_due_date=_FORECAST_DUE_DATE,
        question_set_filename=_QUESTION_SET_FILENAME,
        questions=[
            _dataset_question("fred-1"),
            _dataset_question("fred-2"),
            _market_question("metaculus-1", "metaculus"),
            _market_question("manifold-1", "manifold"),
        ],
    )


def _combined_forecast_frame(written_files) -> pd.DataFrame:
    """One frame across both variants for the golden, keyed by variant/source/id/resolution_date."""
    rows = []
    for written_file in written_files:
        data = json.loads(written_file.local_filename.read_text(encoding="utf-8"))
        for forecast_row in data["forecasts"]:
            rows.append(
                {
                    "variant": data["forecast_variant_key"],
                    "source": forecast_row["source"],
                    "id": forecast_row["id"],
                    "resolution_date": forecast_row["resolution_date"] or "",
                    "forecast": forecast_row["forecast"],
                }
            )
    return pd.DataFrame(rows)


def test_llm_forecaster_pipeline_offline_writes_forecast_set(tmp_path):
    question_set = _question_set()
    model_run = FixedRun()
    FixedRun.calls.clear()

    forecast_results = runner.run_model(
        model_run=model_run,
        question_set=question_set,
        output_dir=tmp_path,
        is_test=True,
        today_date=_TODAY,
        raise_on_question_error=True,
    )
    written_files = [
        _llm_forecaster_io.write_final_forecast_file(
            model_run=model_run,
            question_set=question_set,
            output_dir=tmp_path,
            forecast_result=forecast_result,
            is_test=True,
        )
        for forecast_result in forecast_results
    ]

    # --- Anchors: the two active variants, in order ---
    assert [written_file.variant for written_file in written_files] == [
        ZERO_SHOT,
        ZERO_SHOT_WITH_FREEZE_VALUES,
    ]

    zero_shot_data = json.loads(written_files[0].local_filename.read_text(encoding="utf-8"))
    freeze_data = json.loads(written_files[1].local_filename.read_text(encoding="utf-8"))

    assert set(zero_shot_data) == {
        "organization",
        "model",
        "model_organization",
        "model_run_key",
        "model_run_slug",
        "forecast_variant_key",
        "market_prompt_uses_freeze_values",
        "question_set",
        "forecast_due_date",
        "forecasts",
    }
    assert zero_shot_data["organization"] == "ForecastBench"
    assert zero_shot_data["forecast_due_date"] == _FORECAST_DUE_DATE
    assert zero_shot_data["question_set"] == _QUESTION_SET_FILENAME
    assert zero_shot_data["forecast_variant_key"] == ZERO_SHOT.key
    assert zero_shot_data["market_prompt_uses_freeze_values"] is False
    assert freeze_data["forecast_variant_key"] == ZERO_SHOT_WITH_FREEZE_VALUES.key
    assert freeze_data["market_prompt_uses_freeze_values"] is True

    # No display-only footnote marker leaks into the persisted JSON.
    for written_file in written_files:
        raw = written_file.local_filename.read_text(encoding="utf-8")
        assert "†" not in raw
        assert "\\u2020" not in raw

    dataset_source_names = set(DATASET_SOURCE_NAMES)

    def _row_key(row: dict) -> tuple:
        return row["source"], row["id"], row["resolution_date"]

    # Dataset rows are shared byte-for-byte across the two variants.
    zero_shot_dataset_rows = [
        row for row in zero_shot_data["forecasts"] if row["source"] in dataset_source_names
    ]
    freeze_dataset_rows = [
        row for row in freeze_data["forecasts"] if row["source"] in dataset_source_names
    ]
    assert zero_shot_dataset_rows == freeze_dataset_rows
    # ...because the dataset questions were forecast ONCE and shared, not once per variant: the
    # provider saw one call per dataset question (all its resolution dates in one prompt) plus one
    # per (variant, market question).
    n_dataset_questions = len({(row["source"], row["id"]) for row in zero_shot_dataset_rows})
    n_market_rows = len(zero_shot_data["forecasts"]) - len(zero_shot_dataset_rows)
    assert len(FixedRun.calls) == n_dataset_questions + 2 * n_market_rows

    # Exact row set: each dataset question exploded per resolution date; each market question once.
    assert {_row_key(row) for row in zero_shot_dataset_rows} == {
        ("fred", "fred-1", "2026-06-01"),
        ("fred", "fred-1", "2026-07-01"),
        ("fred", "fred-2", "2026-06-01"),
        ("fred", "fred-2", "2026-07-01"),
    }
    market_row_keys = {
        _row_key(row)
        for row in zero_shot_data["forecasts"]
        if row["source"] not in dataset_source_names
    }
    assert market_row_keys == {("metaculus", "metaculus-1", None), ("manifold", "manifold-1", None)}

    # --- Golden: the whole forecast set (both variants), the catch-all net ---
    check_golden(
        "e2e_llm_forecaster_forecast_set",
        _combined_forecast_frame(written_files),
        key=["variant", "source", "id", "resolution_date"],
        rtol=0,
    )


@pytest.mark.live  # fetches a real published question set over the network (offline guard blocks it)
def test_llm_forecaster_pipeline_accepts_real_published_question_set(tmp_path):
    from orchestration import _io

    published = _io.read_question_set_json("2026-05-24-llm.json", run_locally=False)
    full_question_set = question_set_module.QuestionSet.from_question_set_json(published)
    dataset_questions, market_questions = question_set_module.split_questions(
        full_question_set.questions
    )
    dataset_questions, market_questions = question_set_module.limit_questions_for_test_mode(
        dataset_questions, market_questions, 2
    )
    question_set = question_set_module.QuestionSet(
        forecast_due_date=full_question_set.forecast_due_date,
        question_set_filename=full_question_set.question_set_filename,
        questions=dataset_questions + market_questions,
    )
    model_run = FixedRun()
    FixedRun.calls.clear()

    forecast_results = runner.run_model(
        model_run=model_run,
        question_set=question_set,
        output_dir=tmp_path,
        is_test=True,
        today_date=full_question_set.forecast_due_date,
        raise_on_question_error=True,
    )
    written_files = [
        _llm_forecaster_io.write_final_forecast_file(
            model_run=model_run,
            question_set=question_set,
            output_dir=tmp_path,
            forecast_result=forecast_result,
            is_test=True,
        )
        for forecast_result in forecast_results
    ]

    assert [written_file.variant for written_file in written_files] == [
        ZERO_SHOT,
        ZERO_SHOT_WITH_FREEZE_VALUES,
    ]
    for written_file in written_files:
        data = json.loads(written_file.local_filename.read_text(encoding="utf-8"))
        expected_row_count = sum(
            (
                len(question["resolution_dates"])
                if question["source"] in set(DATASET_SOURCE_NAMES)
                else 1
            )
            for question in question_set.questions
        )
        assert len(data["forecasts"]) == expected_row_count
        assert (
            output.final_filename(
                full_question_set.forecast_due_date, model_run, written_file.variant, is_test=True
            )
            == written_file.local_filename.name
        )
