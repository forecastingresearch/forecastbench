import logging
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.tests.llm_forecaster.smoke_test import smoke_test


def test_module_import_does_not_require_orchestration_io():
    code = """
import importlib.abc
import sys

class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"orchestration._io", "termcolor"}:
            raise ModuleNotFoundError(f"No module named {fullname!r}")
        return None

sys.meta_path.insert(0, Blocker())
import src.tests.llm_forecaster.smoke_test.smoke_test
print("ok")
"""

    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": "src"},
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "ok"


def test_select_model_runs_defaults_to_all():
    runs = [
        SimpleNamespace(model_run_key="a-run-variant-01", slug="a"),
        SimpleNamespace(model_run_key="b-run-variant-01", slug="b"),
    ]
    assert smoke_test.select_model_runs(runs, None) == runs


def test_select_model_runs_selects_by_model_run_key():
    selected_run = SimpleNamespace(model_run_key="b-run-variant-01", slug="b")
    runs = [
        SimpleNamespace(model_run_key="a-run-variant-01", slug="a"),
        selected_run,
    ]

    assert smoke_test.select_model_runs(runs, ["b-run-variant-01"]) == [selected_run]


def test_select_model_runs_resolves_explicit_keys_from_available_registry():
    configured_runs = [SimpleNamespace(model_run_key="a-run-variant-01", slug="a")]
    requested_run = SimpleNamespace(model_run_key="c-run-variant-03", slug="c")
    available_runs = [*configured_runs, requested_run]

    assert smoke_test.select_model_runs(
        configured_runs,
        ["c-run-variant-03"],
        available_model_runs=available_runs,
    ) == [requested_run]


def test_select_model_runs_rejects_slug_values():
    runs = [SimpleNamespace(model_run_key="a-run-variant-01", slug="a")]
    with pytest.raises(ValueError, match="Unknown model run key"):
        smoke_test.select_model_runs(runs, ["a"])


def test_select_model_runs_rejects_missing_key():
    runs = [SimpleNamespace(model_run_key="a-run-variant-01", slug="a")]
    with pytest.raises(ValueError, match="missing-run-variant-01"):
        smoke_test.select_model_runs(runs, ["missing-run-variant-01"])


def test_select_questions_takes_deterministic_dataset_and_market_prefixes():
    questions = [
        {"id": "market-b", "source": "metaculus"},
        {"id": "dataset-b", "source": "fred"},
        {"id": "market-a", "source": "manifold"},
        {"id": "dataset-a", "source": "acled"},
        {"id": "dataset-c", "source": "wikipedia"},
        {"id": "market-c", "source": "polymarket"},
    ]

    selected = smoke_test.select_questions(questions, sample_size=2)

    assert selected == [
        {"id": "dataset-a", "source": "acled"},
        {"id": "dataset-b", "source": "fred"},
        {"id": "market-a", "source": "manifold"},
        {"id": "market-b", "source": "metaculus"},
    ]


def test_select_questions_rejects_invalid_or_empty_selection():
    with pytest.raises(ValueError, match="at least 1"):
        smoke_test.select_questions([{"id": "a", "source": "fred"}], sample_size=0)

    with pytest.raises(ValueError, match="No questions selected"):
        smoke_test.select_questions([], sample_size=1)


def test_exit_code_for_results_fails_on_empty_or_any_failure():
    assert smoke_test.exit_code_for_results([]) == 1
    assert smoke_test.exit_code_for_results([SimpleNamespace(status=smoke_test.PASS)]) == 0
    assert smoke_test.exit_code_for_results([SimpleNamespace(status=smoke_test.FAIL)]) == 1


def _passthrough_forecast_io():
    return SimpleNamespace(
        write_final_forecast_file=lambda **kwargs: kwargs["forecast_result"],
    )


def test_run_smoke_test_continues_after_model_failure(monkeypatch):
    model_runs = [
        SimpleNamespace(
            slug="bad",
            lab=SimpleNamespace(name="Lab"),
            provider=SimpleNamespace(name="Provider"),
        ),
        SimpleNamespace(
            slug="good",
            lab=SimpleNamespace(name="Lab"),
            provider=SimpleNamespace(name="Provider"),
        ),
    ]
    questions = [{"id": "q1", "source": "manifold"}]
    run_calls = []

    def run_one(**kwargs):
        run_calls.append(kwargs)
        if kwargs["model_run"].slug == "bad":
            raise RuntimeError("provider unavailable")
        return [
            SimpleNamespace(
                local_filename="/tmp/smoke/good.json",
                rows=[{"id": "q1", "forecast": 0.5}],
            )
        ]

    monkeypatch.setattr(smoke_test, "_get_runner", lambda: SimpleNamespace(run_model=run_one))
    monkeypatch.setattr(smoke_test, "_get_llm_forecaster_io_module", _passthrough_forecast_io)

    smoke_run = smoke_test.run_smoke_test(
        model_runs=model_runs,
        question_set=SimpleNamespace(
            forecast_due_date="2026-05-10",
            question_set_filename="2026-05-10-llm.json",
            questions=questions,
        ),
        output_dir="/tmp/smoke",
    )

    assert [result.status for result in smoke_run.results] == [smoke_test.FAIL, smoke_test.PASS]
    assert smoke_run.forecast_file_paths == ["/tmp/smoke/good.json"]
    assert [
        {
            "model_run": call["model_run"].slug,
            "output_dir": call["output_dir"],
            "is_test": call["is_test"],
            "raise_on_question_error": call["raise_on_question_error"],
        }
        for call in run_calls
    ] == [
        {
            "model_run": "bad",
            "output_dir": "/tmp/smoke",
            "is_test": True,
            "raise_on_question_error": False,
        },
        {
            "model_run": "good",
            "output_dir": "/tmp/smoke",
            "is_test": True,
            "raise_on_question_error": False,
        },
    ]


def test_run_smoke_test_writes_partial_files_and_reports_incomplete_rows(monkeypatch):
    model_run = SimpleNamespace(
        slug="partial",
        lab=SimpleNamespace(name="Lab"),
        provider=SimpleNamespace(name="Provider"),
    )
    run_calls = []

    def run_one(**kwargs):
        run_calls.append(kwargs)
        return [
            SimpleNamespace(
                local_filename="/tmp/smoke/partial.json",
                rows=[
                    {"id": "dataset-1", "forecast": 0.2, "resolution_date": "2026-06-14"},
                    {"id": "market-1", "forecast": 0.4, "resolution_date": None},
                ],
            )
        ]

    monkeypatch.setattr(smoke_test, "_get_runner", lambda: SimpleNamespace(run_model=run_one))
    monkeypatch.setattr(smoke_test, "_get_llm_forecaster_io_module", _passthrough_forecast_io)

    smoke_run = smoke_test.run_smoke_test(
        model_runs=[model_run],
        question_set=SimpleNamespace(
            forecast_due_date="2026-05-10",
            question_set_filename="2026-05-10-llm.json",
            questions=[
                {
                    "id": "dataset-1",
                    "source": "fred",
                    "resolution_dates": ["2026-06-14", "2026-07-07"],
                },
                {"id": "market-1", "source": "manifold"},
            ],
        ),
        output_dir="/tmp/smoke",
    )

    assert run_calls[0]["raise_on_question_error"] is False
    assert smoke_run.forecast_file_paths == ["/tmp/smoke/partial.json"]
    assert smoke_run.results[0].status == smoke_test.FAIL
    assert smoke_run.results[0].error_type == "PartialForecast"
    assert smoke_run.results[0].error_message == (
        "Expected 3 rows per forecast file; /tmp/smoke/partial.json has 2."
    )


def test_expected_rows_per_forecast_file_counts_market_questions_once():
    question_set = SimpleNamespace(
        questions=[
            {
                "id": "dataset-1",
                "source": "fred",
                "resolution_dates": ["2026-06-14", "2026-07-07"],
            },
            {
                "id": "market-1",
                "source": "manifold",
                "resolution_dates": ["2026-06-14", "2026-07-07", "2026-09-05"],
            },
        ]
    )

    assert smoke_test._expected_rows_per_forecast_file(question_set) == 3


def test_run_smoke_test_records_runtime_timing(monkeypatch):
    model_run = SimpleNamespace(
        slug="timed",
        lab=SimpleNamespace(name="Lab"),
        provider=SimpleNamespace(name="Provider"),
    )
    clock_values = iter([10.0, 12.5])

    def run_one(**kwargs):
        assert "timing_recorder" not in kwargs
        return [
            SimpleNamespace(
                local_filename="/tmp/smoke/timed.json",
                rows=[{"id": "q1", "forecast": 0.5}],
            )
        ]

    monkeypatch.setattr(smoke_test, "_get_runner", lambda: SimpleNamespace(run_model=run_one))
    monkeypatch.setattr(smoke_test, "_get_llm_forecaster_io_module", _passthrough_forecast_io)
    monkeypatch.setattr(smoke_test, "perf_counter", lambda: next(clock_values))

    smoke_run = smoke_test.run_smoke_test(
        model_runs=[model_run],
        question_set=SimpleNamespace(
            forecast_due_date="2026-05-10",
            question_set_filename="2026-05-10-llm.json",
            questions=[{"id": "q1", "source": "manifold"}],
        ),
        output_dir="/tmp/smoke",
    )

    assert smoke_run.results[0].runtime_seconds == 2.5


def test_run_smoke_test_marks_empty_returned_rows_as_failure(monkeypatch):
    model_run = SimpleNamespace(
        slug="empty",
        lab=SimpleNamespace(name="Lab"),
        provider=SimpleNamespace(name="Provider"),
    )

    monkeypatch.setattr(
        smoke_test,
        "_get_runner",
        lambda: SimpleNamespace(
            run_model=lambda **kwargs: [
                SimpleNamespace(local_filename="/tmp/smoke/empty.json", rows=[])
            ]
        ),
    )
    monkeypatch.setattr(smoke_test, "_get_llm_forecaster_io_module", _passthrough_forecast_io)

    smoke_run = smoke_test.run_smoke_test(
        model_runs=[model_run],
        question_set=SimpleNamespace(
            forecast_due_date="2026-05-10",
            question_set_filename="2026-05-10-llm.json",
            questions=[{"id": "q1", "source": "manifold"}],
        ),
        output_dir="/tmp/smoke",
    )

    assert smoke_run.results[0].status == smoke_test.FAIL
    assert smoke_run.results[0].error_type == "EmptyForecast"
    assert smoke_run.forecast_file_paths == ["/tmp/smoke/empty.json"]


def test_log_results_includes_runtime_timing(caplog):
    caplog.set_level(logging.INFO, logger=smoke_test.__name__)

    smoke_test._log_results(
        smoke_test.SmokeRun(
            results=[
                smoke_test.SmokeResult(
                    model_name="timed",
                    lab="Lab",
                    provider="Provider",
                    status=smoke_test.PASS,
                    error_type="",
                    error_message="",
                    runtime_seconds=1.2345,
                )
            ],
            forecast_file_paths=["/tmp/smoke/timed.json"],
        )
    )

    assert [record.message for record in caplog.records] == [
        "model=timed lab=Lab provider=Provider status=PASS error_type= error=",
        "timing model=timed runtime_seconds=1.234",
    ]


def test_main_uses_latest_metadata_and_configured_paths(monkeypatch):
    calls = {}
    expected_output_dir = Path(smoke_test.SMOKE_OUTPUT_DIR) / "run-1"
    selected_run = SimpleNamespace(
        model_run_key="b-run-variant-01",
        slug="b-slug",
        lab=SimpleNamespace(name="Lab B"),
        provider=SimpleNamespace(name="Provider B"),
    )
    configured_runs = [
        SimpleNamespace(
            model_run_key="a-run-variant-01",
            slug="a-slug",
            lab=SimpleNamespace(name="Lab A"),
            provider=SimpleNamespace(name="Provider A"),
        ),
    ]
    available_runs = [
        *configured_runs,
        selected_run,
    ]

    monkeypatch.delenv("FORECAST_DUE_DATE", raising=False)
    monkeypatch.setattr(
        smoke_test.sys,
        "argv",
        ["smoke_test.py", "--model-run", "b-run-variant-01", "--sample-size", "2"],
    )

    def read_question_set_json(filename, run_locally=False):
        calls["read_question_set_json"] = (filename, run_locally)
        return {
            "forecast_due_date": "2026-05-10",
            "question_set": "2026-05-10-llm.json",
            "questions": [
                {"id": "dataset-2", "source": "fred"},
                {"id": "market-2", "source": "metaculus"},
                {"id": "dataset-1", "source": "acled"},
                {"id": "market-1", "source": "manifold"},
                {"id": "dataset-3", "source": "wikipedia"},
                {"id": "market-3", "source": "polymarket"},
            ],
        }

    class FakeQuestionSet(SimpleNamespace):
        @classmethod
        def from_question_set_json(cls, data):
            calls["parsed_question_set_json"] = data
            return cls(
                forecast_due_date=data["forecast_due_date"],
                question_set_filename=data["question_set"],
                questions=data["questions"],
            )

    def configure_provider_keys(selected_runs):
        calls["configured"] = list(selected_runs)

    def run_smoke(model_runs, question_set, output_dir=smoke_test.SMOKE_OUTPUT_DIR):
        calls["smoke"] = (list(model_runs), question_set, output_dir)
        return smoke_test.SmokeRun(
            results=[
                smoke_test.SmokeResult(
                    model_name="b",
                    lab="Lab B",
                    provider="Provider B",
                    status=smoke_test.FAIL,
                    error_type="RuntimeError",
                    error_message="failed",
                )
            ],
            forecast_file_paths=["/tmp/smoke/b.json"],
        )

    monkeypatch.setattr(
        smoke_test,
        "_get_io_module",
        lambda: SimpleNamespace(
            get_latest_llm_question_set_metadata=lambda run_locally=False: {
                "forecast_due_date": "2026-05-10",
                "question_set": "latest",
            },
            read_question_set_json=read_question_set_json,
        ),
    )
    monkeypatch.setattr(
        smoke_test,
        "_get_question_set_module",
        lambda: SimpleNamespace(
            QuestionSet=FakeQuestionSet,
        ),
    )
    monkeypatch.setattr(
        smoke_test,
        "_get_fb_model_runs_module",
        lambda: SimpleNamespace(
            FB_MODEL_RUNS=configured_runs,
            configure_and_validate_provider_keys=configure_provider_keys,
        ),
    )
    monkeypatch.setattr(
        smoke_test,
        "_get_shared_model_runs_module",
        lambda: SimpleNamespace(MODEL_RUNS=available_runs),
    )
    monkeypatch.setattr(smoke_test, "run_smoke_test", run_smoke)
    monkeypatch.setattr(smoke_test, "_new_output_dir", lambda: expected_output_dir)

    with pytest.raises(SystemExit) as exc_info:
        smoke_test.main()

    assert exc_info.value.code == 1
    assert calls["read_question_set_json"] == ("2026-05-10-llm.json", False)
    assert calls["parsed_question_set_json"]["forecast_due_date"] == "2026-05-10"
    assert calls["configured"] == [selected_run]
    selected_model_runs, selected_question_set, output_dir = calls["smoke"]
    assert selected_model_runs == [selected_run]
    assert selected_question_set.questions == [
        {"id": "dataset-1", "source": "acled"},
        {"id": "dataset-2", "source": "fred"},
        {"id": "market-1", "source": "manifold"},
        {"id": "market-2", "source": "metaculus"},
    ]
    assert selected_question_set.forecast_due_date == "2026-05-10"
    assert selected_question_set.question_set_filename == "2026-05-10-llm.json"
    assert output_dir == expected_output_dir
    assert output_dir.parent == Path(smoke_test.SMOKE_OUTPUT_DIR)
