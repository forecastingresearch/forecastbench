import logging
import subprocess
from pathlib import Path
from types import SimpleNamespace

from utils.llm.provider_registry import PROVIDERS

from helpers import dates
from helpers.run_mode import RunMode
from orchestration.func_llm_forecaster_worker import main as worker

ROOT = Path(__file__).resolve().parents[3]
UTILS_PIN = "git+https://github.com/forecastingresearch/utils@"


def _question_set(dataset_count=3, market_count=3):
    return worker.QuestionSet(
        forecast_due_date="2026-05-10",
        question_set_filename="2026-05-10-llm.json",
        questions=[
            *[{"id": f"d{i}", "source": "fred"} for i in range(1, dataset_count + 1)],
            *[{"id": f"m{i}", "source": "metaculus"} for i in range(1, market_count + 1)],
        ],
    )


def test_parse_env_defaults_missing_test_or_prod_to_test(monkeypatch):
    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
    monkeypatch.delenv("TEST_OR_PROD", raising=False)
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", [SimpleNamespace(slug="model")])

    forecast_due_date, run_mode, model_run = worker.parse_env_vars()

    assert forecast_due_date == "2026-05-10"
    assert run_mode == RunMode.TEST
    assert model_run.slug == "model"


def test_parse_env_non_prod_value_defaults_to_test(monkeypatch):
    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
    monkeypatch.setenv("TEST_OR_PROD", "DEV")
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", [SimpleNamespace(slug="model")])

    assert worker.parse_env_vars()[1] == RunMode.TEST


def test_parse_env_requires_cloud_run_task_index(monkeypatch):
    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.delenv("CLOUD_RUN_TASK_INDEX", raising=False)
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", [SimpleNamespace(slug="model")])

    try:
        worker.parse_env_vars()
    except ValueError as exc:
        assert str(exc) == "CLOUD_RUN_TASK_INDEX must be set."
    else:
        raise AssertionError("parse_env_vars should reject missing CLOUD_RUN_TASK_INDEX")


def test_parse_env_rejects_cloud_run_task_index_outside_model_runs(monkeypatch):
    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "1")
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", [SimpleNamespace(slug="model")])

    try:
        worker.parse_env_vars()
    except ValueError as exc:
        assert str(exc) == "CLOUD_RUN_TASK_INDEX must be between 0 and 0; got 1."
    else:
        raise AssertionError("parse_env_vars should reject out-of-range CLOUD_RUN_TASK_INDEX")


def test_parse_env_logs_running_marker_before_selected_model(monkeypatch, caplog):
    selected_model = SimpleNamespace(slug="selected")
    other_model = SimpleNamespace(slug="other")

    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "1")
    monkeypatch.setenv("TEST_OR_PROD", "TEST")
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", [other_model, selected_model])

    with caplog.at_level(logging.INFO, logger=worker.logger.name):
        worker.parse_env_vars()

    messages = [record.message for record in caplog.records]
    assert messages == [
        "0: namespace(slug='other')",
        "1: 🌟️ running 🌟️ namespace(slug='selected')",
    ]


def test_worker_limits_questions_to_default_dataset_then_market_count_when_not_prod(
    monkeypatch,
):
    calls = {}
    selected_model = SimpleNamespace(slug="model", provider=PROVIDERS["OpenAI"])
    test_question_limit = worker.DEFAULT_TEST_QUESTIONS_PER_TYPE

    monkeypatch.setattr(dates, "get_date_today_as_iso", lambda: "2026-05-09")
    monkeypatch.setattr(
        worker,
        "load_question_set",
        lambda forecast_due_date: _question_set(
            dataset_count=test_question_limit + 1,
            market_count=test_question_limit + 1,
        ),
    )
    monkeypatch.setattr(
        worker.fb_model_runs,
        "configure_and_validate_provider_keys",
        lambda runs: calls.setdefault("configured_runs", runs),
    )

    def fake_forecasts(**kwargs):
        calls["run"] = kwargs
        return iter([])

    monkeypatch.setattr(
        worker, "_all_remote_final_files_exist_message", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(worker, "_upload_llm_call_transcripts", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.runner, "iter_model_forecasts", fake_forecasts)

    worker.run_worker(
        forecast_due_date="2026-05-10",
        run_mode=RunMode.TEST,
        model_run=selected_model,
    )

    question_set = calls["run"]["question_set"]
    assert question_set.forecast_due_date == "2026-05-10"
    assert question_set.question_set_filename == "2026-05-10-llm.json"
    assert [q["id"] for q in question_set.questions] == [
        *[f"d{i}" for i in range(1, test_question_limit + 1)],
        *[f"m{i}" for i in range(1, test_question_limit + 1)],
    ]
    assert calls["configured_runs"] == [selected_model]
    assert calls["run"]["is_test"] is True
    assert calls["run"]["today_date"] == "2026-05-09"


def test_worker_logs_written_forecast_files(monkeypatch, caplog):
    selected_model = SimpleNamespace(slug="model", provider=PROVIDERS["OpenAI"])
    forecast_results = [
        SimpleNamespace(variant=SimpleNamespace(key="zero-shot"), rows=[{"id": "q1"}]),
        SimpleNamespace(
            variant=SimpleNamespace(key="zero-shot-with-freeze-values"),
            rows=[{"id": "q1"}],
        ),
    ]
    written_files = [
        SimpleNamespace(
            variant=SimpleNamespace(key="zero-shot"),
            local_filename=Path("/tmp/zero-shot.json"),
            rows=[{"id": "q1"}, {"id": "q2"}],
        ),
        SimpleNamespace(
            variant=SimpleNamespace(key="zero-shot-with-freeze-values"),
            local_filename=Path("/tmp/freeze.json"),
            rows=[{"id": "q1"}],
        ),
    ]

    monkeypatch.setattr(dates, "get_date_today_as_iso", lambda: "2026-05-09")
    monkeypatch.setattr(worker, "load_question_set", lambda forecast_due_date: _question_set())
    monkeypatch.setattr(
        worker.fb_model_runs, "configure_and_validate_provider_keys", lambda runs: None
    )
    monkeypatch.setattr(
        worker, "_all_remote_final_files_exist_message", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(worker, "_upload_llm_call_transcripts", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        worker,
        "_write_and_upload_forecast_result",
        lambda **kwargs: written_files.pop(0),
    )
    monkeypatch.setattr(
        worker.runner,
        "iter_model_forecasts",
        lambda **kwargs: iter(forecast_results),
    )

    with caplog.at_level(logging.INFO, logger=worker.logger.name):
        worker.run_worker(
            forecast_due_date="2026-05-10",
            run_mode=RunMode.PROD,
            model_run=selected_model,
        )

    assert (
        "Wrote and uploaded 2 LLM forecast files: "
        "zero-shot -> /tmp/zero-shot.json (2 rows); "
        "zero-shot-with-freeze-values -> /tmp/freeze.json (1 rows)"
    ) in caplog.text


def test_worker_prod_does_not_limit_questions_and_uploads_prod(monkeypatch):
    calls = {}
    selected_model = SimpleNamespace(slug="model", provider=PROVIDERS["OpenAI"])

    monkeypatch.setattr(dates, "get_date_today_as_iso", lambda: "2026-05-09")
    monkeypatch.setattr(worker, "load_question_set", lambda forecast_due_date: _question_set())
    monkeypatch.setattr(
        worker.fb_model_runs, "configure_and_validate_provider_keys", lambda runs: None
    )

    def fake_forecasts(**kwargs):
        calls["run"] = kwargs
        return iter([])

    monkeypatch.setattr(
        worker, "_all_remote_final_files_exist_message", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(worker, "_upload_llm_call_transcripts", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.runner, "iter_model_forecasts", fake_forecasts)

    worker.run_worker(
        forecast_due_date="2026-05-10",
        run_mode=RunMode.PROD,
        model_run=selected_model,
    )

    assert [q["id"] for q in calls["run"]["question_set"].questions] == [
        "d1",
        "d2",
        "d3",
        "m1",
        "m2",
        "m3",
    ]
    assert calls["run"]["model_run"] == selected_model
    assert calls["run"]["is_test"] is False
    assert calls["run"]["today_date"] == "2026-05-09"


def test_worker_uploads_each_written_file_before_later_variant_failure(monkeypatch, tmp_path):
    selected_model = SimpleNamespace(slug="model", provider=PROVIDERS["OpenAI"])
    uploaded = []

    monkeypatch.setattr(dates, "get_date_today_as_iso", lambda: "2026-05-09")
    monkeypatch.setattr(worker, "load_question_set", lambda forecast_due_date: _question_set())
    monkeypatch.setattr(
        worker.fb_model_runs, "configure_and_validate_provider_keys", lambda runs: None
    )
    monkeypatch.setattr(
        worker, "_all_remote_final_files_exist_message", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        worker.runner,
        "llm_call_transcript_upload_targets",
        lambda *args, **kwargs: [],
    )

    def fake_forecasts(**kwargs):
        yield SimpleNamespace(
            variant=SimpleNamespace(key="zero-shot"),
            rows=[{"id": "q1"}],
        )
        raise RuntimeError("freeze failed")

    monkeypatch.setattr(worker.runner, "iter_model_forecasts", fake_forecasts)
    monkeypatch.setattr(
        worker,
        "_write_and_upload_forecast_result",
        lambda **kwargs: uploaded.append(kwargs["forecast_result"].variant.key),
    )

    try:
        worker.run_worker(
            forecast_due_date="2026-05-10",
            run_mode=RunMode.TEST,
            model_run=selected_model,
        )
    except RuntimeError as exc:
        assert str(exc) == "freeze failed"
    else:
        raise AssertionError("worker should propagate the later variant failure")

    assert uploaded == ["zero-shot"]


def test_worker_remote_existing_message_checks_all_final_forecast_files(monkeypatch):
    checked = []

    def fake_exists(filename):
        checked.append(filename)
        return True

    monkeypatch.setattr(worker._io, "forecast_file_exists", fake_exists)

    message = worker._all_remote_final_files_exist_message(
        model_run=SimpleNamespace(
            slug="model",
            model_run_key="test-model-run-variant-01",
            lab=SimpleNamespace(name="Test Lab"),
            provider=PROVIDERS["OpenAI"],
        ),
        question_set=_question_set(),
        is_test=True,
    )

    assert message is not None
    assert message.startswith("All final forecast files already exist: ")
    assert checked
    assert all(filename in message for filename in checked)


def test_worker_logs_transcript_upload_failure_without_contents(
    monkeypatch,
    tmp_path,
    caplog,
):
    transcript_file = tmp_path / "calls.llm-calls.md"
    transcript_file.write_text("Market background\nquestion_id: market-1", encoding="utf-8")
    target = SimpleNamespace(
        local_filename=transcript_file,
        destination_blob_name="2026-05-10/test/calls.llm-calls.md",
    )

    monkeypatch.setattr(
        worker.runner,
        "llm_call_transcript_upload_targets",
        lambda *args, **kwargs: [target],
    )

    def fail_upload(local_filename, filename):
        raise RuntimeError(f"cannot upload transcript {filename}")

    monkeypatch.setattr(worker._llm_forecaster_io, "upload_llm_call_transcript", fail_upload)
    caplog.set_level(logging.ERROR, logger=worker.logger.name)

    worker._upload_llm_call_transcripts(
        forecast_due_date="2026-05-10",
        model_run=SimpleNamespace(slug="model"),
        output_dir=str(tmp_path),
        is_test=True,
    )

    assert "Failed to upload LLM call transcript" in caplog.text
    assert "cannot upload transcript" in caplog.text
    assert "Market background" not in caplog.text
    assert "question_id: market-1" not in caplog.text


def test_parse_env_selects_model_by_cloud_run_task_index(monkeypatch):
    model_runs = [
        SimpleNamespace(slug="first"),
        SimpleNamespace(slug="second"),
        SimpleNamespace(slug="third"),
    ]
    monkeypatch.setenv("FORECAST_DUE_DATE", "2026-05-10")
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "1")
    monkeypatch.setenv("TEST_OR_PROD", "PROD")
    monkeypatch.setattr(worker.fb_model_runs, "FB_MODEL_RUNS", model_runs)

    forecast_due_date, run_mode, model_run = worker.parse_env_vars()

    assert forecast_due_date == "2026-05-10"
    assert run_mode == RunMode.PROD
    assert model_run == model_runs[1]


def test_worker_deploy_stages_runtime_requirements_and_shared_code():
    deploy_dir = ROOT / "src/orchestration/func_llm_forecaster_worker"
    makefile = (deploy_dir / "Makefile").read_text()
    requirements = (deploy_dir / "requirements.txt").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(deploy_dir), "deploy"],
        text=True,
    )

    assert "func-llm-forecaster-worker" in makefile
    assert "--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT)" in makefile
    assert "include $(ROOT_DIR)orchestration_upload.mk" in makefile
    assert "ORCHESTRATION_EXTRA_PACKAGES = llm_forecaster" in makefile
    assert (
        f"cat {ROOT}/requirements.runtime.txt requirements.txt > upload/requirements.txt"
        in deploy_recipe
    )
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/sources upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/llm_forecaster upload/llm_forecaster" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_source_io.py upload/orchestration/" in deploy_recipe
    assert (
        f"cp {ROOT}/src/orchestration/_llm_forecaster_io.py upload/orchestration/" in deploy_recipe
    )
    assert f"cp {ROOT}/src/_fb_types.py upload/" in deploy_recipe
    assert f"cp {ROOT}/src/_schemas.py upload/" in deploy_recipe
    assert UTILS_PIN not in requirements
