import importlib
import sys
import types

import pytest

_MISSING = object()


@pytest.fixture()
def import_manager_with_cloud_run_stub(monkeypatch):
    module_names = [
        "helpers.cloud_run",
        "orchestration.func_llm_forecaster_manager.main",
    ]
    original_modules = {name: sys.modules.get(name, _MISSING) for name in module_names}
    helpers_package = sys.modules.get("helpers")
    original_helpers_cloud_run = (
        getattr(helpers_package, "cloud_run", _MISSING) if helpers_package else _MISSING
    )
    manager_package = sys.modules.get("orchestration.func_llm_forecaster_manager")
    original_manager_main = (
        getattr(manager_package, "main", _MISSING) if manager_package else _MISSING
    )

    cloud_run = types.ModuleType("helpers.cloud_run")
    cloud_run.timeout_1h = 3600
    cloud_run.call_worker = None
    cloud_run.block_and_check_job_result = None

    sys.modules.pop("orchestration.func_llm_forecaster_manager.main", None)
    monkeypatch.setitem(sys.modules, "helpers.cloud_run", cloud_run)
    if helpers_package:
        helpers_package.cloud_run = cloud_run

    try:
        yield importlib.import_module("orchestration.func_llm_forecaster_manager.main")
    finally:
        sys.modules.pop("orchestration.func_llm_forecaster_manager.main", None)
        for name, original_module in original_modules.items():
            if original_module is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original_module
        helpers_package_after = sys.modules.get("helpers")
        if helpers_package_after:
            if original_helpers_cloud_run is _MISSING:
                try:
                    del helpers_package_after.cloud_run
                except AttributeError:
                    pass
            else:
                helpers_package_after.cloud_run = original_helpers_cloud_run
        manager_package_after = sys.modules.get("orchestration.func_llm_forecaster_manager")
        if manager_package_after:
            if original_manager_main is _MISSING:
                try:
                    del manager_package_after.main
                except AttributeError:
                    pass
            else:
                manager_package_after.main = original_manager_main


def test_main_defaults_missing_env_to_test(
    monkeypatch,
    import_manager_with_cloud_run_stub,
):
    manager = import_manager_with_cloud_run_stub
    calls = {}
    monkeypatch.delenv("TEST_OR_PROD", raising=False)
    monkeypatch.setattr(manager, "run_manager", lambda run_mode: calls.setdefault("mode", run_mode))

    manager.main()

    assert calls["mode"] is manager.RunMode.TEST


def test_main_reads_run_mode_from_env(
    monkeypatch,
    import_manager_with_cloud_run_stub,
):
    manager = import_manager_with_cloud_run_stub
    calls = {}
    monkeypatch.setenv("TEST_OR_PROD", "prod")
    monkeypatch.setattr(manager, "run_manager", lambda run_mode: calls.setdefault("mode", run_mode))

    manager.main()

    assert calls["mode"] is manager.RunMode.PROD


def test_run_manager_uses_io_latest_metadata_and_new_worker(
    monkeypatch,
    import_manager_with_cloud_run_stub,
):
    manager = import_manager_with_cloud_run_stub
    calls = {}

    def fake_call_worker(**kwargs):
        calls["call_worker"] = kwargs
        return "operation"

    monkeypatch.setattr(
        manager._io,
        "get_latest_llm_question_set_metadata",
        lambda: {"forecast_due_date": "2026-05-10", "question_set": "2026-05-10-llm.json"},
    )
    monkeypatch.setattr(manager.fb_model_runs, "FB_MODEL_RUNS", [object(), object(), object()])
    monkeypatch.setattr(manager.cloud_run, "call_worker", fake_call_worker)
    monkeypatch.setattr(
        manager.cloud_run,
        "block_and_check_job_result",
        lambda **kwargs: calls.setdefault("block", kwargs),
    )

    manager.run_manager(manager.RunMode.TEST)

    timeout = manager.cloud_run.timeout_1h * 24
    assert calls["call_worker"] == {
        "job_name": "func-llm-forecaster-worker",
        "env_vars": {
            "FORECAST_DUE_DATE": "2026-05-10",
            "TEST_OR_PROD": "TEST",
        },
        "task_count": 3,
        "timeout": timeout,
    }
    assert calls["block"] == {
        "operation": "operation",
        "name": "llm-forecaster",
        "exit_on_error": True,
        "timeout": timeout,
    }
