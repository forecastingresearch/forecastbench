import importlib
import subprocess
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
UTILS_PIN = "git+https://github.com/forecastingresearch/utils@"
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


@pytest.fixture()
def stale_manager_cloud_run_parent_attribute(monkeypatch):
    import helpers

    stale_cloud_run = types.ModuleType("stale.helpers.cloud_run")
    stale_cloud_run.timeout_1h = -1
    monkeypatch.setattr(helpers, "cloud_run", stale_cloud_run, raising=False)


@pytest.fixture()
def import_manager_with_stale_cloud_run_parent_attribute(
    stale_manager_cloud_run_parent_attribute,
    import_manager_with_cloud_run_stub,
):
    return import_manager_with_cloud_run_stub


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


def test_manager_fixture_replaces_stale_parent_cloud_run_attribute(
    import_manager_with_stale_cloud_run_parent_attribute,
):
    import helpers

    manager = import_manager_with_stale_cloud_run_parent_attribute

    assert manager.cloud_run.timeout_1h == 3600
    assert helpers.cloud_run is manager.cloud_run


def test_manager_deploy_stages_runtime_requirements_and_shared_code():
    deploy_dir = ROOT / "src/orchestration/func_llm_forecaster_manager"
    makefile = (deploy_dir / "Makefile").read_text()
    requirements = (deploy_dir / "requirements.txt").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(deploy_dir), "deploy"],
        text=True,
    )

    assert "func-llm-forecaster-manager" in makefile
    assert "--service-account $(WORKFLOW_SERVICE_ACCOUNT)" in makefile
    assert 's/"main.py"/"main.py",' not in makefile
    assert "TEST_OR_PROD=$(if $(filter $(BUILD_ENV),prod),PROD,TEST)" in makefile
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
    assert f"cp {ROOT}/src/_fb_types.py upload/" in deploy_recipe
    assert f"cp {ROOT}/src/_schemas.py upload/" in deploy_recipe
    assert UTILS_PIN not in requirements
