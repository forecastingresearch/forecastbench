import importlib
import sys
import types

import pytest

_MISSING = object()


@pytest.fixture()
def import_nightly_worker_with_cloud_run_stub(monkeypatch):
    module_names = [
        "helpers.cloud_run",
        "helpers.question_curation",
        "nightly_update_workflow.worker.main",
    ]
    original_modules = {name: sys.modules.get(name, _MISSING) for name in module_names}
    helpers_package = sys.modules.get("helpers")
    original_helpers_cloud_run = (
        getattr(helpers_package, "cloud_run", _MISSING) if helpers_package else _MISSING
    )
    original_helpers_question_curation = (
        getattr(helpers_package, "question_curation", _MISSING) if helpers_package else _MISSING
    )
    worker_package = sys.modules.get("nightly_update_workflow.worker")
    original_worker_main = getattr(worker_package, "main", _MISSING) if worker_package else _MISSING

    cloud_run = types.ModuleType("helpers.cloud_run")
    cloud_run.timeout_1h = 3600
    cloud_run.run_job = None
    cloud_run.block_and_check_job_result = None

    question_curation = types.ModuleType("helpers.question_curation")
    question_curation.is_today_question_set_publication_date = lambda: False
    question_curation.is_today_question_curation_date = lambda: False

    sys.modules.pop("nightly_update_workflow.worker.main", None)
    monkeypatch.setitem(sys.modules, "helpers.cloud_run", cloud_run)
    monkeypatch.setitem(sys.modules, "helpers.question_curation", question_curation)
    if helpers_package:
        helpers_package.cloud_run = cloud_run
        helpers_package.question_curation = question_curation

    try:
        yield importlib.import_module("nightly_update_workflow.worker.main")
    finally:
        sys.modules.pop("nightly_update_workflow.worker.main", None)
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
            if original_helpers_question_curation is _MISSING:
                try:
                    del helpers_package_after.question_curation
                except AttributeError:
                    pass
            else:
                helpers_package_after.question_curation = original_helpers_question_curation
        worker_package_after = sys.modules.get("nightly_update_workflow.worker")
        if worker_package_after:
            if original_worker_main is _MISSING:
                try:
                    del worker_package_after.main
                except AttributeError:
                    pass
            else:
                worker_package_after.main = original_worker_main


def test_publish_question_set_make_llm_baseline_uses_refactored_manager(
    monkeypatch,
    import_nightly_worker_with_cloud_run_stub,
):
    worker = import_nightly_worker_with_cloud_run_stub
    monkeypatch.setattr(
        worker.question_curation,
        "is_today_question_set_publication_date",
        lambda: True,
    )

    jobs = worker.get_publish_question_set_make_llm_baseline()

    assert jobs == [
        [
            ("func-question-set-publish", True, worker.cloud_run.timeout_1h, 1),
            ("func-llm-forecaster-manager", True, worker.cloud_run.timeout_1h * 24, 1),
        ],
    ]
