import importlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

_MISSING = object()


@pytest.fixture()
def import_cloud_run_with_stubs(monkeypatch):
    """Import helpers.cloud_run with external Cloud Run and Slack deps stubbed."""
    module_names = [
        "helpers.cloud_run",
        "helpers.slack",
        "google.cloud.run_v2",
    ]
    original_modules = {name: sys.modules.get(name, _MISSING) for name in module_names}
    helpers_package = sys.modules.get("helpers")
    original_helpers_cloud_run = (
        getattr(helpers_package, "cloud_run", _MISSING) if helpers_package else _MISSING
    )
    original_helpers_slack = (
        getattr(helpers_package, "slack", _MISSING) if helpers_package else _MISSING
    )
    google_cloud_package = sys.modules.get("google.cloud")
    original_google_cloud_run_v2 = (
        getattr(google_cloud_package, "run_v2", _MISSING) if google_cloud_package else _MISSING
    )

    run_v2 = types.ModuleType("google.cloud.run_v2")
    run_v2.Condition = types.SimpleNamespace(
        State=types.SimpleNamespace(CONDITION_SUCCEEDED="CONDITION_SUCCEEDED")
    )

    slack = types.ModuleType("helpers.slack")
    slack.sent_messages = []
    slack.send_message = lambda message: slack.sent_messages.append(message)

    sys.modules.pop("helpers.cloud_run", None)
    monkeypatch.setitem(sys.modules, "google.cloud.run_v2", run_v2)
    monkeypatch.setitem(sys.modules, "helpers.slack", slack)
    if helpers_package:
        helpers_package.slack = slack
    if google_cloud_package:
        google_cloud_package.run_v2 = run_v2

    try:
        cloud_run = importlib.import_module("helpers.cloud_run")
        yield cloud_run, slack, run_v2
    finally:
        sys.modules.pop("helpers.cloud_run", None)
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
            if original_helpers_slack is _MISSING:
                try:
                    del helpers_package_after.slack
                except AttributeError:
                    pass
            else:
                helpers_package_after.slack = original_helpers_slack
        google_cloud_package_after = sys.modules.get("google.cloud")
        if google_cloud_package_after:
            if original_google_cloud_run_v2 is _MISSING:
                try:
                    del google_cloud_package_after.run_v2
                except AttributeError:
                    pass
            else:
                google_cloud_package_after.run_v2 = original_google_cloud_run_v2


@pytest.fixture()
def stale_cloud_run_parent_attributes(monkeypatch):
    import google.cloud

    import helpers

    stale_run_v2 = types.ModuleType("stale.google.cloud.run_v2")
    stale_slack = types.ModuleType("stale.helpers.slack")
    monkeypatch.setattr(google.cloud, "run_v2", stale_run_v2, raising=False)
    monkeypatch.setattr(helpers, "slack", stale_slack, raising=False)


@pytest.fixture()
def import_cloud_run_with_stale_parent_attributes(
    stale_cloud_run_parent_attributes,
    import_cloud_run_with_stubs,
):
    return import_cloud_run_with_stubs


class _Operation:
    def __init__(self, execution):
        self.execution = execution

    def result(self, timeout):
        return self.execution


def _execution_with_completed_state(state):
    start_time = datetime(2026, 5, 7, 12, 0, tzinfo=timezone.utc)
    return types.SimpleNamespace(
        name="projects/project/locations/location/jobs/job/executions/execution-123",
        conditions=[types.SimpleNamespace(type_="Completed", state=state)],
        start_time=start_time,
        completion_time=start_time + timedelta(seconds=90),
    )


def test_block_and_check_job_result_exits_when_completed_condition_failed(
    import_cloud_run_with_stubs,
):
    cloud_run, slack, _run_v2 = import_cloud_run_with_stubs
    operation = _Operation(_execution_with_completed_state("CONDITION_FAILED"))

    with pytest.raises(SystemExit) as exc_info:
        cloud_run.block_and_check_job_result(
            operation=operation,
            name="llm-forecaster",
            exit_on_error=True,
            additional_slack_message_on_error="extra context",
        )

    assert exc_info.value.code == 1
    assert len(slack.sent_messages) == 2
    assert "execution-123 (llm-forecaster)" in slack.sent_messages[0]
    assert slack.sent_messages[1] == "extra context"


def test_cloud_run_stub_import_leaves_no_parent_package_attribute():
    helpers_package = sys.modules.get("helpers")

    assert helpers_package is None or "cloud_run" not in vars(helpers_package)


def test_cloud_run_fixture_replaces_stale_parent_package_attributes(
    import_cloud_run_with_stale_parent_attributes,
):
    import google.cloud

    import helpers

    cloud_run, slack, run_v2 = import_cloud_run_with_stale_parent_attributes

    assert cloud_run.run_v2 is run_v2
    assert cloud_run.slack is slack
    assert google.cloud.run_v2 is run_v2
    assert helpers.slack is slack
