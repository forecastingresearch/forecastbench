"""The nightly manager must launch enough worker tasks to cover every source it fetches."""

import types
from unittest.mock import patch

import pytest

from tests._module_stubs import imported_with_stubs

WEEKDAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def _stub_cloud_run():
    """Build a stand-in for `helpers.cloud_run`.

    The real module imports `google.cloud.run_v2`, which is unavailable in credential-free test
    runs.
    """
    cloud_run = types.ModuleType("helpers.cloud_run")
    cloud_run.timeout_1h = 3600
    cloud_run.run_job = None
    cloud_run.block_and_check_job_result = None
    cloud_run.call_worker = None
    return cloud_run


@pytest.fixture()
def worker():
    """Import the nightly worker credential-free."""
    with imported_with_stubs(
        "nightly_update_workflow.worker.main",
        {"helpers.cloud_run": _stub_cloud_run()},
    ) as module:
        yield module


@pytest.fixture()
def manager():
    """Import the nightly manager credential-free.

    `helpers.slack` pulls a bot token out of Secret Manager at import time.
    """
    with imported_with_stubs(
        "nightly_update_workflow.manager.main",
        {
            "helpers.cloud_run": _stub_cloud_run(),
            "helpers.slack": types.ModuleType("helpers.slack"),
        },
    ) as module:
        yield module


def _jobs_on(worker, day_of_week):
    with patch.object(worker.dates, "get_datetime_today") as mock_today:
        mock_today.return_value.strftime.return_value = day_of_week
        return worker.get_fetch_and_update()


def _job_names(jobs):
    return {job[0] for group in jobs for job in group}


def test_infer_has_update_but_no_fetch(worker):
    names = _job_names(worker.get_fetch_and_update())
    assert "func-data-infer-update-questions" in names
    assert "func-data-infer-fetch" not in names


def test_active_source_has_both(worker):
    names = _job_names(worker.get_fetch_and_update())
    assert "func-data-manifold-fetch" in names
    assert "func-data-manifold-update-questions" in names


@pytest.mark.parametrize("day_of_week", WEEKDAYS)
def test_manager_launches_enough_tasks_for_every_source(manager, worker, day_of_week):
    # The worker exits any task index >= len(jobs), so an undersized task_count silently drops
    # whole sources -- no error, no log. ACLED is only in the list on Wednesdays, which makes an
    # off-by-one here invisible six days a week.
    jobs = _jobs_on(worker, day_of_week)
    assert manager.get_fetch_and_update_task_count() >= len(jobs)


def test_acled_is_fetched_on_wednesdays(worker):
    names = _job_names(_jobs_on(worker, "Wednesday"))
    assert "func-data-acled-fetch" in names
    assert "func-data-acled-update-questions" in names
