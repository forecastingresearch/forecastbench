"""The publish job: gate on the publication date; deploy under the name the worker dispatches."""

import importlib
import subprocess
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
DEPLOY_DIR = ROOT / "src/orchestration/func_question_set_publish"


@pytest.fixture()
def publish_job():
    return importlib.import_module("orchestration.func_question_set_publish.main")


def test_driver_publishes_the_set_due_today_on_a_publication_date(
    freeze_today, monkeypatch, publish_job
):
    """Publication dates fall every 14 days from the first forecast due date, 2024-07-21."""
    freeze_today(date(2026, 10, 25))
    published = []
    monkeypatch.setattr(
        publish_job._io,
        "publish_question_set",
        lambda forecast_due_date, **kwargs: published.append(forecast_due_date),
    )

    publish_job.driver(None)

    assert published == ["2026-10-25"]


def test_driver_does_nothing_between_publication_dates(freeze_today, monkeypatch, publish_job):
    freeze_today(date(2026, 10, 26))
    monkeypatch.setattr(
        publish_job._io,
        "publish_question_set",
        lambda *args, **kwargs: pytest.fail("published between publication dates"),
    )

    publish_job.driver(None)


def test_publish_job_deploys_the_io_layer_under_the_name_the_nightly_worker_dispatches():
    worker = (ROOT / "src/nightly_update_workflow/worker/main.py").read_text()
    root_makefile = (ROOT / "Makefile").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(DEPLOY_DIR), "deploy"],
        text=True,
    )
    gcloud_command = " ".join(
        line.strip().rstrip("\\").strip() for line in deploy_recipe.splitlines()
    )

    assert '"func-question-set-publish"' in worker
    assert "gcloud run jobs deploy func-question-set-publish --project" in gcloud_command
    assert "$(MAKE) -C src/orchestration/func_question_set_publish" in root_makefile
    assert "src/curate_questions" not in root_makefile
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert (
        f"cat {ROOT}/requirements.runtime.txt requirements.txt > upload/requirements.txt"
        in deploy_recipe
    )
