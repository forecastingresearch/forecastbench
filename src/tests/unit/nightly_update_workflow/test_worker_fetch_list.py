"""The nightly manager must launch enough worker tasks to cover every source it fetches."""

from unittest.mock import patch

import pandas as pd
import pytest

from _schemas import MetadataFrame
from helpers import question_curation
from nightly_update_workflow.manager import main as manager_main
from nightly_update_workflow.worker import main as worker_main

WEEKDAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


@pytest.fixture()
def worker():
    """Return the nightly worker entrypoint module."""
    return worker_main


@pytest.fixture()
def manager():
    """Return the nightly manager entrypoint module."""
    return manager_main


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


@pytest.mark.parametrize("source", ["metaculus", "serpapi"])
def test_active_source_has_both(worker, source):
    names = _job_names(worker.get_fetch_and_update())
    assert f"func-data-{source}-fetch" in names
    assert f"func-data-{source}-update-questions" in names


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


def _summarize(manager, dfmeta, dfq):
    """Run the question-bank summary on the given metadata and per-source questions; return it."""
    sent = []
    with (
        patch.object(
            manager.slack, "send_message", side_effect=lambda message: sent.append(message)
        ),
        patch.object(manager._io, "load_question_metadata", return_value=dfmeta),
        patch.object(manager.pd, "read_json", side_effect=lambda path, **kwargs: dfq.copy()),
    ):
        manager.summarize_question_bank()
    (message,) = sent
    return message


def test_question_bank_summary_covers_only_sampled_sources(manager):
    # Wikipedia and INFER still resolve but are no longer sampled, so the summary of what is
    # available for the next question set must leave them out.
    dfmeta = MetadataFrame.validate(
        pd.DataFrame(
            [
                {
                    "id": "q1",
                    "source": s,
                    "category": "Sports",
                    "valid_question": True,
                    "english": True,
                }
                for s in manager.ALL_SOURCE_NAMES
            ]
        )
    )
    message = _summarize(manager, dfmeta, pd.DataFrame([{"id": "q1", "resolved": False}]))
    for source in manager.ALL_SOURCE_NAMES:
        assert (source in message) == (source in question_curation.FREEZE_QUESTION_SOURCES)


def test_question_bank_summary_counts_a_question_as_valid_only_on_a_true_verdict(manager):
    # Between the tag and validate jobs a row holds "" in `valid_question`; it is unresolved but
    # not yet available for sampling, and the summary must still go out.
    source = sorted(question_curation.FREEZE_QUESTION_SOURCES)[0]
    dfmeta = MetadataFrame.validate(
        pd.DataFrame(
            {
                "source": [source] * 4,
                "id": ["q1", "q2", "q3", "q4"],
                "category": ["Sports"] * 4,
                "valid_question": [True, False, "", True],
                "english": [True, True, "", True],
            }
        )
    )
    dfq = pd.DataFrame({"id": ["q1", "q2", "q3", "q4"], "resolved": [False, False, False, True]})
    message = _summarize(manager, dfmeta, dfq)
    (row,) = [line for line in message.splitlines() if line.startswith(f"| {source}")]
    cells = [cell.strip() for cell in row.strip("|").split("|")]
    assert cells == [source, "3", "1"]
