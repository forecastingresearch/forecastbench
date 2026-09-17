"""Everything the nightly run publishes must reach the dataset repo in a single commit."""

import types

import pytest

from tests._module_stubs import stubbed_modules

BUCKET_BLOBS = [
    "datasets/resolution_sets/2026-08-10_resolution_set.json",
    "datasets/resolution_sets/2026-08-24_resolution_set.json",
    "leaderboards/html/leaderboard_tournament.html",
    "leaderboards/html/leaderboard_baseline.html",
    "leaderboards/html/leaderboard_preliminary.html",
    "leaderboards/csv/leaderboard_tournament.csv",
    "leaderboards/csv/leaderboard_baseline.csv",
    "leaderboards/csv/leaderboard_preliminary.csv",
    "leaderboards/csv/sota_graph_tournament.csv",
    "leaderboards/csv/sota_graph_baseline.csv",
    "question-fixed-effects/question_fixed_effects.baseline_leaderboard.json",
    "question-fixed-effects/question_fixed_effects.tournament_leaderboard.json",
    "question-fixed-effects/question_fixed_effects.preliminary_leaderboard.json",
    "question-fixed-effects/question_fixed_effects.2026-08-26.baseline_leaderboard.json",
    "question-fixed-effects/question_fixed_effects.2026-08-27.baseline_leaderboard.json",
    "simulated_llm_parity/parity_dates.baseline_leaderboard.json",
    "simulated_llm_parity/parity_dates.tournament_leaderboard.json",
]


@pytest.fixture()
def pushes(monkeypatch):
    """Run `push_datasets_to_git` against a stand-in bucket and git remote.

    Returns:
        list: One entry per `clone_and_push_files` call, each the pushed
              `{local path: git path}` mapping.
    """
    from helpers import env
    from orchestration import _io

    monkeypatch.setattr(env, "RUNNING_LOCALLY", False)
    monkeypatch.setattr(
        _io.gcp.storage,
        "list_with_prefix",
        lambda bucket_name, prefix: [b for b in BUCKET_BLOBS if b.startswith(prefix)],
    )
    monkeypatch.setattr(
        _io.gcp.storage,
        "download",
        lambda bucket_name, filename, local_filename: local_filename,
    )

    calls = []
    git = types.ModuleType("helpers.git")
    git.clone_and_push_files = lambda repo_url, files, commit_message, mirrors: (
        calls.append(files) or True
    )
    keys = types.ModuleType("helpers.keys")
    keys.API_GITHUB_DATASET_REPO_URL = "git@example.com:datasets.git"
    keys.get_secret_that_may_not_exist = lambda secret_name: None

    def _run():
        with stubbed_modules({"helpers.git": git, "helpers.keys": keys}):
            _io.push_datasets_to_git()
        return calls

    return _run


def test_every_nightly_artifact_is_published_in_one_commit(pushes):
    """All four artifact types reach the repo, at their published paths, in one commit.

    The question fixed effects are one file per leaderboard, overwritten nightly; the dated
    files that predate that are left in the bucket and must not be pushed.
    """
    calls = pushes()

    assert len(calls) == 1, "the night's artifacts must go out as a single commit"
    assert sorted(calls[0].values()) == sorted(
        [
            "datasets/resolution_sets/2026-08-10_resolution_set.json",
            "datasets/resolution_sets/2026-08-24_resolution_set.json",
            "leaderboards/html/leaderboard_tournament.html",
            "leaderboards/html/leaderboard_baseline.html",
            "leaderboards/html/leaderboard_preliminary.html",
            "leaderboards/csv/leaderboard_tournament.csv",
            "leaderboards/csv/leaderboard_baseline.csv",
            "leaderboards/csv/leaderboard_preliminary.csv",
            "datasets/question_fixed_effects/question_fixed_effects.baseline_leaderboard.json",
            "datasets/question_fixed_effects/question_fixed_effects.tournament_leaderboard.json",
            "datasets/question_fixed_effects/question_fixed_effects.preliminary_leaderboard.json",
            "datasets/parity_dates/parity_dates.baseline_leaderboard.json",
            "datasets/parity_dates/parity_dates.tournament_leaderboard.json",
        ]
    )


def test_nothing_is_pushed_when_running_locally(monkeypatch, pushes):
    """A local run must not write to the dataset repo."""
    from helpers import env

    monkeypatch.setattr(env, "RUNNING_LOCALLY", True)

    assert pushes() == []
