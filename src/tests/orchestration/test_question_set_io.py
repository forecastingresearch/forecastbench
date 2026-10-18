import io
import json
import os
from pathlib import Path

import pandas as pd
import pytest

from helpers import git, keys
from orchestration import _io

ROOT = Path(__file__).resolve().parents[3]
RAW_QUESTION_SET_REQUIREMENTS_WITHOUT_GITPYTHON = [
    ROOT / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_manager" / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_worker" / "requirements.txt",
    ROOT / "src" / "base_eval" / "naive_and_dummy_forecasters" / "requirements.txt",
]


def test_read_question_set_json_from_local_file(tmp_path):
    path = tmp_path / "2026-05-10-llm.json"
    path.write_text(
        json.dumps(
            {
                "forecast_due_date": "2026-05-10",
                "question_set": "2026-05-10-llm.json",
                "questions": [{"id": "q1", "source": "fred"}],
            }
        )
    )

    data = _io.read_question_set_json(str(path), run_locally=True)

    assert data["forecast_due_date"] == "2026-05-10"
    assert data["question_set"] == "2026-05-10-llm.json"
    assert data["questions"] == [{"id": "q1", "source": "fred"}]


def test_question_set_file_is_written_as_raw_utf8_not_ascii_escapes(tmp_path):
    question_text = "Will the “Südmo – décor” index rise?"
    question_set = {
        "forecast_due_date": "2026-10-13",
        "question_set": "2026-10-13-llm.json",
        "questions": [{"id": "q1", "source": "dbnomics", "question": question_text}],
    }
    filename = tmp_path / question_set["question_set"]

    _io.write_question_set_file(filename, question_set)

    raw = filename.read_bytes()
    assert b"\\u" not in raw
    assert question_text.encode("utf-8") in raw
    with open(filename, encoding="utf-8") as f:
        assert json.load(f)["questions"][0]["question"] == question_text


def test_question_set_is_uploaded_under_its_name_and_as_the_latest_of_its_kind(
    monkeypatch, tmp_path
):
    """Both copies land at the root of the question-sets bucket, where the publish job looks."""
    monkeypatch.setenv("QUESTION_SETS_BUCKET", "question-sets")
    uploads = []
    monkeypatch.setattr(_io.gcp.storage, "upload", lambda **kwargs: uploads.append(kwargs))
    local_filename = tmp_path / "2026-10-13-human.json"
    local_filename.write_text("{}")

    _io.upload_question_set(local_filename, filename="2026-10-13-human.json", target="human")

    assert [
        (u["bucket_name"], u["local_filename"], u.get("destination_folder", ""), u["filename"])
        for u in uploads
    ] == [
        ("question-sets", str(local_filename), "", "2026-10-13-human.json"),
        ("question-sets", str(local_filename), "", "latest-human.json"),
    ]


def _json_response(data):
    return io.BytesIO(json.dumps(data).encode("utf-8"))


def test_read_question_set_json_reads_published_raw_file(monkeypatch):
    calls = []

    def fake_urlopen(url, timeout):
        calls.append((url, timeout))
        return _json_response(
            {
                "forecast_due_date": "2026-05-10",
                "question_set": "2026-05-10-llm.json",
                "questions": [{"id": "q1", "source": "fred"}],
            }
        )

    monkeypatch.setattr(_io, "urlopen", fake_urlopen)

    data = _io.read_question_set_json("2026-05-10-llm.json", run_locally=False)

    assert data["forecast_due_date"] == "2026-05-10"
    assert calls == [
        (
            (f"{_io.DATASETS_QUESTION_SETS_RAW_BASE_URL}" "/2026-05-10-llm.json"),
            _io.QUESTION_SET_READ_TIMEOUT_SECONDS,
        )
    ]


def test_latest_metadata_follows_raw_symlink_pointer(monkeypatch):
    latest_url = f"{_io.DATASETS_QUESTION_SETS_RAW_BASE_URL}/latest-llm.json"
    question_set_url = f"{_io.DATASETS_QUESTION_SETS_RAW_BASE_URL}/2026-05-10-llm.json"
    responses = {
        latest_url: b"2026-05-10-llm.json",
        question_set_url: {
            "forecast_due_date": "2026-05-10",
            "question_set": "2026-05-10-llm.json",
            "questions": [{"id": "q1", "source": "fred"}],
        },
    }
    calls = []

    def fake_urlopen(url, timeout):
        calls.append((url, timeout))
        response = responses[url]
        if isinstance(response, bytes):
            return io.BytesIO(response)
        return _json_response(response)

    monkeypatch.setattr(_io, "urlopen", fake_urlopen)

    metadata = _io.get_latest_llm_question_set_metadata()

    assert metadata == {
        "forecast_due_date": "2026-05-10",
        "question_set": "2026-05-10-llm.json",
    }
    assert calls == [
        (latest_url, _io.QUESTION_SET_READ_TIMEOUT_SECONDS),
        (question_set_url, _io.QUESTION_SET_READ_TIMEOUT_SECONDS),
    ]


def test_read_question_set_json_rejects_paths_that_escape_question_sets():
    with pytest.raises(ValueError, match="must be relative"):
        _io.read_question_set_json("../2026-05-10-llm.json", run_locally=False)

    with pytest.raises(ValueError, match="must be relative"):
        _io.read_question_set_json("/tmp/2026-05-10-llm.json", run_locally=False)


def test_read_question_set_json_preserves_json_error_from_raw_file(monkeypatch):
    def fake_urlopen(url, timeout):
        return io.BytesIO(b"{")

    monkeypatch.setattr(_io, "urlopen", fake_urlopen)

    with pytest.raises(json.JSONDecodeError):
        _io.read_question_set_json("2026-05-10-llm.json", run_locally=False)


def test_download_and_read_question_set_file_still_returns_dataframe(tmp_path):
    path = tmp_path / "2026-05-10-llm.json"
    path.write_text(
        json.dumps(
            {
                "forecast_due_date": "2026-05-10",
                "question_set": "2026-05-10-llm.json",
                "questions": [{"id": "q1", "source": "fred"}],
            }
        )
    )

    df = _io.download_and_read_question_set_file(str(path), run_locally=True)

    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="records") == [{"id": "q1", "source": "fred"}]


def test_get_latest_llm_question_set_metadata_uses_latest_file(monkeypatch):
    observed = {}

    def fake_read(filename, run_locally=False):
        observed["filename"] = filename
        observed["run_locally"] = run_locally
        return {
            "forecast_due_date": "2026-05-10",
            "question_set": "2026-05-10-llm.json",
            "questions": [],
        }

    monkeypatch.setattr(_io, "read_question_set_json", fake_read)

    assert _io.get_latest_llm_question_set_metadata(run_locally=True) == {
        "forecast_due_date": "2026-05-10",
        "question_set": "2026-05-10-llm.json",
    }
    assert observed == {"filename": "latest-llm.json", "run_locally": True}


def test_raw_question_set_readers_do_not_require_gitpython():
    for requirements_path in RAW_QUESTION_SET_REQUIREMENTS_WITHOUT_GITPYTHON:
        requirements = requirements_path.read_text()
        assert "GitPython" not in requirements, requirements_path


@pytest.fixture()
def publish_stubs(monkeypatch):
    """Stub the bucket and the git push around `publish_question_set` and record what it did."""
    monkeypatch.setenv("QUESTION_SETS_BUCKET", "question-sets")
    seen = {
        "bucket": ["2026-09-27-llm.json", "2026-10-11-llm.json", "latest-llm.json"],
        "mirror": None,
        "downloads": [],
        "pushes": [],
    }

    def fake_list(bucket_name, mnt=""):
        assert bucket_name == "question-sets"
        return seen["bucket"]

    def fake_download(bucket_name, filename, local_filename=None):
        assert bucket_name == "question-sets"
        seen["downloads"].append(filename)
        Path(local_filename).write_text(json.dumps({"question_set": filename}))
        return local_filename

    def fake_push(**kwargs):
        seen["pushes"].append(kwargs)
        return True

    monkeypatch.setattr(_io.gcp.storage, "list", fake_list)
    monkeypatch.setattr(_io.gcp.storage, "download", fake_download)
    monkeypatch.setattr(git, "clone_and_push_files", fake_push)
    # setattr would read the old value first, which fetches the secret; patch the dict instead.
    monkeypatch.setitem(
        vars(keys), "API_GITHUB_DATASET_REPO_URL", "git@github.com:org/datasets.git"
    )
    monkeypatch.setattr(
        keys,
        "get_secret_that_may_not_exist",
        lambda name: seen["mirror"] if name == "HUGGING_FACE_REPO_URL" else None,
    )
    return seen


def test_publish_question_set_pushes_the_set_and_a_latest_symlink_to_the_dataset_repo(
    publish_stubs, tmp_path
):
    """The dated set and `latest-llm.json` land in `datasets/question_sets/` in one commit.

    The symlink is committed as one, so its target is the bare name of the dated file next to it:
    that is the pointer `get_latest_llm_question_set_metadata` follows on the raw GitHub URL.
    """
    publish_stubs["mirror"] = "git@hf.co:datasets/org/forecastbench.git"

    _io.publish_question_set("2026-10-11", local_dir=tmp_path)

    local_folder = tmp_path / "datasets" / "question_sets"
    latest = local_folder / "latest-llm.json"
    assert publish_stubs["downloads"] == ["2026-10-11-llm.json"]
    assert latest.is_symlink() and os.readlink(latest) == "2026-10-11-llm.json"
    assert json.loads(latest.read_text()) == {"question_set": "2026-10-11-llm.json"}
    assert publish_stubs["pushes"] == [
        {
            "repo_url": "git@github.com:org/datasets.git",
            "files": {
                str(local_folder / "2026-10-11-llm.json"): (
                    "datasets/question_sets/2026-10-11-llm.json"
                ),
                str(latest): "datasets/question_sets/latest-llm.json",
            },
            "commit_message": "publish 2026-10-11-llm.json.",
            "mirrors": ["git@hf.co:datasets/org/forecastbench.git"],
        }
    ]


def test_publish_question_set_replaces_the_symlink_left_by_the_previous_round(
    publish_stubs, tmp_path
):
    """`/tmp` persists across warm Cloud Run runs, so the last round's symlink may still be there."""
    local_folder = tmp_path / "datasets" / "question_sets"
    local_folder.mkdir(parents=True)
    (local_folder / "2026-09-27-llm.json").write_text("{}")
    (local_folder / "latest-llm.json").symlink_to("2026-09-27-llm.json")

    _io.publish_question_set("2026-10-11", local_dir=tmp_path)

    assert os.readlink(local_folder / "latest-llm.json") == "2026-10-11-llm.json"


def test_publish_question_set_without_a_mirror_pushes_only_to_the_dataset_repo(
    publish_stubs, tmp_path
):
    _io.publish_question_set("2026-10-11", local_dir=tmp_path)

    (push,) = publish_stubs["pushes"]
    assert push["mirrors"] == []


def test_publish_question_set_fails_when_the_set_is_not_in_the_bucket(publish_stubs, tmp_path):
    """No set under today's name means creation did not run; nothing is downloaded or pushed."""
    with pytest.raises(FileNotFoundError, match="2026-10-25-llm.json"):
        _io.publish_question_set("2026-10-25", local_dir=tmp_path)

    assert publish_stubs["downloads"] == []
    assert publish_stubs["pushes"] == []
