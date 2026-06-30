import io
import json
from pathlib import Path

import pandas as pd
import pytest

from orchestration import _io

ROOT = Path(__file__).resolve().parents[3]
RAW_QUESTION_SET_REQUIREMENTS_WITHOUT_GITPYTHON = [
    ROOT / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_manager" / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_worker" / "requirements.txt",
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
