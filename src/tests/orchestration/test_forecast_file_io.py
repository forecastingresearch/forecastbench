"""Tests for processed forecast file IO."""

import json

from helpers import env
from orchestration import _io


def test_valid_forecast_files_excludes_nested_test_files(monkeypatch):
    """Do not include date-folder test forecast files in leaderboard inputs."""
    monkeypatch.setattr(
        _io.gcp.storage,
        "list",
        lambda bucket_name, mnt: [
            "2026-05-24/2026-05-24.ForecastBench.model.json",
            "2026-05-24/TEST.2026-05-24.ForecastBench.model.json",
            "TEST.2026-05-24.ForecastBench.root-model.json",
            "2026-05-24/notes.txt",
            "2026-05-25/2026-05-25.ForecastBench.model.json",
        ],
    )

    files, dates = _io.get_valid_forecast_files_and_dates("forecastbench-processed-forecast-sets")

    assert files == [
        "2026-05-24/2026-05-24.ForecastBench.model.json",
        "2026-05-25/2026-05-25.ForecastBench.model.json",
    ]
    assert dates == ["2026-05-24", "2026-05-25"]


def test_write_forecast_file_writes_json_through_io_layer(tmp_path):
    path = tmp_path / "nested" / "forecast.json"
    data = {"organization": "ForecastBench", "forecasts": [{"id": "q1"}]}

    _io.write_forecast_file(path, data)

    assert json.loads(path.read_text(encoding="utf-8")) == data
    assert path.read_text(encoding="utf-8").endswith("\n")


def test_text_file_helpers_write_and_append_through_io_layer(tmp_path):
    path = tmp_path / "nested" / "transcript.md"

    _io.write_text_file(path, "start\n")
    _io.append_text_file(path, "end\n")

    assert path.read_text(encoding="utf-8") == "start\nend\n"


def test_forecast_file_storage_helpers_use_forecast_sets_bucket(monkeypatch, tmp_path):
    calls = {}
    path = tmp_path / "forecast.json"

    def fake_file_exists(**kwargs):
        calls["exists"] = kwargs
        return True

    def fake_upload(**kwargs):
        calls["upload"] = kwargs

    monkeypatch.setattr(
        _io.gcp.storage,
        "file_exists",
        fake_file_exists,
    )
    monkeypatch.setattr(_io.gcp.storage, "upload", fake_upload)

    assert _io.forecast_file_exists("2026-05-10/file.json") is True
    _io.upload_forecast_file(path, "2026-05-10/file.json")

    assert calls["exists"] == {
        "bucket_name": env.FORECAST_SETS_BUCKET,
        "filename": "2026-05-10/file.json",
    }
    assert calls["upload"] == {
        "bucket_name": env.FORECAST_SETS_BUCKET,
        "local_filename": str(path),
        "filename": "2026-05-10/file.json",
    }


def test_generic_io_does_not_expose_llm_transcript_upload_helper():
    assert not hasattr(_io, "upload_llm_call_transcript")
