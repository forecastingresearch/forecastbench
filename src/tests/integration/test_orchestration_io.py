"""Integration: orchestration IO boundary tests (``orchestration/_source_io.py``).

These run against a ``local_bucket`` (a temp storage root) — the real IO code path,
no GCP — and pin the empty-resolution-file subtleties the docstrings warn about.
"""

import json
from unittest.mock import patch

import pandas as pd
import pytest

from helpers import git
from orchestration import _io, _source_io


class TestWriteFetchOutput:
    """``write_fetch_output`` serializes a fetch frame to ``<source>_fetch.jsonl``."""

    def test_round_trips_records(self, local_bucket):
        dff = pd.DataFrame(
            [
                {"id": "1", "question": "Will X?", "probability": 0.5},
                {"id": "2", "question": "Will Y?", "probability": 0.7},
            ]
        )
        _source_io.write_fetch_output("polymarket", dff)

        out = local_bucket.read_fetch("polymarket")
        assert sorted(out["id"].astype(str)) == ["1", "2"]
        assert set(out["question"]) == {"Will X?", "Will Y?"}

    def test_unicode_preserved(self, local_bucket):
        dff = pd.DataFrame([{"id": "1", "question": "¿Será “sí”?"}])
        _source_io.write_fetch_output("polymarket", dff)
        out = local_bucket.read_fetch("polymarket")
        assert out.loc[0, "question"] == "¿Será “sí”?"


class TestListExistingResolutionIds:
    """``list_existing_resolution_ids`` is an existence listing (incl. empty files)."""

    def test_lists_bare_ids(self, local_bucket):
        local_bucket.seed_resolution_file("infer", "q1", [{"id": "q1", "date": "2024-01-01"}])
        local_bucket.seed_resolution_file("infer", "q2", [{"id": "q2", "date": "2024-01-02"}])
        assert _source_io.list_existing_resolution_ids("infer") == {"q1", "q2"}

    def test_includes_empty_files(self, local_bucket):
        # An empty resolution file still counts as "exists" — must NOT be derived from
        # load_existing_resolution_files (which drops empty frames).
        local_bucket.seed_resolution_file("infer", "empty", [])
        local_bucket.seed_resolution_file("infer", "full", [{"id": "full", "date": "2024-01-01"}])
        assert _source_io.list_existing_resolution_ids("infer") == {"empty", "full"}

    def test_empty_when_no_files(self, local_bucket):
        assert _source_io.list_existing_resolution_ids("infer") == set()


class TestLoadExistingResolutionFiles:
    """``load_existing_resolution_files`` downloads and drops empty frames."""

    def test_loads_requested_ids(self, local_bucket):
        local_bucket.seed_resolution_file(
            "infer", "q1", [{"id": "q1", "date": "2024-01-01", "value": 0.5}]
        )
        files = _source_io.load_existing_resolution_files("infer", ids=["q1"])
        assert set(files) == {"q1"}
        assert files["q1"].loc[0, "value"] == 0.5

    def test_drops_empty_frames(self, local_bucket):
        local_bucket.seed_resolution_file("infer", "empty", [])
        local_bucket.seed_resolution_file(
            "infer", "full", [{"id": "full", "date": "2024-01-01", "value": 1.0}]
        )
        files = _source_io.load_existing_resolution_files("infer", ids=["empty", "full"])
        assert set(files) == {"full"}  # empty dropped

    def test_missing_id_silently_skipped(self, local_bucket):
        files = _source_io.load_existing_resolution_files("infer", ids=["nope"])
        assert files == {}


class TestUploadResolutionSet:
    """``upload_resolution_set`` builds the public resolution JSON; only GCS/git are mocked.

    Its serialization is fragile: ``direction.apply(len)`` assumes a sized object and
    ``resolution_date.dt.strftime`` assumes a datetime dtype. Pin both so a dtype regression
    (e.g. resolution_date arriving as str) is caught, since the real producer is otherwise only
    ever mocked.
    """

    def test_serializes_direction_and_dates(self, local_bucket, tmp_path):
        df = pd.DataFrame(
            [
                {
                    "id": "q1",
                    "source": "metaculus",
                    "direction": (),  # empty → normalized to null
                    "resolution_date": pd.Timestamp("2025-01-31"),
                    "resolved_to": 1.0,
                    "resolved": True,
                },
                {
                    "id": "q2",
                    "source": "acled",
                    "direction": ("Battles", 1),  # non-empty → preserved
                    "resolution_date": pd.Timestamp("2025-02-15"),
                    "resolved_to": 0.0,
                    "resolved": True,
                },
            ]
        )
        with patch.object(git, "clone_and_push_files"), patch(
            "helpers.keys.get_secret_that_may_not_exist", return_value=None
        ):
            _io.upload_resolution_set(df, "2025-09-09", "2025-09-09-llm.json")

        # Read what THIS call published to the public-release bucket, never the staging copy in
        # /tmp (a leftover from an earlier run would satisfy the assertions with the writer gone).
        written = local_bucket.read_json(
            "PUBLIC_RELEASE_BUCKET", "datasets/resolution_sets/2025-09-09_resolution_set.json"
        )
        assert written["forecast_due_date"] == "2025-09-09"
        assert written["question_set"] == "2025-09-09-llm.json"
        by_id = {r["id"]: r for r in written["resolutions"]}
        # Empty direction → null; non-empty → preserved (JSON list).
        assert by_id["q1"]["direction"] is None
        assert by_id["q2"]["direction"] == ["Battles", 1]
        # Datetime resolution_date → YYYY-MM-DD strings.
        assert by_id["q1"]["resolution_date"] == "2025-01-31"
        assert by_id["q2"]["resolution_date"] == "2025-02-15"


class TestReadForecastFile:
    """``read_forecast_file`` must SKIP (return None) malformed files, never raise."""

    def _write(self, tmp_path, payload: dict):
        path = tmp_path / "forecast.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)

    def test_missing_question_set_returns_none_not_typeerror(self, tmp_path):
        # Regression: question_set absent → the date regex used to run on None and raise TypeError,
        # aborting the whole resolve job. It must return None so the file is skipped.
        path = self._write(
            tmp_path,
            {
                "organization": "OrgA",
                "model": "ModelA",
                "model_organization": "OrgA",
                "forecasts": [{"id": "q1", "source": "fred", "forecast": 0.5}],
                # no "question_set"
            },
        )
        assert _io.read_forecast_file(path) is None

    @pytest.mark.parametrize("question_set", [123, ["2025-01-01-llm.json"], {"name": "x"}])
    def test_non_string_question_set_returns_none_not_typeerror(self, tmp_path, question_set):
        # A truthy non-string passes the presence check but would make the date regex raise.
        path = self._write(
            tmp_path,
            {
                "organization": "OrgA",
                "model": "ModelA",
                "model_organization": "OrgA",
                "question_set": question_set,
                "forecasts": [{"id": "q1", "source": "fred", "forecast": 0.5}],
            },
        )
        assert _io.read_forecast_file(path) is None

    def test_question_set_without_a_date_returns_none(self, tmp_path):
        path = self._write(
            tmp_path,
            {
                "organization": "OrgA",
                "model": "ModelA",
                "model_organization": "OrgA",
                "question_set": "no-date-here.json",
                "forecasts": [{"id": "q1", "source": "fred", "forecast": 0.5}],
            },
        )
        assert _io.read_forecast_file(path) is None

    def test_well_formed_file_is_read(self, tmp_path):
        path = self._write(
            tmp_path,
            {
                "organization": "OrgA",
                "model": "ModelA",
                "model_organization": "OrgA",
                "question_set": "2025-01-01-llm.json",
                "forecasts": [{"id": "q1", "source": "fred", "forecast": 0.5}],
            },
        )
        data = _io.read_forecast_file(path)
        assert data is not None
        assert data["question_set"] == "2025-01-01-llm.json"
        assert len(data["df"]) == 1  # the forecasts frame is attached


class TestUploadResolutionFiles:
    """``upload_resolution_files`` writes only ``[id, date, value]`` with ISO dates."""

    def test_writes_canonical_columns(self, local_bucket):
        df = pd.DataFrame(
            [
                {"id": "q1", "date": "2024-01-01", "value": 0.5, "extra": "drop me"},
                {"id": "q1", "date": "2024-01-02", "value": 0.6, "extra": "drop me"},
            ]
        )
        _source_io.upload_resolution_files("infer", {"q1": df})

        out = local_bucket.read_resolution_file("infer", "q1")
        assert list(out.columns) == ["id", "date", "value"]
        assert "extra" not in out.columns
        assert len(out) == 2

    def test_round_trip_through_loader(self, local_bucket):
        df = pd.DataFrame([{"id": "q9", "date": "2024-03-03", "value": 0.42}])
        _source_io.upload_resolution_files("metaculus", {"q9": df})

        assert _source_io.list_existing_resolution_ids("metaculus") == {"q9"}
        loaded = _source_io.load_existing_resolution_files("metaculus", ids=["q9"])
        assert loaded["q9"].loc[0, "value"] == 0.42
