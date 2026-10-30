"""Shared history storage contracts."""

import json
from unittest.mock import Mock

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from orchestration import _source_io


@pytest.mark.parametrize("retain_timestamps", [False, True])
def test_upload_preserves_only_requested_columns_when_present(monkeypatch, retain_timestamps):
    """Optional timestamps survive mixed histories; default uploads keep their original schema."""
    timestamp = "2026-09-16T01:00:00+00:00"
    frame = pd.DataFrame(
        [dict(id="history", date="2026-09-15", value=12, fetch_datetime=timestamp, unused="omit")]
    )
    frames = {
        "first": frame,
        "second": frame.copy(),
        "snapshot": frame.drop(columns="fetch_datetime"),
    }
    written = []
    to_json = pd.DataFrame.to_json
    monkeypatch.setattr(
        pd.DataFrame,
        "to_json",
        lambda df, path, **kw: written.append(json.loads(to_json(df, None, **kw))),
    )
    monkeypatch.setattr(_source_io.gcp.storage, "upload", Mock())
    # An iterable must remain usable across every history in the batch.
    options = {"extra_columns": iter(["fetch_datetime"])} if retain_timestamps else {}
    _source_io.upload_resolution_files("example", frames, **options)
    assert len(written) == len(frames)
    for record, frame in zip(written, frames.values()):
        expected = {"id", "date", "value"}
        if retain_timestamps and "fetch_datetime" in frame:
            expected.add("fetch_datetime")
            assert record["fetch_datetime"] == timestamp
        assert set(record) == expected


def test_strict_history_download_does_not_reuse_a_stale_local_copy(monkeypatch):
    """A missing remote history stays missing even when a local copy still exists."""
    stale = pd.DataFrame([dict(id="example", date="2026-09-15", value=99)])
    monkeypatch.setattr(_source_io.os.path, "exists", lambda _: True)
    monkeypatch.setattr(pd, "read_json", lambda *args, **kwargs: stale)
    monkeypatch.setattr(_source_io.gcp.storage, "download", Mock(side_effect=NotFound("missing")))
    assert _source_io.load_existing_resolution_files("example", ids=["example"], strict=True) == {}
