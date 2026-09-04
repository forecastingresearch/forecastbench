"""Tests for shared source input/output helpers."""

import json

import pandas as pd

from helpers import env
from orchestration import _source_io


def test_write_fetch_output_preserves_jsonl_rows_and_uploads(monkeypatch, tmp_path):
    """Write every fetch row in order before uploading the resulting JSONL file."""
    path = tmp_path / "example_fetch.jsonl"
    uploaded = {}
    dff = pd.DataFrame(
        [
            {"id": "series_1", "value": 1.5, "label": "Météo-France"},
            {"id": "series_2", "value": "NA", "label": "ECB"},
        ]
    )

    monkeypatch.setattr(
        _source_io.data_utils,
        "generate_filenames",
        lambda source: {"local_fetch": path, "jsonl_fetch": f"{source}_fetch.jsonl"},
    )
    monkeypatch.setattr(
        _source_io.gcp.storage,
        "upload",
        lambda **kwargs: uploaded.update(kwargs),
    )

    _source_io.write_fetch_output("example", dff)

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == dff.to_dict(orient="records")
    assert uploaded == {
        "bucket_name": env.QUESTION_BANK_BUCKET,
        "local_filename": path,
    }
