"""`_io.load_question_metadata` / `upload_question_metadata` against a ``local_bucket``."""

import json

import pandas as pd
import pandera.pandas as pa
import pytest

from _schemas import MetadataFrame
from helpers import constants
from orchestration import _io


def test_load_question_metadata_returns_the_typed_empty_frame_when_the_file_is_empty(
    local_bucket, tmp_path
):
    local_bucket.seed_metadata([])

    dfmeta = _io.load_question_metadata(local_filename=str(tmp_path / "question_metadata.jsonl"))

    assert dfmeta.empty
    assert list(dfmeta.columns) == constants.META_DATA_FILE_COLUMNS


def test_load_question_metadata_keeps_the_placeholders_and_the_string_ids_the_file_holds(
    local_bucket, tmp_path
):
    """Metaculus ids are digit strings; inferring dtypes would turn `"00123"` into `123`."""
    rows = [
        {
            "source": "fred",
            "id": "f1",
            "category": "Economics & Business",
            "valid_question": True,
            "english": True,
        },
        {"source": "kalshi", "id": "k1", "category": "Sports", "valid_question": "", "english": ""},
        {
            "source": "metaculus",
            "id": "00123",
            "category": "Sports",
            "valid_question": True,
            "english": "",
        },
    ]
    local_bucket.seed_metadata(rows)

    dfmeta = _io.load_question_metadata(local_filename=str(tmp_path / "question_metadata.jsonl"))

    assert list(dfmeta.columns) == constants.META_DATA_FILE_COLUMNS
    assert dfmeta.to_dict("records") == rows
    MetadataFrame.validate(dfmeta)


def test_upload_question_metadata_writes_the_rows_as_json_lines_to_the_bucket_root(
    local_bucket, tmp_path
):
    dfmeta = pd.DataFrame(
        [("kalshi", "k1", "Sports", True, ""), ("fred", "f1", "Economics & Business", "", True)],
        columns=constants.META_DATA_FILE_COLUMNS,
    )

    _io.upload_question_metadata(dfmeta, local_filename=str(tmp_path / "question_metadata.jsonl"))

    uploaded = local_bucket.question_bank_dir() / constants.META_DATA_FILENAME
    records = [json.loads(line) for line in uploaded.read_text().splitlines()]
    assert records == dfmeta.to_dict("records")
    assert all(list(record) == constants.META_DATA_FILE_COLUMNS for record in records)


@pytest.mark.parametrize(
    "stored_columns",
    [["source", "id", "category", "valid_question"], ["source", "id", "valid_question"]],
    ids=["before english", "before category and english"],
)
def test_load_question_metadata_adds_the_columns_a_file_written_before_they_existed_lacks(
    stored_columns, local_bucket, tmp_path
):
    row = {"source": "kalshi", "id": "k1", "category": "Sports", "valid_question": True}
    local_bucket.seed_metadata([{k: v for k, v in row.items() if k in stored_columns}])

    dfmeta = _io.load_question_metadata(local_filename=str(tmp_path / "question_metadata.jsonl"))

    assert list(dfmeta.columns) == constants.META_DATA_FILE_COLUMNS
    assert dfmeta.to_dict("records") == [
        {
            "source": "kalshi",
            "id": "k1",
            "category": "Sports" if "category" in stored_columns else "",
            "valid_question": True,
            "english": "",
        }
    ]


def test_load_question_metadata_rejects_a_file_with_a_value_outside_the_contract(
    local_bucket, tmp_path
):
    local_bucket.seed_metadata(
        [
            {
                "source": "kalshi",
                "id": "k1",
                "category": "Sports",
                "valid_question": "maybe",
                "english": True,
            }
        ]
    )

    with pytest.raises(pa.errors.SchemaError):
        _io.load_question_metadata(local_filename=str(tmp_path / "question_metadata.jsonl"))


def test_upload_question_metadata_refuses_to_write_a_malformed_frame(local_bucket, tmp_path):
    dfmeta = pd.DataFrame(
        [("kalshi", "k1", "Sports", True, "yes")], columns=constants.META_DATA_FILE_COLUMNS
    )

    with pytest.raises(pa.errors.SchemaError):
        _io.upload_question_metadata(
            dfmeta, local_filename=str(tmp_path / "question_metadata.jsonl")
        )
    assert not (local_bucket.question_bank_dir() / constants.META_DATA_FILENAME).exists()
