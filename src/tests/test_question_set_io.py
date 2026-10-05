"""Tests for assembling and writing the question set files."""

import json
from datetime import datetime, timedelta

import pandas as pd
import pytest

from helpers import constants
from orchestration import _io
from question_sampling.create_question_set import (
    QuestionSetTarget,
    build_question_set,
    get_resolution_criteria,
    keep_current_forecast_horizons,
)

FORECAST_DATETIME = datetime(2026, 10, 13)

_BANK_COLUMNS = [
    "id",
    "source",
    "question",
    "resolution_criteria",
    "background",
    "market_info_open_datetime",
    "market_info_close_datetime",
    "market_info_resolution_criteria",
    "url",
    "freeze_datetime",
    "freeze_datetime_value",
    "freeze_datetime_value_explanation",
    "source_intro",
    "forecast_horizons",
]


@pytest.mark.parametrize("criteria", [None, "", "  ", "N/A", float("nan")])
def test_custom_criteria_required_for_every_question(criteria):
    dfq = pd.DataFrame({"resolution_criteria": ["Valid criteria", criteria]})
    with pytest.raises(ValueError, match="update the question bank"):
        get_resolution_criteria(dfq, "")


def test_custom_criteria_required_in_older_question_banks():
    with pytest.raises(ValueError, match="update the question bank"):
        get_resolution_criteria(pd.DataFrame({"id": ["q1"]}), "")


def test_source_wide_criteria_still_use_each_question_url():
    dfq = pd.DataFrame(
        {
            "url": ["https://example.com/a", "https://example.com/b"],
            "resolution_criteria": [None, "old"],
        }
    )
    assert get_resolution_criteria(dfq, "Resolves at {url}.").tolist() == [
        "Resolves at https://example.com/a.",
        "Resolves at https://example.com/b.",
    ]


def test_question_set_file_is_written_as_raw_utf8_not_ascii_escapes(tmp_path):
    question_text = "Will the “Südmo – décor” index rise?"
    dfq = pd.DataFrame(
        [["q1", "dbnomics", question_text] + ["N/A"] * 10 + [[7]]],
        columns=_BANK_COLUMNS,
    )
    question_set = build_question_set(
        {"dbnomics": {"dfq": dfq}}, QuestionSetTarget.LLM, FORECAST_DATETIME
    )

    filename = tmp_path / question_set["question_set"]
    _io.write_question_set_file(filename, question_set)

    raw = filename.read_bytes()
    assert b"\\u" not in raw
    assert question_text.encode("utf-8") in raw
    with open(filename, encoding="utf-8") as f:
        assert json.load(f)["questions"][0]["question"] == question_text


def test_question_set_only_asks_current_forecast_horizons():
    """Stale horizon lists in the question bank must not leak into the question set."""
    dfq = pd.DataFrame(
        [
            ["q1", "dbnomics", "Q1?"] + ["N/A"] * 10 + [[7, 30, 90, 180, 365, 1095, 1825, 3650]],
            ["q2", "dbnomics", "Q2?"] + ["N/A"] * 10 + [[30, 365]],
        ],
        columns=_BANK_COLUMNS,
    )

    dfq = keep_current_forecast_horizons(dfq)
    question_set = build_question_set(
        {"dbnomics": {"dfq": dfq}}, QuestionSetTarget.LLM, FORECAST_DATETIME
    )

    assert question_set["forecast_due_date"] == "2026-10-13"
    assert question_set["question_set"] == "2026-10-13-llm.json"
    questions = {q["id"]: q for q in question_set["questions"]}
    forecast_date = FORECAST_DATETIME.date()
    expected_q1 = [
        (forecast_date + timedelta(days=h)).isoformat() for h in constants.FORECAST_HORIZONS_IN_DAYS
    ]
    assert questions["q1"]["resolution_dates"] == expected_q1
    assert questions["q2"]["resolution_dates"] == [
        (forecast_date + timedelta(days=h)).isoformat() for h in [30, 365]
    ]
    assert "forecast_horizons" not in questions["q1"]
