"""Tests for the question set writer in create_question_set."""

import json
from datetime import timedelta

import pandas as pd

from curate_questions.create_question_set import main as create_question_set
from curate_questions.create_question_set.main import QuestionSetTarget
from helpers import constants, env, question_curation

_QUESTION_COLUMNS = [
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


def test_write_questions_writes_raw_utf8_not_ascii_escapes(monkeypatch):
    monkeypatch.setattr(env, "RUNNING_LOCALLY", True)
    question_text = "Will the “Südmo – décor” index rise?"
    dfq = pd.DataFrame(
        [["q1", "dbnomics", question_text] + ["N/A"] * 10 + [[7]]],
        columns=_QUESTION_COLUMNS,
    )

    create_question_set.write_questions({"dbnomics": {"dfq": dfq}}, QuestionSetTarget.LLM)

    filename = f"/tmp/{question_curation.FORECAST_DATE.isoformat()}-llm.json"
    with open(filename, "rb") as f:
        raw = f.read()
    assert b"\\u" not in raw
    assert question_text.encode("utf-8") in raw
    with open(filename, encoding="utf-8") as f:
        assert json.load(f)["questions"][0]["question"] == question_text


def test_question_set_only_asks_current_forecast_horizons(monkeypatch):
    """Stale horizon lists in the question bank must not leak into the question set."""
    monkeypatch.setattr(env, "RUNNING_LOCALLY", True)
    dfq = pd.DataFrame(
        [
            ["q1", "dbnomics", "Q1?"] + ["N/A"] * 10 + [[7, 30, 90, 180, 365, 1095, 1825, 3650]],
            ["q2", "dbnomics", "Q2?"] + ["N/A"] * 10 + [[30, 365]],
        ],
        columns=_QUESTION_COLUMNS,
    )

    dfq = create_question_set.keep_current_forecast_horizons(dfq)
    create_question_set.write_questions({"dbnomics": {"dfq": dfq}}, QuestionSetTarget.LLM)

    filename = f"/tmp/{question_curation.FORECAST_DATE.isoformat()}-llm.json"
    with open(filename, encoding="utf-8") as f:
        questions = {q["id"]: q for q in json.load(f)["questions"]}
    forecast_date = question_curation.FORECAST_DATETIME.date()
    expected_q1 = [
        (forecast_date + timedelta(days=h)).isoformat() for h in constants.FORECAST_HORIZONS_IN_DAYS
    ]
    assert questions["q1"]["resolution_dates"] == expected_q1
    assert questions["q2"]["resolution_dates"] == [
        (forecast_date + timedelta(days=h)).isoformat() for h in [30, 365]
    ]
