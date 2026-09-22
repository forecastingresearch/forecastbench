"""Tests for the question set writer in create_question_set."""

import json

import pandas as pd

from curate_questions.create_question_set import main as create_question_set
from curate_questions.create_question_set.main import QuestionSetTarget
from helpers import env, question_curation

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
