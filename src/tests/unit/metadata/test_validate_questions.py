"""Unit tests for the ``metadata/validate_questions`` verdict parsing, with the LLM mocked."""

from unittest.mock import patch

import pandas as pd

from metadata.validate_questions import main as validate


def _to_validate(rows):
    """Build a question frame with empty ``valid_question`` (the to-validate marker)."""
    df = pd.DataFrame(rows)
    df["valid_question"] = ""
    return df


def test_validate_parses_each_verdict():
    df = _to_validate(
        [
            {"id": "ok", "question": "Q-ok"},
            {"id": "flag", "question": "Q-flag"},
            {"id": "noclass", "question": "Q-noclass"},
            {"id": "ambiguous", "question": "Q-ambiguous"},
        ]
    )

    def fake_model(prompt, max_output_tokens=None):
        if "Q-ok" in prompt:
            return "Reasoning...\nClassification: ok"
        if "Q-flag" in prompt:
            return "Reasoning...\nClassification: flag"
        if "Q-noclass" in prompt:
            return "I have no verdict here."
        return "Classification: hmmm"

    with patch.object(validate.metadata_llm, "get_metadata_model_response", side_effect=fake_model):
        out = validate.validate_questions(df)

    by_id = dict(zip(out["id"], out["valid_question"]))
    assert by_id["ok"] is True
    assert by_id["flag"] is False
    assert by_id["noclass"] == ""  # left unvalidated; the driver drops it
    assert by_id["ambiguous"] is True


def test_model_error_defaults_to_valid():
    df = _to_validate([{"id": "boom", "question": "Q-boom"}])

    with patch.object(
        validate.metadata_llm, "get_metadata_model_response", side_effect=RuntimeError("API down")
    ):
        out = validate.validate_questions(df)

    assert out.loc[out["id"] == "boom", "valid_question"].iloc[0] is True
