"""Unit tests for the ``metadata/validate_questions`` LLM gate.

Same model-boundary pattern as the tagger: MOCK ``metadata_llm.get_metadata_model_response`` and assert
the parsing of its raw text — ``Classification: ok`` -> valid, ``flag`` -> invalid, a missing
``Classification:`` -> left unvalidated (``""``), and an ambiguous verdict -> default-valid. The
model's actual judgement is out of scope.
"""

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
            return "I have no verdict here."  # no 'Classification:' -> None -> left ""
        return "Classification: hmmm"  # ambiguous (neither ok nor flag) -> default True

    with patch.object(validate.metadata_llm, "get_metadata_model_response", side_effect=fake_model):
        out = validate.validate_questions(df)

    by_id = dict(zip(out["id"], out["valid_question"]))
    assert by_id["ok"] is True
    assert by_id["flag"] is False
    assert by_id["noclass"] == ""  # unparseable -> stays unvalidated (driver drops it)
    assert by_id["ambiguous"] is True


def test_incidental_ok_substring_does_not_override_an_explicit_flag():
    # Regression: the parser used a loose ``"ok" in end_resp`` checked before ``"flag"``, so a
    # verdict whose prose contained "ok" inside another word (looks/book/broke) would silently pass
    # a question the model meant to flag. Word-boundary + first-token matching must flag it.
    df = _to_validate(
        [
            {"id": "sneaky_flag", "question": "Q-sneaky"},
            {"id": "ok_then_words", "question": "Q-okwords"},
        ]
    )

    def fake_model(prompt, max_output_tokens=None):
        if "Q-sneaky" in prompt:
            return "Classification:\nThis looks broken and inappropriate, flag"
        return "Classification: ok, this is not a flag"  # first standalone token is 'ok'

    with patch.object(validate.metadata_llm, "get_metadata_model_response", side_effect=fake_model):
        out = validate.validate_questions(df)

    by_id = dict(zip(out["id"], out["valid_question"]))
    assert by_id["sneaky_flag"] is False  # 'flag' wins; the incidental 'ok' in "looks" is ignored
    assert by_id["ok_then_words"] is True  # explicit 'ok' first → valid


def test_inflected_verdicts_and_an_echoed_template_are_read_correctly():
    # A model may answer "Flagged." rather than the bare "flag" the prompt asks for, and may echo
    # the prompt's own 'Classification:\n{{ insert "flag" or "ok"}}' template before its real
    # verdict. Neither may pass an ill-defined question or flag a fine one.
    df = _to_validate(
        [
            {"id": "flagged", "question": "Q-flagged"},
            {"id": "okay", "question": "Q-okay"},
            {"id": "echo", "question": "Q-echo"},
        ]
    )

    def fake_model(prompt, max_output_tokens=None):
        if "Q-flagged" in prompt:
            return "Classification: Flagged - personal information."
        if "Q-okay" in prompt:
            return "Classification: Okay"
        return 'Classification:\n{{ insert "flag" or "ok"}}\n...\nClassification: ok'

    with patch.object(validate.metadata_llm, "get_metadata_model_response", side_effect=fake_model):
        out = validate.validate_questions(df)

    by_id = dict(zip(out["id"], out["valid_question"]))
    assert by_id["flagged"] is False
    assert by_id["okay"] is True
    assert by_id["echo"] is True


def test_model_error_defaults_to_valid():
    df = _to_validate([{"id": "boom", "question": "Q-boom"}])

    with patch.object(
        validate.metadata_llm, "get_metadata_model_response", side_effect=RuntimeError("API down")
    ):
        out = validate.validate_questions(df)

    assert out.loc[out["id"] == "boom", "valid_question"].iloc[0] is True
