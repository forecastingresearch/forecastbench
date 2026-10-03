"""Unit tests for the ``metadata/tag_questions`` categoriser, with the LLM mocked.

The model's raw text is normalised to a member of ``QUESTION_CATEGORIES``; anything unrecognised
becomes ``"Other"``.
"""

from unittest.mock import patch

import pandas as pd

from helpers import constants
from metadata.tag_questions import main as tag


def _to_tag(rows):
    """Build a question frame with empty ``category`` (the to-tag marker tag_questions reads)."""
    df = pd.DataFrame(rows)
    df["source"] = "manifold"
    df["background"] = "bg"
    df["category"] = ""
    return df


def test_get_categories_parses_normalizes_and_falls_back():
    df = _to_tag(
        [
            {"id": "known", "question": "Q-known"},
            {"id": "quoted", "question": "Q-quoted"},
            {"id": "period", "question": "Q-period"},
            {"id": "unknown", "question": "Q-unknown"},
        ]
    )

    def fake_model(prompt, max_output_tokens=None):
        # The prompt embeds the question text, so route the canned response off it.
        if "Q-known" in prompt:
            return "Science & Tech"
        if "Q-quoted" in prompt:
            return '"Sports"'
        if "Q-period" in prompt:
            return "Politics & Governance."
        return "Banana"

    with patch.object(tag.metadata_llm, "get_metadata_model_response", side_effect=fake_model):
        out = tag.get_categories_from_llm(df)

    by_id = dict(zip(out["id"], out["category"]))
    assert by_id["known"] == "Science & Tech"
    assert by_id["quoted"] == "Sports"
    assert by_id["period"] == "Politics & Governance"
    assert by_id["unknown"] == "Other"
    # Every emitted category is a member of the allowed set — the contract the curator relies on.
    assert set(out["category"]) <= set(constants.QUESTION_CATEGORIES)


def test_already_categorized_rows_are_not_retagged():
    df = _to_tag([{"id": "fresh", "question": "Q-fresh"}, {"id": "done", "question": "Q-done"}])
    df.loc[df["id"] == "done", "category"] = "Sports"

    def fake_model(prompt, max_output_tokens=None):
        assert "Q-done" not in prompt, "must not re-tag an already-categorized question"
        return "Science & Tech"

    with patch.object(tag.metadata_llm, "get_metadata_model_response", side_effect=fake_model) as m:
        out = tag.get_categories_from_llm(df)

    assert m.call_count == 1
    by_id = dict(zip(out["id"], out["category"]))
    assert by_id["fresh"] == "Science & Tech"
    assert by_id["done"] == "Sports"


def test_model_error_falls_back_to_other():
    df = _to_tag([{"id": "boom", "question": "Q-boom"}])

    with patch.object(
        tag.metadata_llm, "get_metadata_model_response", side_effect=RuntimeError("API down")
    ):
        out = tag.get_categories_from_llm(df)

    assert out.loc[out["id"] == "boom", "category"].iloc[0] == "Other"
