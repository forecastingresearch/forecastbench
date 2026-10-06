"""Question curation samples only from questions whose metadata marks them as English."""

import pandas as pd

from curate_questions.create_question_set import main as create_question_set


def test_questions_not_in_english_are_dropped():
    dfq = pd.DataFrame(
        {
            "id": ["en", "es", "unclassified"],
            "question": ["Will it rain?", "¿Lloverá?", "Will it snow?"],
            "english": [True, False, False],
        }
    )
    result = create_question_set.drop_questions_not_in_english(dfq)
    assert result["id"].tolist() == ["en"]
    assert "english" not in result.columns
