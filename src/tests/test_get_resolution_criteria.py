"""Resolution criteria come from the source template, or from each question when it is empty."""

import pandas as pd
import pytest

from question_sampling.create_question_set import get_resolution_criteria


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
