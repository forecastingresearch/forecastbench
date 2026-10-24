"""Market questions resolving more than the cap after the forecast due date are not sampled."""

from datetime import timedelta

import pandas as pd

from curate_questions.create_question_set import main as create_question_set
from helpers import question_curation


def _market_frame(rows: dict[str, tuple[int | None, int | None]]) -> pd.DataFrame:
    """Build market questions from {id: (close days after due, resolution days after due)}.

    None stands for "N/A", which is what the question bank stores before a market resolves.
    """
    due = question_curation.FORECAST_DATETIME

    def _iso(days_after_due: int | None) -> str:
        return (
            "N/A" if days_after_due is None else (due + timedelta(days=days_after_due)).isoformat()
        )

    return pd.DataFrame(
        {
            "id": list(rows),
            "market_info_close_datetime": [_iso(close) for close, _ in rows.values()],
            "market_info_resolution_datetime": [
                _iso(resolution) for _, resolution in rows.values()
            ],
        }
    )


def test_market_questions_resolving_after_the_cap_are_dropped():
    cap = create_question_set.MAX_DAYS_TO_MARKET_RESOLUTION
    dfq = _market_frame(
        {
            "early": (30, None),
            "under_cap": (cap - 1, None),
            "at_cap": (cap, None),
            "over_cap": (cap + 1, None),
            "far_over_cap": (cap + 400, None),
            "resolves_over_cap": (30, cap + 1),
            "both_under_cap": (30, 60),
        }
    )
    result = create_question_set.drop_questions_that_resolve_too_late(source="polymarket", dfq=dfq)
    assert result["id"].tolist() == ["early", "under_cap", "at_cap", "both_under_cap"]


def test_empty_market_frame_stays_empty():
    dfq = _market_frame({})
    result = create_question_set.drop_questions_that_resolve_too_late(source="polymarket", dfq=dfq)
    assert result.empty


def test_data_questions_are_untouched():
    dfq = pd.DataFrame(
        {
            "id": ["a"],
            "market_info_close_datetime": ["N/A"],
            "market_info_resolution_datetime": ["N/A"],
        }
    )
    result = create_question_set.drop_questions_that_resolve_too_late(source="acled", dfq=dfq)
    assert result["id"].tolist() == ["a"]
