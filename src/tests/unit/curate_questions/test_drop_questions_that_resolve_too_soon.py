"""Market questions resolving within the minimum after the forecast due date are not sampled."""

from datetime import timedelta

import pandas as pd

from curate_questions.create_question_set import main as create_question_set
from helpers import question_curation


def _market_frame(rows: dict[str, tuple[timedelta | None, timedelta | None]]) -> pd.DataFrame:
    """Build market questions from {id: (close offset, resolution offset)} after the due date.

    Offsets are measured from the end of the forecast due date. None stands for "N/A", which is
    what the question bank stores before a market resolves.
    """
    due_end_of_day = question_curation.FORECAST_DATETIME.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    def _iso(offset: timedelta | None) -> str:
        return "N/A" if offset is None else (due_end_of_day + offset).isoformat()

    return pd.DataFrame(
        {
            "id": list(rows),
            "market_info_close_datetime": [_iso(close) for close, _ in rows.values()],
            "market_info_resolution_datetime": [
                _iso(resolution) for _, resolution in rows.values()
            ],
        }
    )


def test_market_questions_resolving_within_the_minimum_are_dropped():
    minimum = timedelta(days=create_question_set.MIN_DAYS_TO_MARKET_RESOLUTION)
    dfq = _market_frame(
        {
            "before_due": (timedelta(days=-1), None),
            "at_due": (timedelta(0), None),
            "inside_minimum": (minimum - timedelta(days=1), None),
            "at_minimum": (minimum, None),
            "just_after_minimum": (minimum + timedelta(seconds=1), None),
            "well_after_minimum": (minimum + timedelta(days=30), None),
            "resolves_inside_minimum": (minimum + timedelta(days=30), minimum),
            "both_after_minimum": (minimum + timedelta(days=30), minimum + timedelta(days=60)),
        }
    )
    result = create_question_set.drop_questions_that_resolve_too_soon(source="polymarket", dfq=dfq)
    assert result["id"].tolist() == [
        "just_after_minimum",
        "well_after_minimum",
        "both_after_minimum",
    ]


def test_empty_market_frame_stays_empty():
    dfq = _market_frame({})
    result = create_question_set.drop_questions_that_resolve_too_soon(source="polymarket", dfq=dfq)
    assert result.empty


def test_data_questions_without_horizons_are_dropped():
    dfq = pd.DataFrame({"id": ["a", "b", "c"], "forecast_horizons": [[], "N/A", [7]]})
    result = create_question_set.drop_questions_that_resolve_too_soon(source="acled", dfq=dfq)
    assert result["id"].tolist() == ["c"]
