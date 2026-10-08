"""Question curation culls weather, ACLED, single-ticker yfinance, and Metaculus wide-gap questions."""

from pathlib import Path

import pandas as pd

from curate_questions.create_question_set import main as create_question_set


def test_drop_dbnomics_weather_questions():
    dfq = pd.DataFrame(
        {
            "id": ["meteofrance_TEMPERATURE_celsius.07005.D", "ECB_FM_M.U2.EUR.4F.KR.DFR.LEV"],
            "question": ["temp at Abbeville?", "ECB deposit rate?"],
        }
    )
    result = create_question_set.drop_culled_questions(source="dbnomics", dfq=dfq)
    assert result["id"].tolist() == ["ECB_FM_M.U2.EUR.4F.KR.DFR.LEV"]


def test_drop_acled_x10_questions():
    dfq = pd.DataFrame(
        {
            "id": ["a", "b"],
            "question": [
                "Will there be more than ten times as many 'Battles' in Yemen for the 30 days...",
                "Will there be more 'Battles' in Yemen for the 30 days before...",
            ],
            "freeze_datetime_value": ["11.5", "10.5"],
        }
    )
    result = create_question_set.drop_culled_questions(source="acled", dfq=dfq)
    assert result["id"].tolist() == ["b"]


def test_drop_acled_questions_with_a_zero_baseline():
    """A zero 30-day average makes the question "will any event happen", so it is not sampled."""
    dfq = pd.DataFrame(
        {
            "id": ["zero", "small", "large"],
            "question": [
                "Will there be more 'Battles' in Spain for the 30 days before...",
                "Will there be more 'Riots' in Spain for the 30 days before...",
                "Will there be more 'Protests' in Spain for the 30 days before...",
            ],
            "freeze_datetime_value": ["0.0", "0.08333333333333333", "325.75"],
        }
    )
    result = create_question_set.drop_culled_questions(source="acled", dfq=dfq)
    assert result["id"].tolist() == ["small", "large"]


def test_drop_acled_questions_under_a_retired_country_spelling():
    """ACLED served Akrotiri and Dhekelia under two spellings; only the current one is sampled."""
    dfq = pd.DataFrame(
        {
            "id": ["old", "current"],
            "question": [
                "Will there be more 'Protests' in Akrotiri and Dekhelia for the 30 days before...",
                "Will there be more 'Protests' in Akrotiri and Dhekelia for the 30 days before...",
            ],
            "freeze_datetime_value": ["1.5", "1.5"],
        }
    )
    result = create_question_set.drop_culled_questions(source="acled", dfq=dfq)
    assert result["id"].tolist() == ["current"]


def test_other_sources_untouched():
    dfq = pd.DataFrame(
        {
            "id": ["meteofrance_x"],
            "question": ["Will there be more than ten times as many ... in Nauru for the 30 days"],
        }
    )
    result = create_question_set.drop_culled_questions(source="polymarket", dfq=dfq)
    assert len(result) == 1


def test_drop_retired_single_ticker_yfinance_questions():
    """Only pair questions (id X_Y) are sampled; single-ticker rows stay for resolution only."""
    dfq = pd.DataFrame(
        {
            "id": ["AAPL", "AAPL_MSFT", "MSFT"],
            "question": ["Will AAPL go up?", "Will AAPL beat MSFT?", "Will MSFT go up?"],
        }
    )
    result = create_question_set.drop_culled_questions(source="yfinance", dfq=dfq)
    assert result["id"].tolist() == ["AAPL_MSFT"]


def test_drop_metaculus_questions_that_resolve_too_long_after_close():
    """Every listed id is dropped; a question not on the list stays."""
    ids_file = Path(create_question_set.__file__).parent / "metaculus_culled_ids.txt"
    culled = ids_file.read_text().split()
    assert culled, "the culled id set must not be empty"
    dfq = pd.DataFrame(
        {
            "id": culled + ["kept"],
            "question": ["Will it?"] * (len(culled) + 1),
        }
    )
    result = create_question_set.drop_culled_questions(source="metaculus", dfq=dfq)
    assert result["id"].tolist() == ["kept"]


def test_drop_nullified_questions_whatever_their_start_date(monkeypatch):
    """A nullified question is never sampled again, even when its nullification starts later."""
    from datetime import date

    from _fb_types import NullifiedQuestion

    monkeypatch.setitem(
        create_question_set.SOURCE_METADATA,
        "metaculus",
        {
            "nullified_questions": [
                NullifiedQuestion(id="gone", nullification_start_date=date(2020, 1, 1)),
                NullifiedQuestion(id="later", nullification_start_date=date(2999, 1, 1)),
            ]
        },
    )
    dfq = pd.DataFrame({"id": ["gone", "later", "kept"], "question": ["a", "b", "c"]})
    result = create_question_set.drop_nullified_questions(source="metaculus", dfq=dfq)
    assert result["id"].tolist() == ["kept"]
