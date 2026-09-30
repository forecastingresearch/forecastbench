"""Question curation culls DBnomics weather, ACLED x10, and zero-baseline ACLED questions."""

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


def test_other_sources_untouched():
    dfq = pd.DataFrame(
        {
            "id": ["meteofrance_x"],
            "question": ["Will there be more than ten times as many ... in Nauru for the 30 days"],
        }
    )
    result = create_question_set.drop_culled_questions(source="polymarket", dfq=dfq)
    assert len(result) == 1
