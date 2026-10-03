"""Unit tests for the leaderboard's JS artifact writers.

The JS is not golden-frozen: ``LAST_UPDATED_DATE`` is set at import, so structure is asserted, not
bytes.
"""

import pandas as pd

from leaderboard.main import (
    LeaderboardType,
    write_leaderboard_js_file_compact,
    write_leaderboard_js_file_full,
)


def _display_df():
    """A minimal post-formatting leaderboard frame with the display columns the writers read."""
    base = {
        "Rank": 1,
        "Team": "",
        "Team Name": "OrgGood",
        "Model Organization": "OrgGood",
        "Model Organization Logo": "",
        "Model": "GoodModel",
        "Dataset": 0.4,
        "N dataset": 2,
        "Dataset 95% CI": "[0.3, 0.5]",
        "Market": 0.4,
        "N market": 1,
        "Market 95% CI": "[0.3, 0.5]",
        "Overall": 0.4,
        "N": 3,
        "Overall 95% CI": "[0.3, 0.5]",
        "Supers > Forecaster?": "",
        "p-val Supers > Forecaster?": "",
        "Forecaster > Public?": "",
        "p-val Forecaster > Public?": "",
    }
    anon = {**base, "Rank": 2, "Team Name": "Anonymous 7", "Model": "HiddenModel", "Overall": 0.1}
    return pd.DataFrame([base, anon])


def test_full_js_filename_and_embeds_rows():
    out = write_leaderboard_js_file_full(_display_df(), LeaderboardType.BASELINE)
    assert out["filename"] == "leaderboard_baseline_full.js"
    assert "GoodModel" in out["js"]
    # The full page keeps anonymous submissions (behind its toggle); only the compact view drops them.
    assert "HiddenModel" in out["js"]


def test_full_js_filename_is_per_leaderboard_type():
    out = write_leaderboard_js_file_full(_display_df(), LeaderboardType.TOURNAMENT)
    assert out["filename"] == "leaderboard_tournament_full.js"


def test_compact_js_drops_anonymous_submissions():
    out = write_leaderboard_js_file_compact(_display_df(), LeaderboardType.BASELINE)
    assert out["filename"] == "leaderboard_baseline_compact.js"
    assert "GoodModel" in out["js"]
    assert "HiddenModel" not in out["js"]
