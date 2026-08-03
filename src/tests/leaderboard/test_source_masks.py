"""Leaderboard question categorization by source, including legacy market sources."""

import pandas as pd


def test_dataset_sources_counted(leaderboard_main):
    df = pd.DataFrame({"source": ["acled", "yfinance", "manifold", "infer"]})
    mask = leaderboard_main.get_dataset_mask(df)
    # dataset sources True; market sources (incl. legacy infer) False
    assert mask.tolist() == [True, True, False, False]


def test_infer_counts_as_market(leaderboard_main):
    # INFER questions must still be categorized as market on the leaderboard.
    df = pd.DataFrame({"source": ["infer", "manifold", "acled"]})
    mask = leaderboard_main.get_market_mask(df)
    assert mask.tolist() == [True, True, False]
