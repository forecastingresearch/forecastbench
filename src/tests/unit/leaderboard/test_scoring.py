"""Unit tests for `score_models` aggregation + metamorphic invariants (pure, no IO).

Uses the two non-regression scorers (`peer_score`, `brier_skill_score`); these need only the
Naive Forecaster baseline and avoid the `pyfixest` two-way-fixed-effects regression, which is
fragile on tiny synthetic data and is better exercised by the real-data/e2e path. The key
guarantee here is **row-order invariance**: shuffling the input must not change any model's score.
"""

from datetime import date

import pandas as pd

from leaderboard.main import (
    BASELINE_ORG_NAIVE_MODEL,
    MarketQuestionAdjustment,
    brier_skill_score,
    peer_score,
    score_models,
)

# dataset (fred) + market (metaculus) questions, all resolved; matches get_df_info's question_pk.
_QUESTIONS = [
    ("fred", "d1", "2025-01-01_fred_d1_30", 1.0),
    ("fred", "d2", "2025-01-01_fred_d2_30", 0.0),
    ("metaculus", "m1", "2025-01-01_metaculus_m1", 1.0),
    ("metaculus", "m2", "2025-01-01_metaculus_m2", 0.0),
]
_SCORERS = [peer_score, brier_skill_score]


def make_scored_frame(extra_models):
    """Build a combined (post-`get_df_info`) frame: Naive Forecaster baseline + extra models.

    ``extra_models`` is a list of ``(organization, model)`` pairs (model_organization == org).
    Every model forecasts every question; the Naive Forecaster is required by brier_skill_score.
    """
    models = [(BASELINE_ORG_NAIVE_MODEL["organization"], BASELINE_ORG_NAIVE_MODEL["model"])]
    models += list(extra_models)

    rows = []
    for org, model in models:
        offset = 0.0 if model == BASELINE_ORG_NAIVE_MODEL["model"] else 0.05
        for i, (source, qid, qpk, resolved_to) in enumerate(_QUESTIONS):
            rows.append(
                {
                    "organization": org,
                    "model_organization": org,
                    "model": model,
                    "model_pk": f"{org}_{org}_{model}",
                    "source": source,
                    "id": qid,
                    "question_pk": qpk,
                    "forecast": 0.3 + 0.1 * i + offset,
                    "resolved_to": resolved_to,
                    "resolved": True,
                    "first_forecast_due_date": date(2025, 1, 1),
                }
            )
    return pd.DataFrame(rows)


class TestScoreModels:
    """Aggregation shape and order-invariance of `score_models`."""

    def test_one_row_per_model_with_expected_columns(self):
        df = make_scored_frame([("OrgA", "ModelA")])
        lb, qfe = score_models(df, _SCORERS, MarketQuestionAdjustment.MARKET_BRIER)

        assert set(lb["model_pk"]) == {
            "ForecastBench_ForecastBench_Naive Forecaster",
            "OrgA_OrgA_ModelA",
        }
        for col in [
            "n_dataset",
            "n_market",
            "n_overall",
            "peer_score_overall",
            "brier_skill_score_overall",
        ]:
            assert col in lb.columns
        # 2 dataset + 2 market questions per model.
        row = lb[lb["model_pk"] == "OrgA_OrgA_ModelA"].iloc[0]
        assert row["n_dataset"] == 2 and row["n_market"] == 2 and row["n_overall"] == 4
        assert qfe == {}  # no fixed effects without two_way_fixed_effects

    def test_row_order_invariance(self):
        df = make_scored_frame([("OrgA", "ModelA"), ("OrgB", "ModelB")])
        lb1, _ = score_models(df, _SCORERS, MarketQuestionAdjustment.MARKET_BRIER)
        shuffled = df.sample(frac=1, random_state=0).reset_index(drop=True)
        lb2, _ = score_models(shuffled, _SCORERS, MarketQuestionAdjustment.MARKET_BRIER)

        a = lb1.set_index("model_pk").sort_index()
        b = lb2.set_index("model_pk").sort_index()
        pd.testing.assert_frame_equal(
            a[sorted(a.columns)], b[sorted(b.columns)], check_exact=False, atol=1e-9
        )

    def test_overall_is_mean_of_dataset_and_market(self):
        df = make_scored_frame([("OrgA", "ModelA")])
        lb, _ = score_models(df, _SCORERS, MarketQuestionAdjustment.MARKET_BRIER)
        row = lb[lb["model_pk"] == "OrgA_OrgA_ModelA"].iloc[0]
        expected = (row["peer_score_dataset"] + row["peer_score_market"]) / 2
        assert row["peer_score_overall"] == expected
