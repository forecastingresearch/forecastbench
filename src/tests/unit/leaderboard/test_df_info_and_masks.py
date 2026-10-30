"""Unit tests for the leaderboard's pre-scoring helpers: masks, ``get_df_info``, round combining."""

from datetime import date

import pandas as pd

from helpers import constants
from leaderboard.main import combine_forecasting_rounds, get_df_info, get_masks


def _forecast_rows():
    """A small forecast set: one resolved dataset, one resolved + one unresolved market."""
    return pd.DataFrame(
        [
            {
                "id": "d1",
                "source": "fred",
                "resolved": True,
                "imputed": False,
                "forecast": 0.6,
                "resolved_to": 1.0,
                "resolution_date": "2025-01-31",
            },
            {
                "id": "m1",
                "source": "metaculus",
                "resolved": True,
                "imputed": False,
                "forecast": 0.7,
                "resolved_to": 1.0,
                "resolution_date": "2025-01-31",
            },
            {
                "id": "m2",
                "source": "metaculus",
                "resolved": False,
                "imputed": False,
                "forecast": 0.4,
                "resolved_to": None,
                "resolution_date": "2025-01-31",
            },
        ]
    )


class TestGetMasks:
    """Masks split dataset/market by source and fold in resolution status."""

    def test_separates_dataset_and_market(self):
        df = pd.DataFrame({"source": ["fred", "metaculus"], "resolved": [True, True]})
        masks = get_masks(df)
        assert masks["dataset"].tolist() == [True, False]
        assert masks["market"].tolist() == [False, True]

    def test_unresolved_market_flagged(self):
        df = pd.DataFrame({"source": ["metaculus", "metaculus"], "resolved": [True, False]})
        masks = get_masks(df)
        assert masks["market_resolved"].tolist() == [True, False]
        assert masks["market_unresolved"].tolist() == [False, True]


class TestGetDfInfo:
    """`get_df_info` preprocesses a forecast set for one (org, model, due date)."""

    _ORG = {"organization": "OrgA", "model": "ModelA", "model_organization": "OrgA"}

    def test_drops_unresolved_market_and_builds_question_pk(self):
        out = get_df_info(_forecast_rows(), self._ORG, "2025-01-01")

        assert set(out["id"]) == {"d1", "m1"}

        d1 = out[out["id"] == "d1"].iloc[0]
        m1 = out[out["id"] == "m1"].iloc[0]
        assert d1["horizon"] == 30
        assert d1["question_pk"] == "2025-01-01_fred_d1_30"
        assert m1["question_pk"] == "2025-01-01_metaculus_m1"

    def test_sorted_by_due_date_source_id(self):
        # Reversed input proves the sort sets the order; the seeded bootstrap draws from this frame,
        # so a different row order gives different replicates.
        rows = _forecast_rows()
        extra = rows.iloc[[0]].assign(id="d0")  # a second fred row with a lower id
        shuffled = pd.concat([rows.iloc[::-1], extra], ignore_index=True)

        out = get_df_info(shuffled, self._ORG, "2025-01-01")

        assert out["source"].tolist() == ["fred", "fred", "metaculus"]
        assert out["id"].tolist() == ["d0", "d1", "m1"]

    def test_returns_none_when_over_imputed_cutoff(self):
        df = _forecast_rows()
        df["imputed"] = True  # far over the 5% cutoff
        assert get_df_info(df, self._ORG, "2025-01-01") is None

    def test_benchmark_dummy_model_skips_imputed_cutoff(self):
        df = _forecast_rows()
        df["imputed"] = True
        dummy = {
            "organization": constants.BENCHMARK_NAME,
            "model": "Imputed Forecaster",
            "model_organization": constants.BENCHMARK_NAME,
        }
        assert get_df_info(df, dummy, "2025-01-01") is not None


class TestCombineForecastingRounds:
    """`combine_forecasting_rounds` stamps each model's earliest forecast due date."""

    def test_first_forecast_due_date_is_earliest(self):
        org = {"organization": "OrgA", "model": "ModelA", "model_organization": "OrgA"}
        jan = get_df_info(_forecast_rows(), org, "2025-01-01")
        feb = get_df_info(_forecast_rows(), org, "2025-02-01")
        combined = combine_forecasting_rounds([jan, feb])
        assert (combined["first_forecast_due_date"] == date(2025, 1, 1)).all()

    def test_no_duplicate_model_pk_question_pk_after_combine(self):
        # Scoring keys on (model_pk, question_pk), so a collision double-counts a question; the
        # due-date prefix keeps the same question id unique across rounds.
        orgs = [
            {"organization": "OrgA", "model": "ModelA", "model_organization": "OrgA"},
            {"organization": "OrgB", "model": "ModelB", "model_organization": "OrgB"},
        ]
        entries = [
            get_df_info(_forecast_rows(), org, due)
            for org in orgs
            for due in ("2025-01-01", "2025-02-01")
        ]
        combined = combine_forecasting_rounds(entries)
        assert not combined.duplicated(subset=["model_pk", "question_pk"]).any()
        d1_per_model = combined[combined["id"] == "d1"].groupby("model_pk")["question_pk"].nunique()
        assert (d1_per_model == 2).all()
