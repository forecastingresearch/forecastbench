"""End-to-end: bucket update, explode, resolve, impute and leaderboard scoring, offline.

polymarket comes through a real bucket update and stays open (a tracked value, ``resolved=False``,
excluded from the leaderboard); metaculus is a resolved market and fred a dataset with two
resolution dates. The ``nullified`` scenario adds a nullified polymarket question that must be
dropped end to end.
"""

from datetime import date

import pandas as pd
import pytest

from _fb_types import SourceQuestionBank
from leaderboard.main import (
    BASELINE_ORG_NAIVE_MODEL,
    MarketQuestionAdjustment,
    brier_skill_score,
    combine_forecasting_rounds,
    get_df_info,
    peer_score,
    score_models,
)
from orchestration.func_polymarket_update.main import driver as polymarket_update_driver
from resolve._impute import impute_missing_forecasts
from resolve._prepare import check_and_prepare_forecast_file, set_resolution_dates
from resolve.explode_question_set import explode_question_set, get_resolution_dates
from resolve.resolve_all import resolve_all
from sources.polymarket import PolymarketSource
from sources.registry import SOURCES
from tests._golden import check_golden
from tests.factories import (
    make_polymarket_fetch_df,
    make_question_df,
    make_question_set_df,
    make_resolution_df,
)

_DUE = date(2025, 1, 1)
_DUE_STR = "2025-01-01"
# Daily polymarket price series spanning the window (yesterday == 2025-01-31 after freeze).
_PRICE_HISTORY = [{"date": f"2025-01-{d:02d}", "value": 0.4 + d * 0.01} for d in range(1, 32)]


def _polymarket_bank_from_bucket(local_bucket) -> SourceQuestionBank:
    """Read back the polymarket bank the update driver persisted to the bucket."""
    dfq = local_bucket.read_questions("polymarket")
    ids = local_bucket.list_resolution_ids("polymarket")
    dfr = pd.concat(
        [local_bucket.read_resolution_file("polymarket", qid) for qid in ids], ignore_index=True
    )
    dfr["date"] = pd.to_datetime(dfr["date"])
    return SourceQuestionBank(dfq=dfq, dfr=dfr)


def _build_bank(local_bucket, nullified_id: str | None) -> dict:
    """Build a 3-source question bank: polymarket via bucket update, metaculus + fred seeded."""
    polymarket_rows = [
        {"id": "polymarket1", "resolved": False, "historical_prices": _PRICE_HISTORY}
    ]
    if nullified_id:
        polymarket_rows.append(
            {"id": nullified_id, "resolved": False, "historical_prices": _PRICE_HISTORY}
        )
    local_bucket.seed_questions("polymarket", [])
    local_bucket.seed_fetch(
        "polymarket", make_polymarket_fetch_df(polymarket_rows).to_dict("records")
    )
    polymarket_update_driver(None)

    metaculus = SourceQuestionBank(
        dfq=make_question_df(
            [
                {
                    "id": "metaculus1",
                    "source": "metaculus",
                    "resolved": True,
                    "market_info_resolution_datetime": "2025-01-31T00:00:00Z",
                }
            ]
        ),
        dfr=make_resolution_df(
            [
                {"id": "metaculus1", "date": "2025-01-01", "value": 0.6},
                {"id": "metaculus1", "date": "2025-01-31", "value": 1.0},
            ]
        ),
    )
    fred = SourceQuestionBank(
        dfq=make_question_df([{"id": "fred1", "source": "fred"}]),
        dfr=make_resolution_df(
            [
                {"id": "fred1", "date": "2025-01-01", "value": 100},  # baseline at due date
                {"id": "fred1", "date": "2025-01-08", "value": 110},  # 7-day horizon: rose
                {"id": "fred1", "date": "2025-01-31", "value": 90},  # 30-day horizon: fell
            ]
        ),
    )
    return {
        "polymarket": _polymarket_bank_from_bucket(local_bucket),
        "metaculus": metaculus,
        "fred": fred,
    }


def _question_set(nullified_id: str | None) -> pd.DataFrame:
    rows = [
        {"id": "polymarket1", "source": "polymarket", "resolution_dates": "N/A"},
        {"id": "metaculus1", "source": "metaculus", "resolution_dates": "N/A"},
        {"id": "fred1", "source": "fred", "resolution_dates": ["2025-01-08", "2025-01-31"]},
    ]
    if nullified_id:
        rows.append({"id": nullified_id, "source": "polymarket", "resolution_dates": "N/A"})
    return make_question_set_df(rows)


def _leaderboard_entry(
    org: str, model: str, forecast_fn, resolved: pd.DataFrame, valid_dates: list[str]
) -> pd.DataFrame | None:
    """Run a model's forecasts through the real prepare, merge, impute and get_df_info chain."""
    forecast_df = pd.DataFrame(
        [
            {
                "id": row["id"],
                "source": row["source"],
                "direction": (),
                "forecast": forecast_fn(row["resolved_to"]),
                "resolution_date": pd.Timestamp(row["resolution_date"]).strftime("%Y-%m-%d"),
            }
            for _, row in resolved.iterrows()
        ]
    )
    prepared = check_and_prepare_forecast_file(forecast_df, _DUE_STR, org, valid_dates)
    merged = set_resolution_dates(prepared, resolved)
    imputed = impute_missing_forecasts(merged, org, org, model)
    return get_df_info(
        imputed, {"organization": org, "model": model, "model_organization": org}, _DUE_STR
    )


@pytest.mark.parametrize("nullified", [False, True])
def test_full_pipeline(nullified, local_bucket, freeze_today):
    freeze_today(date(2025, 2, 1))  # yesterday == 2025-01-31
    nullified_id = (
        sorted(PolymarketSource().get_nullified_ids(as_of=_DUE))[0] if nullified else None
    )

    bank = _build_bank(local_bucket, nullified_id)
    if nullified:
        # The nullified question is in the persisted bank, so resolve must drop it.
        assert nullified_id in bank["polymarket"].dfq["id"].astype(str).tolist()

    question_set = _question_set(nullified_id)
    exploded = explode_question_set(question_set, _DUE_STR)
    valid_dates = get_resolution_dates(question_set)
    assert (exploded["source"] == "fred").sum() == 2  # two resolution dates

    resolved, _ = resolve_all(
        exploded,
        question_bank=bank,
        sources={name: SOURCES[name] for name in bank},
        forecast_due_date=_DUE,
    )
    # The nullified question is dropped; the real questions are present in every scenario.
    assert set(resolved["id"]) == {"polymarket1", "metaculus1", "fred1"}
    polymarket = resolved[resolved["id"] == "polymarket1"].iloc[0]
    assert 0 <= polymarket["resolved_to"] <= 1 and not bool(polymarket["resolved"])
    metaculus = resolved[resolved["id"] == "metaculus1"].iloc[0]
    assert bool(metaculus["resolved"]) and metaculus["resolved_to"] == 1.0
    assert set(resolved[resolved["id"] == "fred1"]["resolved_to"]) == {1.0, 0.0}

    partial = pd.DataFrame(
        [
            {
                "id": "metaculus1",
                "source": "metaculus",
                "direction": (),
                "forecast": 0.8,
                "resolution_date": "2025-01-31",
            }
        ]  # metaculus1 only; the rest are imputed
    )
    prepared = check_and_prepare_forecast_file(partial, _DUE_STR, "PartialOrg", valid_dates)
    merged = set_resolution_dates(prepared, resolved)
    imputed = impute_missing_forecasts(merged, "PartialOrg", "PartialOrg", "PartialModel")
    metaculus_row = imputed[imputed["id"] == "metaculus1"].iloc[0]
    assert metaculus_row["forecast"] == 0.8 and bool(metaculus_row["imputed"]) is False
    fred_rows = imputed[imputed["id"] == "fred1"]
    assert (fred_rows["forecast"] == 0.5).all() and fred_rows["imputed"].all()

    entries = [
        _leaderboard_entry(
            BASELINE_ORG_NAIVE_MODEL["organization"],
            BASELINE_ORG_NAIVE_MODEL["model"],
            lambda rt: 0.5,
            resolved,
            valid_dates,
        ),
        # Outcome-dependent forecasts give each question its own Brier score, so merging the fred
        # horizons or misweighting dataset and market questions changes the goldens.
        _leaderboard_entry(
            "OrgGood", "Good", lambda rt: 0.9 if rt == 1 else 0.3, resolved, valid_dates
        ),
        _leaderboard_entry(
            "OrgBad", "Bad", lambda rt: 0.2 if rt == 1 else 0.7, resolved, valid_dates
        ),
    ]
    combined = combine_forecasting_rounds(entries)
    assert "polymarket1" not in set(combined["id"])
    assert set(combined["id"]) == {"metaculus1", "fred1"}

    lb, _ = score_models(
        combined, [peer_score, brier_skill_score], MarketQuestionAdjustment.MARKET_BRIER
    )
    # 1 resolved market + 2 dataset (question, date) rows = 3 scored per model.
    assert (lb["n_overall"] == 3).all()
    ranked = lb.sort_values("peer_score_overall", ascending=False)["model"].tolist()
    assert ranked[0] == "Good"
    assert ranked.index("Good") < ranked.index("Bad")

    # The nullified goldens equal the base ones, so only the id assertion pins the drop; the
    # arithmetic is pure pandas/numpy, so rtol=0.
    scenario = "nullified" if nullified else "base"
    check_golden(
        f"e2e_resolved_{scenario}",
        resolved,
        key=["id", "resolution_date"],
        cols=["id", "source", "resolution_date", "resolved", "resolved_to"],
        rtol=0,
    )
    check_golden(f"e2e_leaderboard_{scenario}", lb, key="model_pk", rtol=0)


def test_full_pipeline_is_deterministic(local_bucket, freeze_today):
    """Re-running scoring on the same resolved data yields the same leaderboard (no hidden RNG)."""
    freeze_today(date(2025, 2, 1))
    bank = _build_bank(local_bucket, None)
    question_set = _question_set(None)
    exploded = explode_question_set(question_set, _DUE_STR)
    valid_dates = get_resolution_dates(question_set)
    resolved, _ = resolve_all(
        exploded,
        question_bank=bank,
        sources={name: SOURCES[name] for name in bank},
        forecast_due_date=_DUE,
    )

    def _leaderboard() -> pd.DataFrame:
        entries = [
            _leaderboard_entry(
                BASELINE_ORG_NAIVE_MODEL["organization"],
                BASELINE_ORG_NAIVE_MODEL["model"],
                lambda rt: 0.5,
                resolved,
                valid_dates,
            ),
            _leaderboard_entry("OrgA", "ModelA", lambda rt: abs(rt - 0.2), resolved, valid_dates),
        ]
        lb, _ = score_models(
            combine_forecasting_rounds(entries),
            [peer_score, brier_skill_score],
            MarketQuestionAdjustment.MARKET_BRIER,
        )
        return lb.set_index("model_pk").sort_index()

    first, second = _leaderboard(), _leaderboard()
    pd.testing.assert_frame_equal(
        first[sorted(first.columns)], second[sorted(second.columns)], check_exact=False, atol=1e-9
    )
