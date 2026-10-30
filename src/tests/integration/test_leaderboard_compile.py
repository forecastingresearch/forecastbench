"""Integration: the leaderboard's intake over processed forecast files in a ``local_bucket``.

``download_and_compile_processed_forecast_files`` drops dates that are too recent or whose Naive
Forecaster file has too few resolved questions, then compiles each eligible file into an entry keyed
by ``question_pk``. ``MIN_NUM_DATASET_QUESTIONS`` is patched small so a tiny fixture suffices.
"""

from datetime import date
from unittest.mock import patch

from helpers import data_utils, env
from leaderboard import main as lb
from tests.factories import make_processed_forecast_set

_KEEP = "2025-01-01"  # old enough; Naive has enough resolved questions
_SPARSE = "2025-02-01"  # old enough, but Naive has too few resolved questions
_NEW = "2025-05-20"  # newer than the min-days cutoff

_NAIVE = lb.BASELINE_ORG_NAIVE_MODEL


def _resolved_mix() -> list[dict]:
    """2 resolved dataset rows (distinct horizons) + 1 resolved market row."""
    return [
        {"id": "fred1", "source": "fred", "resolved": True, "resolution_date": "2025-01-08"},
        {"id": "fred2", "source": "fred", "resolved": True, "resolution_date": "2025-01-31"},
        {
            "id": "metaculus1",
            "source": "metaculus",
            "resolved": True,
            "resolution_date": "2025-01-15",
        },
    ]


def _unresolved_mix() -> list[dict]:
    """Same questions, none resolved, so the date fails the min-resolved threshold."""
    return [{**row, "resolved": False} for row in _resolved_mix()]


def _seed_model(
    local_bucket, due: str, org: str, model: str, forecasts: list[dict], *, eligible: bool = True
) -> None:
    payload = make_processed_forecast_set(
        forecasts,
        organization=org,
        model=model,
        model_organization=org,
        forecast_due_date=due,
        leaderboard_eligible=eligible,
    )
    fname = f"{due}.{org}.{model.lower().replace(' ', '-')}.json"
    local_bucket.seed_processed_forecast_set(due, fname, payload)


def _seed_naive(local_bucket, due: str, forecasts: list[dict]) -> None:
    payload = make_processed_forecast_set(
        forecasts,
        organization=_NAIVE["organization"],
        model=_NAIVE["model"],
        model_organization=_NAIVE["model_organization"],
        forecast_due_date=due,
        leaderboard_eligible=True,
    )
    # The min-resolved filter looks for exactly this Naive filename per date.
    fname = data_utils.get_forecast_filename(due, _NAIVE["model"])
    local_bucket.seed_processed_forecast_set(due, fname, payload)


def test_compile_filters_dates_eligibility_and_min_resolved(local_bucket, freeze_today):
    freeze_today(date(2025, 6, 1))  # with min_days=50 the cutoff is 2025-04-12

    _seed_naive(local_bucket, _KEEP, _resolved_mix())
    _seed_model(local_bucket, _KEEP, "OrgA", "ModelA", _resolved_mix())
    _seed_model(local_bucket, _KEEP, "OrgB", "ModelB", _resolved_mix(), eligible=False)
    _seed_naive(local_bucket, _SPARSE, _unresolved_mix())
    _seed_model(local_bucket, _SPARSE, "OrgA", "ModelA", _resolved_mix())
    _seed_naive(local_bucket, _NEW, _resolved_mix())
    _seed_model(local_bucket, _NEW, "OrgA", "ModelA", _resolved_mix())

    with patch.object(lb, "MIN_NUM_DATASET_QUESTIONS", 2):
        entries, valid_dates = lb.download_and_compile_processed_forecast_files(
            bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
            min_days=50,
            min_num_market_questions=1,
        )

    assert valid_dates == [_KEEP]

    org_models = {(e["organization"].iloc[0], e["model"].iloc[0]) for e in entries}
    assert org_models == {(_NAIVE["organization"], _NAIVE["model"]), ("OrgA", "ModelA")}
    assert ("OrgB", "ModelB") not in org_models

    for e in entries:
        assert "question_pk" in e.columns
        assert (e["question_pk"].astype(str) != "").all()

    all_pks = set()
    for e in entries:
        all_pks |= set(e["question_pk"].astype(str))
    assert f"{_KEEP}_fred_fred1_7" in all_pks  # dataset: resolution_date − due (2025-01-08) = 7d
    assert f"{_KEEP}_metaculus_metaculus1" in all_pks  # market: no horizon component


def test_dataset_threshold_is_enforced_independently_of_the_market_threshold(
    local_bucket, freeze_today
):
    # One resolved dataset question of the two required fails the date despite the market threshold.
    freeze_today(date(2025, 6, 1))
    one_dataset_one_market = [
        {"id": "fred1", "source": "fred", "resolved": True, "resolution_date": "2025-01-08"},
        {
            "id": "metaculus1",
            "source": "metaculus",
            "resolved": True,
            "resolution_date": "2025-01-15",
        },
    ]
    _seed_naive(local_bucket, _KEEP, one_dataset_one_market)
    _seed_model(local_bucket, _KEEP, "OrgA", "ModelA", one_dataset_one_market)

    with patch.object(lb, "MIN_NUM_DATASET_QUESTIONS", 2):
        entries, valid_dates = lb.download_and_compile_processed_forecast_files(
            bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
            min_days=50,
            min_num_market_questions=1,
        )

    assert valid_dates == []
    assert entries == []


def test_market_threshold_is_enforced_independently_of_the_dataset_threshold(
    local_bucket, freeze_today
):
    # Two resolved dataset questions meet the dataset threshold, so only the market one can fail.
    freeze_today(date(2025, 6, 1))
    two_datasets_no_market = [
        {"id": "fred1", "source": "fred", "resolved": True, "resolution_date": "2025-01-08"},
        {"id": "fred2", "source": "fred", "resolved": True, "resolution_date": "2025-01-31"},
        {
            "id": "metaculus1",
            "source": "metaculus",
            "resolved": False,
            "resolution_date": "2025-01-15",
        },
    ]
    _seed_naive(local_bucket, _KEEP, two_datasets_no_market)
    _seed_model(local_bucket, _KEEP, "OrgA", "ModelA", two_datasets_no_market)

    with patch.object(lb, "MIN_NUM_DATASET_QUESTIONS", 2):
        entries, valid_dates = lb.download_and_compile_processed_forecast_files(
            bucket=env.PROCESSED_FORECAST_SETS_BUCKET,
            min_days=50,
            min_num_market_questions=1,
        )

    assert valid_dates == []
    assert entries == []
