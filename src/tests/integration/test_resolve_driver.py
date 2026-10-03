"""Integration: the ``func_resolve`` driver() — raw forecast set → processed forecast set.

Runs the real ``orchestration/func_resolve/main.py:driver()`` against a ``local_bucket``: it lists
the raw forecast files, downloads + resolves the question set, imputes missing forecasts, and writes
the processed file the leaderboard later consumes. This is the round-trip the resolution e2e skips by
hand-building frames — here the *driver* orchestrates it.

Only the GCP/git-coupled seams are patched; everything between is the real code path:
- ``_io.load_question_bank`` — raises on a GCS freshness check in prod → return a built bank.
- ``_io.load_hash_mapping`` — reads ``<source>/hash_mapping.json`` from GCS (and leaves a stale
  ``/tmp`` copy on a miss) → return ``""`` so the run is hermetic.
- ``slack.send_message`` — posts market-resolution warnings → mock.
"""

import contextlib
from datetime import date
from unittest.mock import patch

from _fb_types import SourceQuestionBank
from orchestration.func_resolve import main as func_resolve
from tests.factories import make_question_df, make_raw_forecast_set, make_resolution_df

_DUE = "2025-01-01"
_FILENAME = f"{_DUE}.OrgA.modela.json"


def _question_bank() -> dict:
    """metaculus (resolved market → 1.0) + fred (dataset: rose → 1.0, fell → 0.0)."""
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
                {
                    "id": "metaculus1",
                    "date": "2025-01-31",
                    "value": 1.0,
                },  # final value 1.0 → resolved_to 1
            ]
        ),
    )
    fred = SourceQuestionBank(
        dfq=make_question_df([{"id": "fred1", "source": "fred"}]),
        dfr=make_resolution_df(
            [
                {"id": "fred1", "date": "2025-01-01", "value": 100},  # baseline at due date
                {"id": "fred1", "date": "2025-01-08", "value": 110},  # +7 rose  → 1.0
                {"id": "fred1", "date": "2025-01-31", "value": 90},  # +30 fell → 0.0
            ]
        ),
    )
    return {"metaculus": metaculus, "fred": fred}


# The published (GitHub dataset-repo) question sets the resolver downloads for this due date.
# main's resolve reads these via ``_io.read_question_set_json(run_locally=False)`` — a GitHub
# fetch — so ``_run_driver`` patches that seam to serve them offline.
_LLM_QSET = {
    "questions": [
        {"id": "metaculus1", "source": "metaculus", "resolution_dates": "N/A"},
        {"id": "fred1", "source": "fred", "resolution_dates": ["2025-01-08", "2025-01-31"]},
    ]
}
_HUMAN_QSET = {
    "questions": [{"id": "metaculus1", "source": "metaculus", "resolution_dates": "N/A"}]
}


def _run_driver(
    local_bucket,
    monkeypatch,
    freeze_today,
    forecasts: list[dict],
    *,
    eligible: bool = True,
    organization: str = "OrgA",
    model: str = "ModelA",
    model_organization: str = "OrgA",
) -> dict:
    """Seed inputs, patch the GCP/git seams, run the real driver, return the processed file."""
    raw = make_raw_forecast_set(
        forecasts,
        organization=organization,
        model=model,
        model_organization=model_organization,
        question_set=f"{_DUE}-llm.json",
        leaderboard_eligible=eligible,
    )
    local_bucket.seed_forecast_set(_DUE, _FILENAME, raw)

    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
    freeze_today(date(2025, 2, 1))  # cutoff = today − 10 ≥ due, so the date is resolved

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch.object(func_resolve._io, "load_question_bank", return_value=_question_bank())
        )
        stack.enter_context(patch.object(func_resolve._io, "load_hash_mapping", return_value=""))
        stack.enter_context(patch.object(func_resolve.slack, "send_message"))
        # main's resolve downloads the published question set from GitHub; serve it offline.
        stack.enter_context(
            patch.object(
                func_resolve._io,
                "read_question_set_json",
                side_effect=lambda filename, run_locally=False: (
                    _LLM_QSET if "llm" in filename else _HUMAN_QSET
                ),
            )
        )
        func_resolve.driver(None)

    return local_bucket.read_processed_forecast_set(_DUE, _FILENAME)


def _full_forecasts() -> list[dict]:
    return [
        {
            "id": "metaculus1",
            "source": "metaculus",
            "forecast": 0.9,
            "resolution_date": "2025-01-31",
        },
        {"id": "fred1", "source": "fred", "forecast": 0.7, "resolution_date": "2025-01-08"},
        {"id": "fred1", "source": "fred", "forecast": 0.3, "resolution_date": "2025-01-31"},
    ]


def test_resolves_and_writes_processed_file(local_bucket, monkeypatch, freeze_today):
    processed = _run_driver(local_bucket, monkeypatch, freeze_today, _full_forecasts())

    # The processed-file schema the leaderboard later reads.
    assert set(processed) >= {
        "organization",
        "model",
        "model_organization",
        "forecast_due_date",
        "question_set",
        "leaderboard_eligible",
        "forecasts",
    }
    assert processed["organization"] == "OrgA"
    assert processed["forecast_due_date"] == _DUE

    rows = processed["forecasts"]
    # Resolved market: binary outcome, marked resolved; the forecaster's value is preserved.
    (metaculus,) = [r for r in rows if r["id"] == "metaculus1"]
    assert metaculus["resolved"] is True and metaculus["resolved_to"] == 1.0
    assert metaculus["imputed"] is False and metaculus["forecast"] == 0.9
    # Dataset resolves to 1.0 (value rose) and 0.0 (value fell) at the two horizons.
    fred = {r["resolution_date"][:10]: r for r in rows if r["id"] == "fred1"}
    assert fred["2025-01-08"]["resolved_to"] == 1.0
    assert fred["2025-01-31"]["resolved_to"] == 0.0
    assert all(r["imputed"] is False for r in fred.values())

    # The same run publishes the public resolution set (real serializer, real bucket write).
    published = local_bucket.read_json(
        "PUBLIC_RELEASE_BUCKET", f"datasets/resolution_sets/{_DUE}_resolution_set.json"
    )
    assert published["forecast_due_date"] == _DUE
    assert {r["id"] for r in published["resolutions"]} == {"metaculus1", "fred1"}


def test_imputes_missing_forecast(local_bucket, monkeypatch, freeze_today):
    # Forecaster omits fred1 @ +30 → that row is filled (0.5) and flagged imputed; the rest are kept.
    partial = [
        {
            "id": "metaculus1",
            "source": "metaculus",
            "forecast": 0.9,
            "resolution_date": "2025-01-31",
        },
        {"id": "fred1", "source": "fred", "forecast": 0.7, "resolution_date": "2025-01-08"},
    ]
    processed = _run_driver(local_bucket, monkeypatch, freeze_today, partial)

    fred = {r["resolution_date"][:10]: r for r in processed["forecasts"] if r["id"] == "fred1"}
    assert fred["2025-01-08"]["imputed"] is False and fred["2025-01-08"]["forecast"] == 0.7
    assert fred["2025-01-31"]["imputed"] is True and fred["2025-01-31"]["forecast"] == 0.5


def test_imputed_forecaster_fills_market_value_not_default(local_bucket, monkeypatch, freeze_today):
    # The Imputed Forecaster (a ForecastBench dummy) leaves market questions unforecast; its missing
    # MARKET forecast must be filled with market_value_on_due_date carried from resolution, NOT the
    # 0.5 default. This is the only test that proves the column survives the real chain
    # MarketSource._resolve -> set_resolution_dates -> impute_missing_forecasts.
    partial = [
        {"id": "fred1", "source": "fred", "forecast": 0.7, "resolution_date": "2025-01-08"},
    ]  # metaculus1 and fred1@+30 omitted → both imputed
    processed = _run_driver(
        local_bucket,
        monkeypatch,
        freeze_today,
        partial,
        organization="ForecastBench",
        model="Imputed Forecaster",
        model_organization="ForecastBench",
    )
    (metaculus,) = [r for r in processed["forecasts"] if r["id"] == "metaculus1"]
    assert metaculus["imputed"] is True
    # market_value_on_due_date == the resolution value at the due date (2025-01-01 → 0.6), not 0.5.
    assert metaculus["forecast"] == 0.6
    # A dataset gap, by contrast, imputes to the 0.5 default even for this benchmark model.
    fred = {r["resolution_date"][:10]: r for r in processed["forecasts"] if r["id"] == "fred1"}
    assert fred["2025-01-31"]["imputed"] is True and fred["2025-01-31"]["forecast"] == 0.5


def test_leaderboard_eligible_false_is_preserved(local_bucket, monkeypatch, freeze_today):
    # Eligibility is consumed downstream (by the leaderboard), not here: the file still resolves.
    processed = _run_driver(
        local_bucket, monkeypatch, freeze_today, _full_forecasts(), eligible=False
    )
    assert processed["leaderboard_eligible"] is False
    assert {r["id"] for r in processed["forecasts"]} == {"metaculus1", "fred1"}
