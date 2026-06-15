"""Shared fixtures and DataFrame factories for ForecastBench tests."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from sources.acled import AcledSource
from sources.fred import FredSource
from sources.infer import InferSource
from sources.manifold import ManifoldSource
from sources.metaculus import MetaculusSource
from sources.polymarket import PolymarketSource
from sources.yfinance import YfinanceSource

# ---------------------------------------------------------------------------
# Time-freezing fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def freeze_today():
    """Patch dates.get_date_today() and get_date_yesterday() to return deterministic values.

    Usage:
        def test_something(freeze_today):
            freeze_today(date(2025, 1, 15))
            # Now dates.get_date_today() returns date(2025, 1, 15)
            # and dates.get_date_yesterday() returns date(2025, 1, 14)
    """
    patches = []

    def _freeze(target_date):
        target_datetime = datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
        )
        p1 = patch("helpers.dates.get_date_today", return_value=target_date)
        p2 = patch("helpers.dates.get_date_yesterday", return_value=target_date - timedelta(days=1))
        p3 = patch("helpers.dates.get_datetime_today", return_value=target_datetime)
        patches.extend([p1, p2, p3])
        for p in [p1, p2, p3]:
            p.start()

    yield _freeze

    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Source instance fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def market_source():
    """Return a concrete MarketSource instance."""
    return MetaculusSource()


@pytest.fixture()
def dataset_source():
    """Return a concrete DatasetSource instance."""
    return FredSource()


@pytest.fixture()
def acled_source():
    """Return an AcledSource instance."""
    return AcledSource()


@pytest.fixture()
def infer_source():
    """Return an InferSource instance with a fake API key."""
    src = InferSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def manifold_source():
    """Return a ManifoldSource instance."""
    return ManifoldSource()


@pytest.fixture()
def metaculus_source():
    """Return a MetaculusSource instance with a fake API key."""
    src = MetaculusSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def polymarket_source():
    """Return a PolymarketSource instance."""
    return PolymarketSource()


@pytest.fixture()
def yfinance_source():
    """Return a YfinanceSource instance."""
    return YfinanceSource()


# ---------------------------------------------------------------------------
# DataFrame factories
# ---------------------------------------------------------------------------


def make_forecast_df(rows):
    """Build a DataFrame for resolution input.

    Each row is a dict with keys from:
    [id, source, direction, forecast_due_date, resolution_date].
    """
    df = pd.DataFrame(rows)
    if "direction" not in df.columns:
        df["direction"] = [() for _ in range(len(df))]
    if "forecast_due_date" in df.columns:
        df["forecast_due_date"] = pd.to_datetime(df["forecast_due_date"])
    # Default needed so error-path tests pass ExplodedQuestionSetFrame validation in resolve_all()
    if "resolution_date" not in df.columns:
        df["resolution_date"] = pd.to_datetime("2025-12-31")
    else:
        df["resolution_date"] = pd.to_datetime(df["resolution_date"])
    # resolve_all() sets these before calling _resolve()
    if "resolved" not in df.columns:
        df["resolved"] = False
    if "resolved_to" not in df.columns:
        df["resolved_to"] = np.nan
    if "market_value_on_due_date" not in df.columns:
        df["market_value_on_due_date"] = np.nan
    return df


def make_question_df(rows):
    """Build a DataFrame matching QuestionFrame schema.

    Each row should have at least 'id'. Missing columns get defaults.
    """
    defaults = {
        "question": "N/A",
        "background": "N/A",
        "url": "N/A",
        "resolved": False,
        "forecast_horizons": "N/A",
        "freeze_datetime_value": "N/A",
        "freeze_datetime_value_explanation": "N/A",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "market_info_resolution_datetime": "N/A",
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def make_resolution_df(rows):
    """Build a DataFrame with [id, date, value] matching ResolutionFrame."""
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def make_acled_resolution_df(rows, event_columns=None):
    """Build a DataFrame matching AcledResolutionFrame.

    Args:
        rows: list of dicts with 'country', 'event_date', and event type columns.
        event_columns: list of event type column names (e.g. ['Battles', 'Riots']).
    """
    df = pd.DataFrame(rows)
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def make_question_set_df(rows):
    """Build a DataFrame with [id, source, resolution_dates] for explode_question_set."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# INFER-specific factories
# ---------------------------------------------------------------------------


def make_infer_api_question(**overrides):
    """Build a realistic INFER API question dict. Override specific fields as needed."""
    base = {
        "id": 9999,
        "name": "Will X happen by end of 2026?",
        "description": "<p>Background text.</p>",
        "clarifications": [],
        "state": "active",
        "type": "Forecast::YesNoQuestion",
        "active?": True,
        "binary?": False,
        "resolved?": False,
        "resolved_at": None,
        "ends_at": "2026-06-01T04:00:00.000Z",
        "starts_at": "2026-01-01T20:00:00.000Z",
        "scoring_start_time": "2026-01-01T15:00:00.000-05:00",
        "scoring_end_time": "2026-06-01T00:00:00.000-05:00",
        "created_at": "2026-01-01T18:00:00.000Z",
        "closed_at": None,
        "voided_at": None,
        "answers": [
            {
                "id": 9001,
                "name": "Yes",
                "probability": 0.65,
                "display_probability": "65%",
                "predictions_count": 50,
                "answer_name": "Yes",
            },
            {
                "id": 9002,
                "name": "No",
                "probability": 0.35,
                "display_probability": "35%",
                "predictions_count": 50,
                "answer_name": "No",
            },
        ],
    }
    base.update(overrides)
    return base


def make_infer_prediction_set(created_at, yes_prob):
    """Build a realistic INFER prediction set dict."""
    return {
        "id": 999999,
        "type": "Forecast::OpinionPoolPredictionSet",
        "question_id": 9999,
        "created_at": created_at,
        "predictions": [
            {
                "answer_name": "Yes",
                "final_probability": yes_prob,
                "forecasted_probability": yes_prob,
                "starting_probability": yes_prob,
            },
            {
                "answer_name": "No",
                "final_probability": round(1 - yes_prob, 4),
                "forecasted_probability": round(1 - yes_prob, 4),
                "starting_probability": round(1 - yes_prob, 4),
            },
        ],
    }


def make_infer_fetch_df(rows):
    """Build a DataFrame matching InferFetchFrame schema."""
    defaults = {
        "question": "N/A",
        "background": "N/A",
        "url": "N/A",
        "resolved": False,
        "forecast_horizons": "N/A",
        "freeze_datetime_value": "N/A",
        "freeze_datetime_value_explanation": "N/A",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "market_info_resolution_datetime": "N/A",
        "fetch_datetime": "2026-01-15T00:00:00+00:00",
        "probability": 0.5,
        "nullify_question": False,
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ---------------------------------------------------------------------------
# Yfinance-specific factories
# ---------------------------------------------------------------------------


def make_yfinance_fetch_df(rows):
    """Build a DataFrame matching YfinanceFetchFrame schema.

    Each row should have at least 'id'. Missing columns get defaults.
    """
    defaults = {
        "question": "Will {id} go up?",
        "background": "N/A",
        "url": "N/A",
        "resolved": False,
        "forecast_horizons": "N/A",
        "freeze_datetime_value": "100.0",
        "freeze_datetime_value_explanation": "N/A",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "market_info_resolution_datetime": "N/A",
        "fetch_datetime": "2026-03-18T00:00:00+00:00",
        "probability": 100.0,
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ---------------------------------------------------------------------------
# Manifold-specific factories
# ---------------------------------------------------------------------------


def make_manifold_api_market(**overrides):
    """Build a realistic Manifold market dict as returned by /market/{id}."""
    base = {
        "id": "mkt_001",
        "question": "Will X happen by 2026?",
        "textDescription": "Background text.",
        "createdTime": 1704067200000,  # 2024-01-01 epoch ms
        "closeTime": 1735689600000,  # 2025-01-01 epoch ms
        "isResolved": False,
        "resolution": None,
        "resolutionTime": None,
        "resolutionProbability": None,
        "url": "https://manifold.markets/user/test-market",
        "uniqueBettorCount": 20,
        "totalLiquidity": 200,
    }
    base.update(overrides)
    return base


def make_manifold_search_result(**overrides):
    """Build a search result item from /search-markets (subset of market fields)."""
    base = {
        "id": "mkt_001",
        "uniqueBettorCount": 20,
        "totalLiquidity": 200,
        "closeTime": 1735689600000,  # 2025-01-01 epoch ms
    }
    base.update(overrides)
    return base


def make_manifold_bet(**overrides):
    """Build a single bet dict as returned by /bets endpoint."""
    base = {
        "id": "bet_001",
        "contractId": "mkt_001",
        "createdTime": 1717200000000,  # ~2024-06-01 epoch ms
        "probAfter": 0.6,
        "probBefore": 0.5,
        "isFilled": True,
        "amount": 10,
    }
    base.update(overrides)
    return base


def make_manifold_fetch_df(rows):
    """Build a DataFrame matching ManifoldFetchFrame schema (just id column)."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Metaculus-specific factories
# ---------------------------------------------------------------------------


def make_metaculus_market(**overrides):
    """Build a realistic Metaculus per-question API response dict.

    Simulates GET /api/posts/{id}/ response. Supports nested overrides for the
    ``question`` sub-dict via the ``question`` keyword argument.
    """
    base = {
        "id": 42472,
        "title": "Will X happen by 2027?",
        "resolved": False,
        "nr_forecasters": 50,
        "status": "open",
        "question": {
            "description": "Background text for the question.",
            "resolution_criteria": "Resolves Yes if X happens.",
            "open_time": "2026-01-01T00:00:00Z",
            "actual_close_time": "2027-01-01T00:00:00Z",
            "actual_resolve_time": None,
            "scheduled_close_time": "2027-01-01T00:00:00Z",
            "scheduled_resolve_time": "2027-01-02T00:00:00Z",
            "cp_reveal_time": "2026-01-03T00:00:00Z",
            "resolution": None,
            "type": "binary",
            "aggregations": {
                "recency_weighted": {
                    "history": [
                        {
                            "start_time": 1735689600.0,  # 2025-01-01 00:00 UTC
                            "end_time": 1735776000.0,  # 2025-01-02 00:00 UTC
                            "centers": [0.4],
                            "forecaster_count": 10,
                        },
                        {
                            "start_time": 1735776000.0,  # 2025-01-02 00:00 UTC
                            "end_time": 1735862400.0,  # 2025-01-03 00:00 UTC
                            "centers": [0.5],
                            "forecaster_count": 20,
                        },
                        {
                            "start_time": 1735862400.0,  # 2025-01-03 00:00 UTC
                            "end_time": 1735948800.0,  # 2025-01-04 00:00 UTC
                            "centers": [0.6],
                            "forecaster_count": 30,
                        },
                    ],
                }
            },
        },
    }
    question_overrides = overrides.pop("question", None)
    base.update(overrides)
    if question_overrides:
        base["question"].update(question_overrides)
    return base


def make_metaculus_search_result(**overrides):
    """Build a single Metaculus search result entry (lighter than full market)."""
    base = {
        "id": 42472,
        "nr_forecasters": 50,
        "question": {
            "cp_reveal_time": "2025-01-01T00:00:00Z",
        },
    }
    question_overrides = overrides.pop("question", None)
    base.update(overrides)
    if question_overrides:
        base["question"].update(question_overrides)
    return base


def make_metaculus_fetch_df(ids):
    """Build a DataFrame matching MetaculusFetchFrame schema."""
    return pd.DataFrame({"id": [str(i) for i in ids]})


# ---------------------------------------------------------------------------
# Polymarket-specific factories
# ---------------------------------------------------------------------------


def make_polymarket_api_market(**overrides):
    """Build a realistic Polymarket Gamma API market dict.

    Override specific fields as needed. All JSON-encoded string fields
    (outcomes, outcomePrices, clobTokenIds) match the real API format.
    """
    base = {
        "conditionId": "0xabc123",
        "question": "Will X happen by 2026?",
        "description": "Background text.",
        "slug": "will-x-happen-by-2026",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.65", "0.35"]',
        "clobTokenIds": '["token_yes", "token_no"]',
        "liquidityNum": 50000,
        "active": True,
        "closed": False,
        "archived": False,
        "startDateIso": "2025-01-01",
        "endDate": "2026-06-01T00:00:00Z",
        "umaResolutionStatus": None,
        "umaEndDate": None,
        "events": [{"endDate": "2026-06-01T00:00:00Z"}],
    }
    base.update(overrides)
    return base


def make_polymarket_price_history(entries):
    """Build a price history list as returned by the CLOB API.

    Args:
        entries: list of (epoch_sec, prob) tuples.
    """
    return [{"t": t, "p": p} for t, p in entries]


def make_polymarket_fetch_df(rows):
    """Build a DataFrame matching PolymarketFetchFrame schema."""
    defaults = {
        "question": "N/A",
        "background": "N/A",
        "url": "N/A",
        "resolved": False,
        "forecast_horizons": "N/A",
        "freeze_datetime_value": "N/A",
        "freeze_datetime_value_explanation": "N/A",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "market_info_resolution_datetime": "N/A",
        "fetch_datetime": "2026-01-15T00:00:00+00:00",
        "probability": 0.5,
        "historical_prices": [{"date": "2024-06-01", "value": 0.5}],
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df
