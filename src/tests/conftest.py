"""Shared fixtures and DataFrame factories for ForecastBench tests."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from sources.acled import AcledSource
from sources.fred import FredSource
from sources.metaculus import MetaculusSource

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
    if "resolution_date" in df.columns:
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
