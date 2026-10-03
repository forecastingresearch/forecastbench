"""DataFrame and API-response builders for ForecastBench tests.

Plain functions (not fixtures) that construct small, realistic test inputs. Import them
directly, e.g. ``from tests.factories import make_question_df``. Builders take a list of
row dicts (or **overrides) and fill sensible defaults so a test only specifies the fields
it cares about.
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd


def make_forecast_df(rows: list[dict]) -> pd.DataFrame:
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


def make_question_df(rows: list[dict]) -> pd.DataFrame:
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


def make_resolution_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame with [id, date, value] matching ResolutionFrame."""
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def make_acled_resolution_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching AcledResolutionFrame.

    Args:
        rows: list of dicts with 'country', 'event_date', and event type columns.
    """
    df = pd.DataFrame(rows)
    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def make_question_set_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame with [id, source, resolution_dates] for explode_question_set."""
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# ACLED-specific factories
# ---------------------------------------------------------------------------


def make_acled_api_auth_response(**overrides: object) -> dict:
    """Build a realistic ACLED OAuth token response dict."""
    base = {
        "token_type": "Bearer",
        "expires_in": 86400,
        "access_token": "test-token-abc123",
        "refresh_token": "test-refresh-xyz",
    }
    base.update(overrides)
    return base


def make_acled_api_data_response(
    data: list[dict], count: int | None = None, next_cursor: str | None = None, **overrides: object
) -> dict:
    """Build a realistic ACLED data API page response dict.

    `next_cursor` is None on the last page, as in the API's cursor-based pagination.
    """
    base = {
        "status": 200,
        "success": True,
        "count": count if count is not None else len(data),
        "data": data,
        "filename": "results.json",
        "next_cursor": next_cursor,
    }
    base.update(overrides)
    return base


def make_acled_event(**overrides: object) -> dict:
    """Build a single ACLED event dict as returned by the API."""
    base = {
        "event_id_cnty": "TST001",
        "event_date": "2025-06-15",
        "iso": 706,
        "region": "Eastern Africa",
        "country": "Somalia",
        "admin1": "Banadir",
        "event_type": "Battles",
        "fatalities": 1,
        "timestamp": "1750000000",
    }
    base.update(overrides)
    return base


def make_acled_fetch_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching AcledFetchFrame from partial row dicts."""
    defaults = make_acled_event()
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ---------------------------------------------------------------------------
# Wikipedia-specific factories
# ---------------------------------------------------------------------------


def make_wikipedia_fetch_result(**pages: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build a ``WikipediaFetchResult`` with every configured page present.

    ``WikipediaSource.update()`` requires an entry per page in ``_PAGES`` and skips the pages whose
    frame is empty, so pages not named in ``pages`` get an empty frame.

    Args:
        **pages: ``id_root`` -> raw table frame (the page's id/value columns plus ``date``).
    """
    from sources.wikipedia import _PAGES

    return {p["id_root"]: pages.get(p["id_root"], pd.DataFrame()) for p in _PAGES}


# ---------------------------------------------------------------------------
# Forecast-set factories (raw input to func_resolve; processed input to leaderboard)
# ---------------------------------------------------------------------------

_DEFAULT_FORECAST_DUE_DATE = "2025-01-01"


def make_raw_forecast_set(forecasts: list[dict], **overrides: object) -> dict:
    """Build a raw forecast-set dict as a forecaster uploads it to ``FORECAST_SETS_BUCKET``.

    ``func_resolve`` reads ``organization``/``model``/``model_organization``/``question_set`` and the
    ``forecasts`` list, then resolves them. Each forecast row needs at least ``id``, ``source``,
    ``forecast``, ``resolution_date``; ``direction`` is optional (defaults to ``()`` downstream).

    Args:
        forecasts (list): Forecast row dicts.
        **overrides: Top-level field overrides (e.g. ``leaderboard_eligible=False``). Pass
            ``forecast_due_date`` to set the default ``question_set`` filename.
    """
    due = overrides.pop("forecast_due_date", _DEFAULT_FORECAST_DUE_DATE)
    base = {
        "organization": "OrgA",
        "model": "ModelA",
        "model_organization": "OrgA",
        "question_set": f"{due}-llm.json",
        "leaderboard_eligible": True,
        "forecasts": forecasts,
    }
    base.update(overrides)
    return base


def make_processed_forecast_set(forecasts: list[dict], **overrides: object) -> dict:
    """Build a processed forecast-set dict as ``func_resolve`` writes to the processed bucket.

    Each forecast row is filled with the resolution fields the leaderboard compile path reads
    (``resolved``/``resolved_to``/``imputed``/``resolution_date``); the top level carries
    ``forecast_due_date``. Pass per-row dicts to override (e.g. ``resolved=False`` for an open
    market, ``imputed=True`` for an imputed row).

    Args:
        forecasts (list): Forecast row dicts (id, source, plus any field overrides).
        **overrides: Top-level field overrides (``organization``, ``model``,
            ``leaderboard_eligible``, ``forecast_due_date``, ...).
    """
    due = overrides.pop("forecast_due_date", _DEFAULT_FORECAST_DUE_DATE)
    rows = []
    for row in forecasts:
        merged = {
            "direction": None,
            "forecast": 0.5,
            "resolution_date": due,
            "resolved": True,
            "resolved_to": 1.0,
            "imputed": False,
            "market_value_on_due_date": None,
            "market_value_on_due_date_minus_one": None,
        }
        merged.update(row)
        rows.append(merged)
    base = {
        "organization": "OrgA",
        "model": "ModelA",
        "model_organization": "OrgA",
        "question_set": f"{due}-llm.json",
        "forecast_due_date": due,
        "leaderboard_eligible": True,
        "forecasts": rows,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Leaderboard-scoring factory (real-data-shaped, for the 2FE / bootstrap path)
# ---------------------------------------------------------------------------

# Mean Brier per model is driven by a base error; a per-question wiggle gives the two-way
# fixed-effects regression enough variation to be identifiable (not rank-deficient). "Always 0.5"
# forecasts exactly 0.5 (Brier 0.25) — the rescale anchor; ``None`` marks that special case.
_LEADERBOARD_MODELS = {
    "Good Model": 0.1,  # skilled  → low Brier  → ranks best
    "Bad Model": 0.7,  # poor     → high Brier → ranks worst
    "Naive Forecaster": 0.5,  # baseline (FE + brier-skill reference)
    "Imputed Forecaster": 0.5,  # baseline (market question fixed effect under MARKET_BRIER)
    "Always 0.5": None,  # baseline (difficulty rescale anchor)
}


def make_leaderboard_entries(
    *, n_dataset: int = 225, n_market: int = 50, forecast_due_date: str = "2024-01-01"
) -> pd.DataFrame:
    """Build a real-data-shaped combined leaderboard frame for 2FE / bootstrap scoring.

    Produces the ForecastBench baselines (Naive Forecaster, Imputed Forecaster, Always 0.5) plus a
    skilled and a poor model, each forecasting every dataset + market question, with per-question
    difficulty so ``two_way_fixed_effects`` is identifiable. 2FE is degenerate on tiny input, so the
    defaults approximate the production shape (``MIN_NUM_DATASET_QUESTIONS`` dataset questions).

    Args:
        n_dataset (int): Number of resolved dataset questions (fred).
        n_market (int): Number of resolved market questions (metaculus).
        forecast_due_date (str): The single forecast due date stamped on every row.

    Returns:
        pd.DataFrame: One row per (model, question), with the columns ``score_models`` consumes.
    """
    org = "ForecastBench"
    questions = [("fred", f"dataset_{i}", 7, i) for i in range(n_dataset)] + [
        ("metaculus", f"market_{i}", None, i) for i in range(n_market)
    ]
    rows = []
    for model, base_error in _LEADERBOARD_MODELS.items():
        model_pk = f"{org}_{org}_{model}"
        for source, qid, horizon, j in questions:
            truth = float(j % 2)
            if base_error is None:
                forecast = 0.5
            else:
                err = min(base_error + (j % 7) * 0.01, 0.95)
                forecast = (1.0 - err) if truth == 1.0 else err
            if horizon is None:
                question_pk = f"{forecast_due_date}_{source}_{qid}"
            else:
                question_pk = f"{forecast_due_date}_{source}_{qid}_{horizon}"
            rows.append(
                {
                    "organization": org,
                    "model": model,
                    "model_organization": org,
                    "model_pk": model_pk,
                    "source": source,
                    "id": qid,
                    "forecast_due_date": forecast_due_date,
                    "resolution_date": forecast_due_date,
                    "horizon": np.nan if horizon is None else horizon,
                    "question_pk": question_pk,
                    "forecast": forecast,
                    "resolved_to": truth,
                    "resolved": True,
                    "imputed": False,
                    "model_age_at_due_date": 0,
                    "external_submission": False,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Yfinance-specific factories
# ---------------------------------------------------------------------------


def make_yfinance_fetch_df(rows: list[dict]) -> pd.DataFrame:
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
        "latest_close_date": "2026-03-17",
        "company_name": "Some Co",
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ---------------------------------------------------------------------------
# SerpAPI-specific factories
# ---------------------------------------------------------------------------


def make_serpapi_fetch_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching SerpapiFetchFrame: one dated measurement per row.

    Each row should have at least 'id'. Missing columns get defaults (a finite value measured on
    the requested date).
    """
    defaults = {
        "date": "2026-03-18",
        "value": 10.0,
        "fetch_datetime": "2026-03-18T00:05:00+00:00",
        "requested_date": "2026-03-18",
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


class FakeJevClient:
    """Stand-in for the Jev (``typesafe_sdk``) language classifier used by ``tag_questions``.

    ``answers`` maps a question's text to the choice it should get (``"english"`` /
    ``"not_english"``) or to an exception to raise for it; unlisted questions get ``default``.
    Records every classified question text in ``asked``.
    """

    def __init__(self, answers: dict | None = None, default: str = "english") -> None:
        self.answers = answers or {}
        self.default = default
        self.asked: list[str] = []

    async def __aenter__(self) -> "FakeJevClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    async def system_one(self, state: dict, questions: dict) -> SimpleNamespace:
        self.asked.append(state["question"])
        outcome = self.answers.get(state["question"], self.default)
        if isinstance(outcome, Exception):
            raise outcome
        answer = SimpleNamespace(choice=outcome, probabilities={"english": 0.9, "not_english": 0.1})
        return SimpleNamespace(choices={"language": answer})


# ---------------------------------------------------------------------------
# Manifold-specific factories
# ---------------------------------------------------------------------------


def make_manifold_api_market(**overrides: object) -> dict:
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


def make_manifold_bet(**overrides: object) -> dict:
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


# ---------------------------------------------------------------------------
# Metaculus-specific factories
# ---------------------------------------------------------------------------


def make_metaculus_market(**overrides: object) -> dict:
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


def make_metaculus_search_result(**overrides: object) -> dict:
    """Build a single Metaculus search result entry (lighter than full market)."""
    base = {
        "id": 42472,
        "nr_forecasters": 50,
        "question": {
            "cp_reveal_time": "2025-01-01T00:00:00Z",
            "scheduled_close_time": "2026-06-01T00:00:00Z",
            "scheduled_resolve_time": "2026-06-10T00:00:00Z",
        },
    }
    question_overrides = overrides.pop("question", None)
    base.update(overrides)
    if question_overrides:
        base["question"].update(question_overrides)
    return base


def make_metaculus_fetch_df(ids: list) -> pd.DataFrame:
    """Build a DataFrame matching MetaculusFetchFrame schema."""
    return pd.DataFrame({"id": [str(i) for i in ids]})


# ---------------------------------------------------------------------------
# Polymarket-specific factories
# ---------------------------------------------------------------------------


def make_polymarket_api_market(**overrides: object) -> dict:
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


def make_polymarket_price_history(entries: list[tuple]) -> list[dict]:
    """Build a price history list as returned by the CLOB API.

    Args:
        entries: list of (epoch_sec, prob) tuples.
    """
    return [{"t": t, "p": p} for t, p in entries]


def make_polymarket_fetch_df(rows: list[dict]) -> pd.DataFrame:
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


# ---------------------------------------------------------------------------
# Kalshi-specific factories
# ---------------------------------------------------------------------------


def make_kalshi_api_market(**overrides: object) -> dict:
    """Build a realistic Kalshi market dict as returned by /markets/{ticker}."""
    base = {
        "ticker": "KXTEST-001",
        "event_ticker": "KXTEST",
        "title": "Will X happen by 2026?",
        "yes_sub_title": "X happens",
        "market_type": "binary",
        "open_time": "2025-06-01T00:00:00Z",
        "occurrence_datetime": "2026-12-01T00:00:00Z",
        "close_time": "2026-12-01T00:00:00Z",
        "status": "active",
        "result": "",
        "volume_fp": "10000.00",
        "volume_24h_fp": "500.00",
        "open_interest_fp": "2000.00",
        "rules_primary": "Resolves Yes if X happens.",
        "rules_secondary": "",
        "settlement_ts": None,
        "expected_expiration_time": "2026-12-01T00:00:00Z",
        "latest_expiration_time": "2026-12-08T00:00:00Z",
    }
    base.update(overrides)
    return base


def make_kalshi_event(
    markets: list[dict] | None = None, category: str = "Economics", **overrides: object
) -> dict:
    """Build a Kalshi event dict with nested markets as returned by /events."""
    base = {
        "event_ticker": "KXTEST",
        "series_ticker": "KXTEST",
        "category": category,
        "title": "Test event",
        "settlement_sources": [],
        "markets": markets if markets is not None else [make_kalshi_api_market()],
    }
    base.update(overrides)
    return base


def make_kalshi_candlestick(
    end_period_ts: int, close_dollars: float | str | None = None, **overrides: object
) -> dict:
    """Build a candlestick dict as returned by the candlesticks endpoint.

    ``price`` is empty ({}) when no trade occurred during the period, matching the API.
    """
    price = {} if close_dollars is None else {"close_dollars": str(close_dollars)}
    base = {
        "end_period_ts": end_period_ts,
        "price": price,
        "volume_fp": "100.00",
        "open_interest_fp": "500.00",
    }
    base.update(overrides)
    return base


def make_kalshi_fetch_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching KalshiFetchFrame schema."""
    defaults = dict(
        event_ticker="KXTEST",
        needs_yes_label=False,
        series_ticker="KXTEST",
        settlement_sources=[],
    )
    records = [{**defaults, **row} for row in rows]
    return pd.DataFrame(records, columns=["id", *defaults])


# ---------------------------------------------------------------------------
# DBnomics-specific factories
# ---------------------------------------------------------------------------


def make_dbnomics_api_response(
    period_values: list[tuple],
    provider_code: str = "meteofrance",
    dataset_name: str = "Temperature",
    series_name: str = "Abbeville",
) -> dict:
    """Build a DBnomics ``/series`` API response dict.

    Args:
        period_values (list): List of (period_str, value) tuples; value is a float or "NA".
        provider_code (str): Provider code returned on the series document.
        dataset_name (str): Dataset name on the series doc.
        series_name (str): Series name on the series doc.
    """
    periods = [p for p, _ in period_values]
    values = [v for _, v in period_values]
    return {
        "series": {
            "docs": [
                {
                    "period": periods,
                    "value": values,
                    "provider_code": provider_code,
                    "dataset_name": dataset_name,
                    "series_name": series_name,
                }
            ]
        },
    }


def make_dbnomics_fetch_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching DbnomicsFetchFrame (one row per observation).

    Each row should have at least 'id', 'period', 'value'. Missing columns get defaults.
    """
    defaults = {
        "provider_name": "MeteoFrance",
        "dataset_name": "Temperature",
        "series_name": "Abbeville",
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


# ---------------------------------------------------------------------------
# FRED-specific factories
# ---------------------------------------------------------------------------


def make_fred_api_release(**overrides: object) -> dict:
    """Build a realistic FRED release API response dict."""
    base = {
        "id": 18,
        "realtime_start": "2026-03-18",
        "realtime_end": "2026-03-18",
        "name": "H.15 Selected Interest Rates",
        "press_release": True,
        "link": "http://www.federalreserve.gov/releases/h15/",
        "notes": "For questions on the data, please contact the data source.",
    }
    base.update(overrides)
    return base


def make_fred_api_series(**overrides: object) -> dict:
    """Build a realistic FRED series API response dict."""
    base = {
        "id": "DGS10",
        "title": "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
        "observation_start": "1962-01-02",
        "observation_end": "2026-03-16",
        "frequency": "Daily",
        "frequency_short": "D",
        "units": "Percent",
        "units_short": "%",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "seasonal_adjustment_short": "NSA",
        "last_updated": "2026-03-17 15:16:44-05",
        "popularity": 98,
        "notes": "H.15 Statistical Release.",
    }
    base.update(overrides)
    return base


def make_fred_api_observations(
    series_id: str = "DGS10", date_values: list[tuple] | None = None
) -> list[dict]:
    """Build a list of FRED observation dicts.

    Args:
        series_id (str): The series ID for the id field.
        date_values (list): List of (date_str, value_str) tuples.
    """
    if date_values is None:
        date_values = [
            ("2026-03-14", "4.25"),
            ("2026-03-15", "."),
            ("2026-03-16", "4.30"),
        ]
    return [
        {
            "realtime_start": "2026-03-18",
            "realtime_end": "2026-03-18",
            "date": d,
            "value": v,
        }
        for d, v in date_values
    ]


def make_fred_fetch_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame matching FredFetchFrame schema.

    Each row should have at least 'id'. Missing columns get defaults.
    """
    defaults = {
        "question": (
            "Will X have increased by {resolution_date} as compared to "
            "its value on {forecast_due_date}?"
        ),
        "background": "N/A",
        "url": "https://fred.stlouisfed.org/series/DGS10",
        "resolved": False,
        "forecast_horizons": [7, 30, 90, 180, 365],
        "freeze_datetime_value": 4.30,
        "freeze_datetime_value_explanation": "The latest value released in X from the release Y.",
        "market_info_resolution_criteria": "N/A",
        "market_info_open_datetime": "N/A",
        "market_info_close_datetime": "N/A",
        "market_info_resolution_datetime": "N/A",
        "fetch_datetime": "2026-03-18T00:00:00+00:00",
        "probability": 4.30,
        "resolutions": [
            {"id": "DGS10", "date": "2026-03-14", "value": 4.25},
            {"id": "DGS10", "date": "2026-03-15", "value": 4.25},
            {"id": "DGS10", "date": "2026-03-16", "value": 4.30},
        ],
    }
    df = pd.DataFrame(rows)
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = [default] * len(df) if isinstance(default, list) else default
    return df
