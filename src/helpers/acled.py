"""ACLED shared helpers.

Light home of ACLED's pure computation (aggregations + naive-forecast helpers): the module
imports only numpy/pandas and the lightweight metadata layer at load time. Hash-mapping access
routes through a lazily-instantiated ``AcledSource`` (see ``_get_source``), so importing this
module stays light — only *calling* the hash funcs pulls the (now heavy) ``sources.acled``.

The sole caller of those hash funcs is the unrefactored ``base_eval`` naive forecaster, which
therefore declares ``backoff`` in its requirements. ``question_curation`` imports this module only
for the ``SOURCE_INTRO``/``RESOLUTION_CRITERIA`` constants (served from ``_metadata``); it never
triggers the lazy import, so it and its many consumers stay light.

When ``base_eval`` is refactored to call ``AcledSource.get_naive_forecast()`` this computation can
move onto the source class (Phase 1 plan) and this module shrinks to a metadata-only shim.
"""

from datetime import timedelta

import numpy as np
import pandas as pd

from sources._metadata import SOURCE_METADATA

SOURCE_INTRO = SOURCE_METADATA["acled"]["source_intro"]
RESOLUTION_CRITERIA = SOURCE_METADATA["acled"]["resolution_criteria"]

source = "acled"

# Lazy import to avoid circular imports at module level
_source = None


def _get_source():
    global _source
    if _source is None:
        from sources.acled import AcledSource

        _source = AcledSource()
    return _source


def id_hash(d: dict) -> str:
    """Encode ACLED Ids."""
    return _get_source()._id_hash(d)


def id_unhash(hash_key: str) -> tuple:
    """Decode ACLED Ids."""
    return _get_source()._id_unhash(hash_key)


def populate_hash_mapping():
    """Download and load hash mapping into source singleton."""
    from orchestration._io import load_hash_mapping

    _get_source().populate_hash_mapping(load_hash_mapping(source))


def upload_hash_mapping():
    """Dump and upload hash mapping from source singleton."""
    from orchestration._io import upload_hash_mapping as _upload

    raw_json = _get_source().dump_hash_mapping()
    if raw_json:
        _upload(raw_json, source)


def get_forecast(comparison_value, dfr, country, col, ref_date):
    """Retrun the LHS of the comparison for the question.

    Used for the naive forecaster.
    """
    dfr["country"] = country
    dfr[col] = dfr["yhat"]
    dfr["event_date"] = dfr["ds"]
    start_date = ref_date - timedelta(days=30)
    dfr = dfr[
        (dfr["event_date"].dt.date >= start_date) & (dfr["event_date"].dt.date < ref_date)
    ].reset_index(drop=True)
    simulated_values = []
    dates = [pd.to_datetime(ref_date) - timedelta(days=i) for i in range(len(dfr))]
    for _ in range(1000):
        draws = np.random.normal(dfr[col], (dfr["yhat_upper"] - dfr["yhat_lower"]) / (2 * 1.28))
        df_draws = pd.DataFrame(
            {
                "country": country,
                "event_date": dates,
                col: draws,
            }
        )
        simulated_values.append(
            sum_over_past_30_days(
                dfr=df_draws,
                country=country,
                col=col,
                ref_date=ref_date,
            )
        )

    return float(np.mean([value > comparison_value for value in simulated_values]))


def get_base_comparison_value(key, dfr, country, col, ref_date):
    """Get the base comparison value given the question type.

    Used for the naive forecaster and resolve.
    """
    if key == "last30Days.gt.30DayAvgOverPast360Days":
        return thirty_day_avg_over_past_360_days(
            dfr=dfr, country=country, col=col, ref_date=ref_date
        )
    elif key == "last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1":
        return 10 * thirty_day_avg_over_past_360_days_plus_1(
            dfr=dfr, country=country, col=col, ref_date=ref_date
        )
    raise ValueError("Invalid key.")


def sum_over_past_30_days(dfr, country, col, ref_date):
    """Sum over the 30 days before the ref_date."""
    dfc = dfr[dfr["country"] == country].copy()
    if dfc.empty:
        return 0

    start_date = ref_date - timedelta(days=30)
    dfc = dfc[(dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date < ref_date)]
    return dfc[col].sum() if not dfc.empty else 0


def thirty_day_avg_over_past_360_days(dfr, country, col, ref_date):
    """Get the 30 day average over the 360 days before the ref_date."""
    dfc = dfr[dfr["country"] == country].copy()
    if dfc.empty:
        return 0

    start_date = ref_date - timedelta(days=360)
    dfc = dfc[(dfc["event_date"].dt.date >= start_date) & (dfc["event_date"].dt.date < ref_date)]
    return dfc[col].sum() / 12 if not dfc.empty else 0


def thirty_day_avg_over_past_360_days_plus_1(dfr, country, col, ref_date):
    """Get 1 plus the 30 day average over the 360 days before the ref_date."""
    return 1 + thirty_day_avg_over_past_360_days(dfr, country, col, ref_date)
