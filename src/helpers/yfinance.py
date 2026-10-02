"""Yfinance-specific variables. Delegates to sources._metadata and YfinanceSource."""

import pandas as pd

from sources._metadata import SOURCE_METADATA

SOURCE_INTRO = SOURCE_METADATA["yfinance"]["source_intro"]
RESOLUTION_CRITERIA = SOURCE_METADATA["yfinance"]["resolution_criteria"]

# Lazy import to avoid circular imports at module level
_source = None


def _get_source():
    global _source
    if _source is None:
        from sources.yfinance import YfinanceSource

        _source = YfinanceSource()
    return _source


def is_pair_id(question_id: object) -> bool:
    """Tell whether a yfinance question id names a pair of tickers (``X_Y``)."""
    return _get_source()._is_pair_id(question_id)


def pair_ratio_series(dfr: pd.DataFrame, pair_id: str) -> pd.DataFrame:
    """Return the price-ratio series a pair question resolves on, as [id, date, value] rows."""
    return _get_source()._pair_ratio_series(dfr, pair_id)
