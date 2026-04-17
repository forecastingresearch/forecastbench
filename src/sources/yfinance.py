"""Yahoo Finance question source."""

from __future__ import annotations

from datetime import date
from typing import ClassVar

from _fb_types import NullifiedQuestion, SourceType

from ._dataset import DatasetSource

# Stocks that were delisted (via acquisition, merger, or going private) while still in the question
# pool. nullification_start_date is the first calendar day after the last trading session so that
# question sets whose forecast_due_date falls on or after this date are nullified, while earlier
# sets continue to resolve to the final close price.
DELISTED_STOCKS = [
    NullifiedQuestion(id="MRO", nullification_start_date=date(2024, 11, 22)),
    NullifiedQuestion(id="CTLT", nullification_start_date=date(2024, 12, 18)),
    NullifiedQuestion(id="DFS", nullification_start_date=date(2025, 5, 19)),
    NullifiedQuestion(id="JNPR", nullification_start_date=date(2025, 7, 2)),
    NullifiedQuestion(id="ANSS", nullification_start_date=date(2025, 7, 17)),
    NullifiedQuestion(id="HES", nullification_start_date=date(2025, 7, 18)),
    NullifiedQuestion(id="PARA", nullification_start_date=date(2025, 8, 7)),
    NullifiedQuestion(id="WBA", nullification_start_date=date(2025, 8, 28)),
    NullifiedQuestion(id="K", nullification_start_date=date(2025, 12, 11)),
    NullifiedQuestion(id="DAY", nullification_start_date=date(2026, 2, 4)),
]

# Tickers that were renamed on yfinance while still in the question pool. yfinance serves all price
# history under the replacement ticker; the original ticker returns no data. The update_questions
# code fetches data under the replacement ticker and writes it to the original ticker's resolution
# file so that existing questions resolve correctly.
TICKER_RENAMES = [
    {"original_ticker": "FI", "replacement_ticker": "FISV"},
    {"original_ticker": "MMC", "replacement_ticker": "MRSH"},
]


class YfinanceSource(DatasetSource):
    """Yahoo Finance financial data source."""

    name: ClassVar[str] = "yfinance"
    display_name: ClassVar[str] = "Yahoo Finance"
    source_type: ClassVar[SourceType] = SourceType.DATASET
    nullified_questions: ClassVar[list[NullifiedQuestion]] = DELISTED_STOCKS
