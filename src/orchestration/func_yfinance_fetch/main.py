"""Yfinance fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import data_utils, decorator, slack
from orchestration import _source_io
from sources.yfinance import YfinanceSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "yfinance"


def _alert_uncurated_delisted(tickers: list[str]) -> None:
    """Slack alert for pooled tickers that 404'd but aren't curated.

    They are either newly delisted or newly renamed and need prompt triage into
    nullified_questions or ticker_renames (a rename left uncurated resolves old questions to stale
    prices).
    """
    message = (
        f":warning: yfinance fetch: {len(tickers)} ticker(s) failed to fetch and are no longer in "
        f"the S&P 500 (likely delisted or renamed): {', '.join(tickers)}. "
        "If delisted, add to nullified_questions; if renamed, add to ticker_renames "
        "(mapping to the replacement symbol)."
    )
    logger.warning(message)
    slack.send_message(message=message)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Yahoo Finance stock data and upload to question bank."""
    source = YfinanceSource()

    dfq = data_utils.get_data_from_cloud_storage(SOURCE, return_question_data=True)

    dff = source.fetch(dfq=dfq)

    _source_io.write_fetch_output(SOURCE, dff)

    if source.uncurated_delisted_tickers:
        _alert_uncurated_delisted(source.uncurated_delisted_tickers)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
