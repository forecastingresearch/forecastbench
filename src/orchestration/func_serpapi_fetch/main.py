"""SerpAPI fetch entry point."""

import logging
from importlib import import_module
from typing import Any

import requests

from helpers import decorator, keys
from orchestration import _source_io
from sources.serpapi import SerpapiSource
from sources.serpapi_helpers import is_finite_number

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "serpapi"
MONTHLY_USAGE_WARNING_THRESHOLD = 10_000


def warn_on_monthly_usage(api_key: str) -> None:
    """Send a non-fatal Slack warning when monthly SerpAPI usage exceeds the threshold."""
    try:
        response = requests.get(
            "https://serpapi.com/account.json", params={"api_key": api_key}, timeout=(15, 30)
        )
        response.raise_for_status()
        account = response.json()
        consumed = account["this_month_usage"]
        allowance = account["searches_per_month"]
        if not all(is_finite_number(value) and value >= 0 for value in (consumed, allowance)):
            raise ValueError("Invalid SerpAPI usage values.")
        if consumed <= MONTHLY_USAGE_WARNING_THRESHOLD:
            return
        renewal = account.get("plan_renewal_date") or "Unavailable"
        message = (
            ":warning: SerpAPI monthly usage warning\n"
            f"Monthly allowance: {allowance:,} credits\n"
            f"Consumed this billing cycle: {consumed:,} credits\n"
            f"Subscription renewal: {renewal}"
        )
        logger.warning(message)
        import_module("helpers.slack").send_message(message=message)
    except Exception as exc:
        # Request exception text can contain the API key in the URL.
        logger.warning(f"SerpAPI monthly usage notification failed ({type(exc).__name__}).")


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch SerpAPI measurements and upload the dated observations."""
    source = SerpapiSource()
    source.api_key = keys.API_KEY_SERPAPI
    try:
        fetched = source.fetch()
    finally:
        # Check even when exhausted credits cause every search request to fail.
        warn_on_monthly_usage(source.api_key)
    _source_io.write_fetch_output(SOURCE, fetched)
    # Only attempted IDs appear here; recovered historical observations also count as successes.
    empty_categories = [
        f"{name} ({measurements['id'].nunique()} attempted IDs)"
        for name, measurements in fetched.groupby(fetched["id"].str.split("__", n=1).str[0])
        if not measurements["value"].map(is_finite_number).any()
    ]
    if empty_categories:
        message = ":warning: SerpAPI: no valid measurements for " + ", ".join(empty_categories)
        logger.warning(message)
        try:
            import_module("helpers.slack").send_message(message=message)
        except Exception:
            logger.exception("Failed to send the SerpAPI category warning to Slack.")
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
