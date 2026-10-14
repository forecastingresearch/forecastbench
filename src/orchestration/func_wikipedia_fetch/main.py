"""Wikipedia fetch entry point."""

import logging
from typing import Any

from helpers import decorator, slack
from orchestration import _source_io
from sources.wikipedia import WikipediaSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "wikipedia"


def _alert_failed_pages(failures: list[str]) -> None:
    """Slack alert for pages whose fetch failed.

    The pages that succeeded were written, so their questions update and resolve as usual. A failed
    page keeps its previous fetch file, so its questions update from that until the next
    successful fetch.

    Args:
        failures (list[str]): One "<id_root>: <reason>" line per failed page.
    """
    message = (
        f":warning: Wikipedia fetch: {len(failures)} page(s) failed and were left at their "
        "previous fetch:\n" + "\n".join(failures)
    )
    logger.warning(message)
    slack.send_message(message=message)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch Wikipedia data and store in GCP Cloud Storage."""
    source = WikipediaSource()

    fetch_result = source.fetch()

    _source_io.write_wikipedia_fetch_output(fetch_result)

    if source.failed_pages:
        _alert_failed_pages(source.failed_pages)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
