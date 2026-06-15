"""ACLED fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import decorator, keys
from orchestration import _source_io
from sources.acled import AcledSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "acled"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch ACLED data and store in GCP Cloud Storage."""
    source = AcledSource()
    source.api_email = keys.API_EMAIL_ACLED
    source.api_password = keys.API_PASSWORD_ACLED

    dff = source.fetch()

    if dff.empty:
        logger.error("No ACLED data was downloaded.")
        return

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
