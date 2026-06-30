"""DBnomics fetch entry point."""

from __future__ import annotations

import logging
from typing import Any

from helpers import decorator
from orchestration import _source_io
from sources.dbnomics import DbnomicsSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "dbnomics"


@decorator.log_runtime
def driver(_: Any) -> None:
    """Fetch DBnomics data and upload to question bank."""
    source = DbnomicsSource()

    dff = source.fetch()

    _source_io.write_fetch_output(SOURCE, dff)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
