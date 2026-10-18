"""Publish question set entry point."""

import logging
from typing import Any

from helpers import dates, decorator, question_curation
from orchestration import _io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def driver(_: Any) -> None:
    """Publish the LLM question set to the dataset repo if today is the forecast due date."""
    if not question_curation.is_today_question_set_publication_date():
        logger.info("Today is NOT the question set publication date.")
        return

    _io.publish_question_set(dates.get_date_today_as_iso(), local_dir="/tmp")
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
