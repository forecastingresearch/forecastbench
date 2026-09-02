"""DBnomics update entry point."""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.dbnomics import MAX_OBSERVATION_AGE_DAYS, DbnomicsSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "dbnomics"


def _alert_stale_series(warnings: list[str]) -> None:
    """Notify operators about excluded series without risking the saved update."""
    if not warnings:
        return
    message = (
        f":warning: DBnomics: {len(warnings)} series excluded from new question sets. "
        f"No fetched data or no non-missing observation within {MAX_OBSERVATION_AGE_DAYS} days.\n"
        + "\n".join(warnings[:20])
    )
    if len(warnings) > 20:
        message += f"\n...and {len(warnings) - 20} more; see the update job logs for all IDs."
    try:
        slack = import_module("helpers.slack")
        slack.send_message(message=message)
    except Exception:
        logger.exception("Failed to send the DBnomics freshness Slack alert.")


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update DBnomics questions and resolution files."""
    source = DbnomicsSource()

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    result = source.update(dfq, dff)

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    _alert_stale_series(source.freshness_warnings)
    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
