"""Kalshi update entry point."""

import logging
from importlib import import_module
from typing import Any

from helpers import data_utils, decorator
from orchestration import _source_io
from sources.kalshi import KalshiSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE = "kalshi"


def _alert_invalid_resolution_windows(actions: list[str]) -> None:
    """Alert on isolated invalid markets without risking the update job."""
    if not actions:
        return
    message = (
        f":warning: Kalshi update found {len(actions)} market(s) with invalid resolution windows: "
        f"{', '.join(actions)}.\n"
        "Review any quarantined existing question and resolution file manually."
    )
    try:
        slack = import_module("helpers.slack")
        slack.send_message(message=message)
    except Exception:
        logger.exception("Failed to send the Kalshi invalid-resolution-window Slack alert.")


@decorator.log_runtime
def driver(_: Any) -> None:
    """Update Kalshi questions and resolution files."""
    source = KalshiSource()

    dfq, dff = data_utils.get_data_from_cloud_storage(
        SOURCE, return_question_data=True, return_fetch_data=True
    )

    logger.info("Loading existing resolution files...")
    nullified_ids = source.get_nullified_ids()
    unresolved_mask = ~dfq["resolved"] & ~dfq["id"].astype(str).isin(nullified_ids)
    unresolved_ids = dfq.loc[unresolved_mask, "id"].astype(str).tolist()
    existing_resolution_files = _source_io.load_existing_resolution_files(
        SOURCE, ids=unresolved_ids
    )
    logger.info(f"Loaded {len(existing_resolution_files)} resolution files")

    existing_resolution_ids = _source_io.list_existing_resolution_ids(SOURCE)

    result = source.update(
        dfq,
        dff,
        existing_resolution_files=existing_resolution_files,
        existing_resolution_ids=existing_resolution_ids,
    )

    logger.info("Uploading to GCP...")
    data_utils.upload_questions(result.dfq, SOURCE)
    if result.resolution_files:
        _source_io.upload_resolution_files(SOURCE, result.resolution_files)
    _alert_invalid_resolution_windows(source.invalid_resolution_window_actions)

    logger.info("Done.")


if __name__ == "__main__":
    driver(None)
