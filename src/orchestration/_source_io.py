"""Shared IO helpers for source fetch/update orchestration."""

from __future__ import annotations

import json
import logging
import os
from typing import Iterable

import pandas as pd

from helpers import constants, data_utils, env
from utils import gcp

logger = logging.getLogger(__name__)


def write_fetch_output(source: str, dff: pd.DataFrame) -> None:
    """Write fetch DataFrame to <source>_fetch.jsonl and upload.

    Args:
        source (str): Source name (e.g. "infer").
        dff (pd.DataFrame): Fetched data to write.
    """
    filenames = data_utils.generate_filenames(source)
    local = filenames["local_fetch"]
    with open(local, "w", encoding="utf-8") as f:
        for record in dff.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    logger.info(f"Uploading {filenames['jsonl_fetch']} to GCP...")
    gcp.storage.upload(
        bucket_name=env.QUESTION_BANK_BUCKET,
        local_filename=local,
    )


def load_existing_resolution_files(
    source: str,
    ids: Iterable[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Download <source>/<id>.jsonl resolution files.

    If ids is given, download only those. If ids is None, list the bucket and
    download every .jsonl under <source>/ — use sparingly, scales with backlog.

    Args:
        source (str): Source name (e.g. "infer").
        ids (Iterable[str] | None): Specific question IDs to load. If None,
            load every resolution file present in the bucket for this source.

    Returns:
        dict mapping question_id to its resolution DataFrame.
    """
    if ids is None:
        paths = gcp.storage.list_with_prefix(
            bucket_name=env.QUESTION_BANK_BUCKET, prefix=f"{source}/"
        )
        question_ids = [
            os.path.basename(p).removesuffix(".jsonl") for p in paths if p.endswith(".jsonl")
        ]
    else:
        question_ids = [str(qid) for qid in ids]

    result: dict[str, pd.DataFrame] = {}
    for question_id in question_ids:
        basename = f"{question_id}.jsonl"
        remote_path = f"{source}/{basename}"
        local_filename = f"/tmp/{source}_{basename}"

        gcp.storage.download_no_error_message_on_404(
            bucket_name=env.QUESTION_BANK_BUCKET,
            filename=remote_path,
            local_filename=local_filename,
        )
        if os.path.exists(local_filename):
            df = pd.read_json(
                local_filename,
                lines=True,
                dtype=constants.RESOLUTION_FILE_COLUMN_DTYPE,
                convert_dates=False,
            )
            if not df.empty:
                result[question_id] = df
    logger.info(f"Loaded {len(result)} existing resolution files for {source}.")
    return result


def upload_resolution_files(source: str, resolution_files: dict[str, pd.DataFrame]) -> None:
    """Upload per-question resolution files to <source>/<id>.jsonl.

    Args:
        source (str): Source name (e.g. "infer").
        resolution_files (dict): Mapping of question_id to resolution DataFrame.
    """
    for question_id, df in resolution_files.items():
        basename = f"{question_id}.jsonl"
        remote_filename = f"{source}/{basename}"
        local_filename = f"/tmp/{basename}"

        df[["id", "date", "value"]].to_json(
            local_filename, orient="records", lines=True, date_format="iso"
        )
        gcp.storage.upload(
            bucket_name=env.QUESTION_BANK_BUCKET,
            local_filename=local_filename,
            filename=remote_filename,
        )
    logger.info(f"Uploaded {len(resolution_files)} resolution files for {source}.")
