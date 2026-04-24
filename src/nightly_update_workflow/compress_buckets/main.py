"""Compress files in bucket to speed downloads in Cloud Run Jobs."""

import logging
import os
import subprocess
import sys
from typing import Any

from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import decorator, env  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bucket_mount_dir(bucket: str) -> str:
    """Return the mounted filesystem path for a bucket available to this job."""
    mount_root = env.BUCKET_MOUNT_POINT or "/mnt"
    return f"{mount_root}/{bucket}"


@decorator.log_runtime
def compress_bucket_and_upload_tarball(bucket: str, compression: str = "gz") -> None:
    """Compress files in bucket and upload to same bucket as `<bucket>.tar.gz`.

    Create a tarball from the mounted bucket path and upload it.

    Args:
        bucket (str): Name of the GCP Storage bucket to process.
        compression (str): Compression type, either "gz" or "xz".

    Returns:
        None
    """
    assert compression in ["gz", "xz"]
    ext = f".tar.{compression}"
    local_tarball = f"/tmp/{bucket}{ext}"
    source_dir = get_bucket_mount_dir(bucket=bucket)
    source_parent, source_leaf = os.path.split(source_dir)

    logger.info(f"Creating tarball {local_tarball} from mounted path {source_dir}.")
    subprocess.run(
        [
            "tar",
            f"{'czf' if compression == 'gz' else 'cJf'}",
            local_tarball,
            "--exclude=*.gz",
            "--exclude=*.xz",
            "-C",
            source_parent,
            source_leaf,
        ],
        check=True,
    )

    remote_path = f"gs://{bucket}/{bucket}{ext}"
    content_type = "application/gzip" if compression == "gz" else "application/x-xz"
    logger.info(f"Uploading {local_tarball} to {remote_path}.")
    subprocess.run(
        ["gsutil", "-h", f"Content-Type:{content_type}", "cp", local_tarball, remote_path],
        check=True,
    )
    logger.info(f"Created {bucket}{ext}.")


@decorator.log_runtime
def driver(_: Any) -> None:
    """Compress the provided buckets.

    Args:
        _ (Any): Unused placeholder argument for GCP Cloud Run Job.

    Returns:
        None: Exits the process on completion.
    """
    BUCKET_TO_COMPRESS = os.environ.get("BUCKET_TO_COMPRESS")

    if not BUCKET_TO_COMPRESS:
        raise ValueError("You must set the `BUCKET_TO_COMPRESS` environment variable.")

    if BUCKET_TO_COMPRESS not in [
        env.QUESTION_BANK_BUCKET,
        env.FORECAST_SETS_BUCKET,
        env.PROCESSED_FORECAST_SETS_BUCKET,
    ]:
        raise ValueError(f"{BUCKET_TO_COMPRESS} is not a valid bucket to compress.")

    logger.info(f"Compressing {BUCKET_TO_COMPRESS}.")
    compress_bucket_and_upload_tarball(bucket=BUCKET_TO_COMPRESS)

    logger.info(colored("Done.", "red"))


if __name__ == "__main__":
    driver(None)
