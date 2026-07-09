"""Compress files in bucket to speed downloads in Cloud Run Jobs."""

import logging
import os
import shutil
import subprocess
import sys
from typing import Any

from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import decorator, env  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def compress_bucket_and_upload_tarball(bucket: str, compression: str = "gz") -> None:
    """Compress files in bucket and upload to same bucket as `<bucket>.tar.gz`.

    Download the bucket to local disk in parallel, create a tarball from the
    local copy, and upload it. A parallel download reads the objects far faster
    than streaming them one-by-one through a Cloud Storage FUSE mount, where
    every file access is a separate network round-trip.

    Args:
        bucket (str): Name of the GCP Storage bucket to process.
        compression (str): Compression type, either "gz" or "xz".

    Returns:
        None
    """
    assert compression in ["gz", "xz"]
    ext = f".tar.{compression}"
    local_dir = f"/tmp/{bucket}"
    local_tarball = f"/tmp/{bucket}{ext}"
    shutil.rmtree(local_dir, ignore_errors=True)
    os.makedirs(local_dir)

    logger.info(f"Downloading gs://{bucket} to {local_dir}.")
    subprocess.run(
        [
            "gsutil",
            "-m",
            "rsync",
            "-r",
            "-x",
            r".*\.(gz|xz)$",
            f"gs://{bucket}",
            local_dir,
        ],
        check=True,
    )

    logger.info(f"Creating tarball {local_tarball} from {local_dir}.")
    subprocess.run(
        [
            "tar",
            f"{'czf' if compression == 'gz' else 'cJf'}",
            local_tarball,
            "--exclude=*.gz",
            "--exclude=*.xz",
            "-C",
            "/tmp",
            bucket,
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
