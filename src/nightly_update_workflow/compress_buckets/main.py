"""Compress files in bucket to speed downloads in Cloud Run Jobs."""

import logging
import os
import subprocess
import sys
import tarfile
from typing import Any

import gcsfs
from termcolor import colored

sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from helpers import decorator, env  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@decorator.log_runtime
def compress_bucket_and_upload_tarball(bucket: str, compression: str = "gz") -> None:
    """Compress files in bucket and upload to same bucket as `<bucket>.tar.gz`.

    Args:
        bucket (str): Name of the GCP Storage bucket to process.

    Returns:
        None
    """
    assert compression in ["gz", "xz"]
    ext = f".tar.{compression}"
    fs = gcsfs.GCSFileSystem(project=env.PROJECT_ID)
    # Recursively list all files
    objects = []
    for prefix, _, files in fs.walk(bucket):
        for filename in files:
            if filename and not filename.endswith(ext):
                objects.append(f"{prefix}/{filename}")

    # Spawn gsutil to upload from stdin
    filename = f"{bucket}{ext}"
    upload = subprocess.Popen(
        ["gsutil", "cp", "-", f"gs://{bucket}/{filename}"],
        stdin=subprocess.PIPE,
    )

    # Open a streaming tarfile with gzip compression
    with tarfile.open(fileobj=upload.stdin, mode=f"w:{compression}") as tarball:
        for blob in objects:
            info = tarfile.TarInfo(name=blob)
            info.size = fs.info(blob)["size"]
            with fs.open(blob, "rb") as f:
                tarball.addfile(info, fileobj=f)

    upload.stdin.close()
    upload.wait()
    logger.info(f"Created {filename}.")


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
        env.QUESTION_SETS_BUCKET,
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
