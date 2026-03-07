"""Environment variables."""

import os

# ---------------------------------------------------------------------------
# GCP / infrastructure
# ---------------------------------------------------------------------------

PROJECT_ID = os.environ.get("CLOUD_PROJECT")
CLOUD_DEPLOY_REGION = os.environ.get("CLOUD_DEPLOY_REGION")
NUM_CPUS = int(os.environ.get("NUM_CPUS", 1))
RUNNING_LOCALLY = bool(int(os.environ.get("RUNNING_LOCALLY", 0)))
BUCKET_MOUNT_POINT = os.environ.get("BUCKET_MOUNT_POINT", "")

# ---------------------------------------------------------------------------
# GCS buckets
# ---------------------------------------------------------------------------

QUESTION_BANK_BUCKET = os.environ.get("QUESTION_BANK_BUCKET")
QUESTION_SETS_BUCKET = os.environ.get("QUESTION_SETS_BUCKET")
FORECAST_SETS_BUCKET = os.environ.get("FORECAST_SETS_BUCKET")
PROCESSED_FORECAST_SETS_BUCKET = os.environ.get("PROCESSED_FORECAST_SETS_BUCKET")
PUBLIC_RELEASE_BUCKET = os.environ.get("PUBLIC_RELEASE_BUCKET")
