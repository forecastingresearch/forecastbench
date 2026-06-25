"""Environment variables.

Values are read from ``os.environ`` lazily on each attribute access (PEP 562 module
``__getattr__``) rather than snapshotted at import.
"""

import os

# Names read as plain strings (``None`` if unset).
_STR_VARS = {
    "PROJECT_ID": "CLOUD_PROJECT",
    "CLOUD_DEPLOY_REGION": "CLOUD_DEPLOY_REGION",
    "QUESTION_BANK_BUCKET": "QUESTION_BANK_BUCKET",
    "QUESTION_SETS_BUCKET": "QUESTION_SETS_BUCKET",
    "FORECAST_SETS_BUCKET": "FORECAST_SETS_BUCKET",
    "FORECAST_SETS_TRANSCRIPTS_BUCKET": "FORECAST_SETS_TRANSCRIPTS_BUCKET",
    "PROCESSED_FORECAST_SETS_BUCKET": "PROCESSED_FORECAST_SETS_BUCKET",
    "PUBLIC_RELEASE_BUCKET": "PUBLIC_RELEASE_BUCKET",
    "WEBSITE_BUCKET": "WEBSITE_BUCKET",
    "WEBSITE_STAGING_ASSETS_BUCKET": "WEBSITE_STAGING_ASSETS_BUCKET",
    "LLM_BASELINE_DOCKER_IMAGE_NAME": "LLM_BASELINE_DOCKER_IMAGE_NAME",
    "LLM_BASELINE_DOCKER_REPO_NAME": "LLM_BASELINE_DOCKER_REPO_NAME",
    "LLM_BASELINE_PUB_SUB_TOPIC_NAME": "LLM_BASELINE_PUB_SUB_TOPIC_NAME",
    "LLM_BASELINE_STAGING_BUCKET": "LLM_BASELINE_STAGING_BUCKET",
    "LLM_BASELINE_SERVICE_ACCOUNT": "LLM_BASELINE_SERVICE_ACCOUNT",
    "LLM_BASELINE_NEWS_BUCKET": "LLM_BASELINE_NEWS_BUCKET",
    "WORKSPACE_BUCKET": "WORKSPACE_BUCKET",
}


def __getattr__(name):
    """Read environment variables lazily on each access (PEP 562)."""
    if name in _STR_VARS:
        return os.environ.get(_STR_VARS[name])
    if name == "NUM_CPUS":
        return int(os.environ.get("NUM_CPUS", 1))
    if name == "RUNNING_LOCALLY":
        return bool(int(os.environ.get("RUNNING_LOCALLY", False)))
    if name == "BUCKET_MOUNT_POINT":
        return os.environ.get("BUCKET_MOUNT_POINT", "")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """Expose the lazily-read environment variable names to ``dir()``/autocomplete."""
    extra = {"NUM_CPUS", "RUNNING_LOCALLY", "BUCKET_MOUNT_POINT"}
    return sorted(set(globals()) | set(_STR_VARS) | extra)
