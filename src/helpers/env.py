"""Environment variables.

Values are read from ``os.environ`` lazily on each attribute access (PEP 562 module
``__getattr__``) rather than snapshotted at import.
"""

import os

# Names read as plain strings (``None`` if unset); the attribute name is the variable name.
_STR_VARS = {
    "CLOUD_PROJECT",
    "CLOUD_DEPLOY_REGION",
    "QUESTION_BANK_BUCKET",
    "QUESTION_SETS_BUCKET",
    "FORECAST_SETS_BUCKET",
    "FORECAST_SETS_TRANSCRIPTS_BUCKET",
    "PROCESSED_FORECAST_SETS_BUCKET",
    "PUBLIC_RELEASE_BUCKET",
    "WEBSITE_BUCKET",
    "WEBSITE_STAGING_ASSETS_BUCKET",
    "LLM_BASELINE_DOCKER_IMAGE_NAME",
    "LLM_BASELINE_DOCKER_REPO_NAME",
    "LLM_BASELINE_PUB_SUB_TOPIC_NAME",
    "LLM_BASELINE_STAGING_BUCKET",
    "LLM_BASELINE_SERVICE_ACCOUNT",
    "LLM_BASELINE_NEWS_BUCKET",
    "WORKSPACE_BUCKET",
}


def __getattr__(name):
    """Read environment variables lazily on each access (PEP 562)."""
    if name in _STR_VARS:
        return os.environ.get(name)
    if name == "NUM_CPUS":
        return int(os.environ.get("NUM_CPUS", 1))
    if name == "RUNNING_LOCALLY":
        return bool(int(os.environ.get("RUNNING_LOCALLY", False)))
    if name == "BUCKET_MOUNT_POINT":
        return os.environ.get("BUCKET_MOUNT_POINT", "")
    if name == "RANDOM_SEED":
        # Seed for sources of non-determinism for testing/reproducibility; must be unset/None in prod
        value = os.environ.get("RANDOM_SEED")
        return int(value) if value else None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """Expose the lazily-read environment variable names to ``dir()``/autocomplete."""
    extra = {"NUM_CPUS", "RUNNING_LOCALLY", "BUCKET_MOUNT_POINT", "RANDOM_SEED"}
    return sorted(set(globals()) | set(_STR_VARS) | extra)
