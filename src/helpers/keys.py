"""utils for key-related tasks in llm-benchmark.

Secrets are resolved lazily on first attribute access (PEP 562 module ``__getattr__``)
and memoized. This ensures that merely *importing* this module performs no network/Secret Manager call.
"""

from google.cloud import secretmanager

from . import env

# Secret Manager secret names; the attribute name is the secret name.
_SECRET_NAMES = {
    # QUESTION DATASET SOURCES
    "API_EMAIL_ACLED",
    "API_PASSWORD_ACLED",
    "API_KEY_FRED",
    # QUESTION MARKET SOURCES
    "API_KEY_METACULUS",
    "API_KEY_POLYMARKET",
    # WORKFLOW BOT
    "API_SLACK_BOT_NOTIFICATION",
    "API_SLACK_BOT_CHANNEL",
    # GITHUB
    "API_GITHUB_DATASET_REPO_URL",
}

_cache: dict = {}


def get_secret(secret_name, version_id="latest"):
    """
    Retrieve the payload of a specified secret version from Secret Manager.

    Accesses the Google Cloud Secret Manager to fetch the payload of a secret version
    identified by `project_id`, `secret_name`, and `version_id`. Decodes the payload
    from bytes to a UTF-8 string and returns it.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{env.CLOUD_PROJECT}/secrets/{secret_name}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_secret_that_may_not_exist(secret_name, version_id="latest"):
    """Get a secret from Secret Manager but don't fail if it doesn't exist."""
    try:
        return get_secret(secret_name, version_id)
    except Exception:
        return None


def __getattr__(name):
    """Lazily resolve and memoize ``API_*`` secrets on first access (PEP 562)."""
    if name not in _SECRET_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name not in _cache:
        _cache[name] = get_secret(name)
    return _cache[name]


def __dir__():
    """Expose the lazily-resolved secret names to ``dir()``/autocomplete."""
    return sorted(set(globals()) | set(_SECRET_NAMES))
