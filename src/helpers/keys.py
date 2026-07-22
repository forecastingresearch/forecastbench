"""utils for key-related tasks in llm-benchmark.

Secrets are resolved lazily on first attribute access (PEP 562 module ``__getattr__``)
and memoized. This ensures that merely *importing* this module performs no network/Secret Manager call.
"""

from google.cloud import secretmanager

from . import env

# Public attribute name -> Secret Manager secret name. The two differ only for GOOGLE/GEMINI.
_SECRET_NAMES = {
    # LLM
    "API_KEY_ANTHROPIC": "API_KEY_ANTHROPIC",
    "API_KEY_OPENAI": "API_KEY_OPENAI",
    "API_KEY_TOGETHERAI": "API_KEY_TOGETHERAI",
    "API_KEY_GOOGLE": "API_KEY_GEMINI",
    "API_KEY_MISTRAL": "API_KEY_MISTRAL",
    "API_KEY_XAI": "API_KEY_XAI",
    # QUESTION DATASET SOURCES
    "API_EMAIL_ACLED": "API_EMAIL_ACLED",
    "API_PASSWORD_ACLED": "API_PASSWORD_ACLED",
    "API_KEY_FRED": "API_KEY_FRED",
    # QUESTION MARKET SOURCES
    "API_KEY_METACULUS": "API_KEY_METACULUS",
    "API_KEY_POLYMARKET": "API_KEY_POLYMARKET",
    "API_KEY_INFER": "API_KEY_INFER",
    # WORKFLOW BOT
    "API_SLACK_BOT_NOTIFICATION": "API_SLACK_BOT_NOTIFICATION",
    "API_SLACK_BOT_CHANNEL": "API_SLACK_BOT_CHANNEL",
    # GITHUB
    "API_GITHUB_DATASET_REPO_URL": "API_GITHUB_DATASET_REPO_URL",
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
    name = f"projects/{env.PROJECT_ID}/secrets/{secret_name}/versions/{version_id}"
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
    secret_name = _SECRET_NAMES.get(name)
    if secret_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name not in _cache:
        _cache[name] = get_secret(secret_name)
    return _cache[name]


def __dir__():
    """Expose the lazily-resolved secret names to ``dir()``/autocomplete."""
    return sorted(set(globals()) | set(_SECRET_NAMES))
