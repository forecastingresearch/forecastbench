"""utils for key-related tasks in llm-benchmark."""

from google.cloud import secretmanager

from . import env


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


# LLM
API_KEY_ANTHROPIC = get_secret("API_KEY_ANTHROPIC")
API_KEY_OPENAI = get_secret("API_KEY_OPENAI")
API_KEY_TOGETHERAI = get_secret("API_KEY_TOGETHERAI")
API_KEY_NEWSCATCHER = get_secret("API_KEY_NEWSCATCHER")
API_KEY_GOOGLE = get_secret("API_KEY_GEMINI")
API_KEY_MISTRAL = get_secret("API_KEY_MISTRAL")

# QUESTION DATASET SOURCES
API_KEY_ACLED = get_secret(secret_name="API_KEY_ACLED")
API_EMAIL_ACLED = get_secret(secret_name="API_EMAIL_ACLED")
API_KEY_FRED = get_secret("API_KEY_FRED")

# QUESTION MARKET SOURCES
API_KEY_METACULUS = get_secret(secret_name="API_KEY_METACULUS")
API_KEY_POLYMARKET = get_secret("API_KEY_POLYMARKET")
API_KEY_INFER = get_secret("API_KEY_INFER")

# WORKFLOW BOT
API_SLACK_BOT_NOTIFICATION = get_secret(secret_name="API_SLACK_BOT_NOTIFICATION")
API_SLACK_BOT_CHANNEL = get_secret(secret_name="API_SLACK_BOT_CHANNEL")

# GITHUB
API_GITHUB_DATASET_REPO_URL = get_secret(secret_name="API_GITHUB_DATASET_REPO_URL")
