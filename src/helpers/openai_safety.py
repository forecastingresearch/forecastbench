"""ForecastBench OpenAI safety identifier helpers."""

from functools import cache

from utils.gcp.secret_manager import get_secret

OPENAI_SAFETY_IDENTIFIER_SECRET_NAME = "OPENAI_SAFETY_IDENTIFIER"


@cache
def get_openai_safety_identifier() -> str:
    """Return the ForecastBench OpenAI safety identifier from Secret Manager."""
    return get_secret(OPENAI_SAFETY_IDENTIFIER_SECRET_NAME).strip()
