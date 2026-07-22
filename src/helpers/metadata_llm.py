"""LLM-related utilities."""

from functools import cache

from utils.llm import model_runs
from utils.llm.model_registry import configure_api_keys, validate_provider_keys

from .openai_safety import get_openai_safety_identifier

_METADATA_MODEL_RUN_KEY = "gpt-5-mini-2025-08-07-run-variant-01"


@cache
def _get_metadata_model_run() -> model_runs.ModelRun:
    """Return the configured shared model run for metadata requests."""
    metadata_model_run = model_runs.get_model_run(_METADATA_MODEL_RUN_KEY)
    configure_api_keys(from_gcp=True)
    validate_provider_keys([metadata_model_run.provider])
    return metadata_model_run


def get_metadata_model_response(prompt: str, max_output_tokens: int) -> str:
    """Get a response from the shared metadata model.

    Args:
      prompt (str): Prompt to send to the metadata model.
      max_output_tokens (int): Maximum number of output tokens to request.
    """
    return _get_metadata_model_run().get_response(
        prompt=prompt,
        max_output_tokens=max_output_tokens,
        reasoning={"effort": "minimal"},
        safety_identifier=get_openai_safety_identifier(),
    )
