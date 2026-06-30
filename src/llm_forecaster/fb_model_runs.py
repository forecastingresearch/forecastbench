"""ForecastBench selected shared LLM model runs."""

from typing import Sequence

from utils.llm import model_runs as shared_model_runs
from utils.llm.model_registry import configure_api_keys, validate_provider_keys
from utils.llm.provider_registry import PROVIDERS, Provider

from helpers.openai_safety import get_openai_safety_identifier

ModelRun = shared_model_runs.ModelRun

FB_MODEL_RUN_KEYS = [
    "gpt-5-nano-2025-08-07-run-variant-01",
    "gpt-5-mini-2025-08-07-run-variant-01",
    "gpt-5.4-nano-2026-03-17-run-variant-01",
    "gpt-5.4-mini-2026-03-17-run-variant-01",
    "gpt-5.4-2026-03-05-run-variant-01",
    "gpt-5.5-2026-04-23-run-variant-01",
    "gpt-5.5-2026-04-23-run-variant-04",
    "minimax-m2.7-run-variant-01",
    "minimax-m2.7-run-variant-02",
    "minimax-m3-run-variant-01",
    "kimi-k2.6-run-variant-02",
    "glm-5.1-run-variant-01",
    "glm-5.2-run-variant-01",
    "glm-5.2-run-variant-02",
    "gemma-4-31b-it-run-variant-01",
    "claude-haiku-4-5-20251001-run-variant-01",
    "claude-sonnet-4-5-20250929-run-variant-01",
    "claude-sonnet-4-6-run-variant-01",
    "claude-sonnet-4-6-run-variant-03",
    "claude-opus-4-7-run-variant-01",
    "claude-opus-4-8-run-variant-01",
    "claude-opus-4-8-run-variant-03",
    "claude-opus-4-8-run-variant-04",
    "grok-4.20-0309-reasoning-run-variant-01",
    "grok-4.20-0309-non-reasoning-run-variant-01",
    "grok-4.3-run-variant-01",
    "gemini-3.5-flash-run-variant-01",
    "gemini-3.5-flash-run-variant-02",
    "gemini-3.1-pro-preview-run-variant-01",
    "gemini-3.1-pro-preview-run-variant-02",
    "gemini-3.1-flash-lite-run-variant-01",
]

FB_MODEL_RUNS = shared_model_runs.select_model_runs(FB_MODEL_RUN_KEYS)
FB_MODEL_RUNS_BY_KEY = {run.model_run_key: run for run in FB_MODEL_RUNS}
FB_MODEL_RUNS_BY_SLUG = {run.slug: run for run in FB_MODEL_RUNS}
FORECAST_EXTRACTION_MODEL = shared_model_runs.get_model_run("gpt-5-mini-2025-08-07-run-variant-01")


DEFAULT_PROVIDER_MAX_WORKERS = 4
PROVIDER_MAX_WORKERS = {
    PROVIDERS["OpenAI"]: 50,
    PROVIDERS["Anthropic"]: 50,
    PROVIDERS["Google"]: 50,
    PROVIDERS["xAI"]: 50,
    PROVIDERS["Together"]: 4,
}


def get_model_run(model_run_key: str) -> ModelRun:
    """Return a ForecastBench model run by immutable key; prefer this for stable references."""
    try:
        return FB_MODEL_RUNS_BY_KEY[model_run_key]
    except KeyError as exc:
        raise KeyError(f"Unknown ForecastBench LLM model run key: {model_run_key}") from exc


def get_model_run_by_slug(slug: str) -> ModelRun:
    """Return a ForecastBench model run by slug; prefer get_model_run when possible."""
    try:
        return FB_MODEL_RUNS_BY_SLUG[slug]
    except KeyError as exc:
        raise KeyError(f"Unknown ForecastBench LLM model run slug: {slug}") from exc


def providers_for_model_runs(model_runs: Sequence[ModelRun]) -> list[Provider]:
    """Return unique providers required for the requested model runs."""
    providers = []
    for run in [*model_runs, FORECAST_EXTRACTION_MODEL]:
        if run.provider not in providers:
            providers.append(run.provider)
    return providers


def _configure_openai_safety_identifier(model_runs: Sequence[ModelRun]) -> None:
    """Add the project safety identifier to selected OpenAI model-run options."""
    openai_runs = [run for run in model_runs if run.provider == PROVIDERS["OpenAI"]]
    if not openai_runs:
        return

    safety_identifier = get_openai_safety_identifier()
    for run in openai_runs:
        run.options["safety_identifier"] = safety_identifier


def configure_and_validate_provider_keys(model_runs: Sequence[ModelRun]) -> None:
    """Configure provider keys from GCP and validate all required providers."""
    unique_providers = providers_for_model_runs(model_runs)
    configure_api_keys(from_gcp=True)
    _configure_openai_safety_identifier([*model_runs, FORECAST_EXTRACTION_MODEL])
    validate_provider_keys(unique_providers)
