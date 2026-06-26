"""Strict normalization for ForecastBench LLM identities."""

from copy import deepcopy
from dataclasses import dataclass

from utils.llm import model_runs

from helpers import constants
from llm_forecaster.forecast_variants import (
    SCRATCHPAD,
    SCRATCHPAD_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_NEWS,
    SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_SECOND_NEWS,
    SUPERFORECASTER_WITH_NEWS_1,
    SUPERFORECASTER_WITH_NEWS_2,
    SUPERFORECASTER_WITH_NEWS_3,
    ZERO_SHOT,
    ZERO_SHOT_WITH_FREEZE_VALUES,
    ForecastVariant,
    get_known_variant,
)
from llm_forecaster.output import display_model_name

LLM_IDENTITY_COLUMNS = (
    "model_key",
    "model_run_key",
    "model_run_slug",
    "forecast_variant_key",
    "uses_freeze_values",
    "uses_tools",
)


@dataclass(frozen=True, slots=True)
class ForecastBenchLLMIdentity:
    """Normalized ForecastBench LLM identity for legacy and current display names."""

    model_run: model_runs.ModelRun
    forecast_variant: ForecastVariant

    @property
    def model_run_key(self) -> str:
        """Return the shared model-run key written to processed files."""
        return self.model_run.model_run_key

    @property
    def model_run_slug(self) -> str:
        """Return the shared model-run slug written to processed files."""
        return self.model_run.slug

    @property
    def model_key(self) -> str:
        """Return the base model key used for release-date joins."""
        return self.model_run.model_key

    @property
    def forecast_variant_key(self) -> str:
        """Return the ForecastBench forecast variant key written to processed files."""
        return self.forecast_variant.key

    @property
    def display_name(self) -> str:
        """Return the normalized display model name."""
        return display_model_name(self.model_run, self.forecast_variant)

    @property
    def model_organization(self) -> str:
        """Return the model-making organization."""
        return self.model_run.lab.name

    @property
    def uses_freeze_values(self) -> bool:
        """Return whether this forecast variant used freeze values."""
        return self.forecast_variant.market_prompt_uses_freeze_values

    @property
    def uses_tools(self) -> bool:
        """Return whether this model run declares provider tools."""
        return _model_run_uses_tools(self.model_run)

    def as_normalized_fields(self) -> dict[str, str | bool]:
        """Return fields stored on processed ForecastBench LLM rows."""
        return {
            "model": self.display_name,
            "model_organization": self.model_organization,
            "model_key": self.model_key,
            "model_run_key": self.model_run_key,
            "model_run_slug": self.model_run_slug,
            "forecast_variant_key": self.forecast_variant_key,
            "uses_freeze_values": self.uses_freeze_values,
            "uses_tools": self.uses_tools,
        }


# Exact parenthetical variant labels that appear in historical processed
# forecast files. Multiple historical labels can map to the same normalized
# forecast variant; tool use is represented by the model run, not the variant.
LEGACY_FORECAST_VARIANTS_BY_LABEL = {
    "scratchpad": SCRATCHPAD,
    "scratchpad with freeze values": SCRATCHPAD_WITH_FREEZE_VALUES,
    "scratchpad with freeze values with web search": SCRATCHPAD_WITH_FREEZE_VALUES,
    "scratchpad with news": SCRATCHPAD_WITH_NEWS,
    "scratchpad with news with freeze values": SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES,
    "scratchpad with SECOND news": SCRATCHPAD_WITH_SECOND_NEWS,
    "scratchpad with web search": SCRATCHPAD,
    "superforecaster with news 1": SUPERFORECASTER_WITH_NEWS_1,
    "superforecaster with news 2": SUPERFORECASTER_WITH_NEWS_2,
    "superforecaster with news 3": SUPERFORECASTER_WITH_NEWS_3,
    "zero shot": ZERO_SHOT,
    "zero shot with freeze values": ZERO_SHOT_WITH_FREEZE_VALUES,
    "zero shot with freeze values with web search": ZERO_SHOT_WITH_FREEZE_VALUES,
    "zero shot with web search": ZERO_SHOT,
}

# The only 2nd variant of the model run key we had was the first websearch
# tool provided by OAI. Handled separately.
_LEGACY_PROVIDER_TOOL_VARIANT_LABELS = frozenset(
    {
        "scratchpad with freeze values with web search",
        "scratchpad with web search",
        "zero shot with freeze values with web search",
        "zero shot with web search",
    }
)
# Maps historical forecast set base identities directly to the shared ModelRun for legacy variants.
# Each value is resolved at import time so bad model_run_key references fail immediately.
_LEGACY_LLM_BASE_MODEL_RUNS = {
    ("Anthropic", "Claude-2.1"): model_runs.get_model_run("claude-2.1-run-variant-01"),
    ("Anthropic", "Claude-3-5-Sonnet-20240620"): model_runs.get_model_run(
        "claude-3-5-sonnet-20240620-run-variant-01"
    ),
    ("Anthropic", "Claude-3-5-Sonnet-20241022"): model_runs.get_model_run(
        "claude-3-5-sonnet-20241022-run-variant-01"
    ),
    ("Anthropic", "Claude-3-7-Sonnet-20250219"): model_runs.get_model_run(
        "claude-3-7-sonnet-20250219-run-variant-01"
    ),
    ("Anthropic", "Claude-3-Haiku-20240307"): model_runs.get_model_run(
        "claude-3-haiku-20240307-run-variant-01"
    ),
    ("Anthropic", "Claude-3-Opus-20240229"): model_runs.get_model_run(
        "claude-3-opus-20240229-run-variant-01"
    ),
    ("Anthropic", "Claude-Haiku-4-5-20251001"): model_runs.get_model_run(
        "claude-haiku-4-5-20251001-run-variant-01"
    ),
    ("Anthropic", "Claude-Opus-4-1-20250805"): model_runs.get_model_run(
        "claude-opus-4-1-20250805-run-variant-01"
    ),
    ("Anthropic", "Claude-Opus-4-20250514"): model_runs.get_model_run(
        "claude-opus-4-20250514-run-variant-01"
    ),
    ("Anthropic", "Claude-Opus-4-5-20251101"): model_runs.get_model_run(
        "claude-opus-4-5-20251101-run-variant-01"
    ),
    ("Anthropic", "Claude-Opus-4-6"): model_runs.get_model_run("claude-opus-4-6-run-variant-01"),
    ("Anthropic", "Claude-Opus-4-7"): model_runs.get_model_run("claude-opus-4-7-run-variant-01"),
    ("Anthropic", "Claude-Opus-4-8"): model_runs.get_model_run("claude-opus-4-8-run-variant-01"),
    ("Anthropic", "Claude-Sonnet-4-20250514"): model_runs.get_model_run(
        "claude-sonnet-4-20250514-run-variant-01"
    ),
    ("Anthropic", "Claude-Sonnet-4-5-20250929"): model_runs.get_model_run(
        "claude-sonnet-4-5-20250929-run-variant-01"
    ),
    ("Anthropic", "Claude-Sonnet-4-6"): model_runs.get_model_run(
        "claude-sonnet-4-6-run-variant-01"
    ),
    ("DeepSeek", "DeepSeek-R1"): model_runs.get_model_run("deepseek-r1-run-variant-01"),
    ("DeepSeek", "DeepSeek-V3"): model_runs.get_model_run("deepseek-v3-run-variant-01"),
    ("DeepSeek", "DeepSeek-V3.1"): model_runs.get_model_run("deepseek-v3.1-run-variant-01"),
    ("DeepSeek", "DeepSeek-V4-Pro"): model_runs.get_model_run("deepseek-v4-pro-run-variant-01"),
    ("Google", "Gemini-1.5-Flash"): model_runs.get_model_run("gemini-1.5-flash-run-variant-01"),
    ("Google", "Gemini-1.5-Pro"): model_runs.get_model_run("gemini-1.5-pro-run-variant-01"),
    ("Google", "Gemini-2.0-Flash-Lite-001"): model_runs.get_model_run(
        "gemini-2.0-flash-lite-001-run-variant-01"
    ),
    ("Google", "Gemini-2.5-Flash"): model_runs.get_model_run("gemini-2.5-flash-run-variant-01"),
    ("Google", "Gemini-2.5-Flash-Preview-04-17"): model_runs.get_model_run(
        "gemini-2.5-flash-preview-04-17-run-variant-01"
    ),
    ("Google", "Gemini-2.5-Pro"): model_runs.get_model_run("gemini-2.5-pro-run-variant-01"),
    ("Google", "Gemini-2.5-Pro-Exp-03-25"): model_runs.get_model_run(
        "gemini-2.5-pro-exp-03-25-run-variant-01"
    ),
    ("Google", "Gemini-2.5-Pro-Preview-03-25"): model_runs.get_model_run(
        "gemini-2.5-pro-preview-03-25-run-variant-01"
    ),
    ("Google", "Gemini-3-Flash-Preview"): model_runs.get_model_run(
        "gemini-3-flash-preview-run-variant-01"
    ),
    ("Google", "Gemini-3-Pro-Preview"): model_runs.get_model_run(
        "gemini-3-pro-preview-run-variant-01"
    ),
    ("Google", "Gemini-3.1-Flash-Lite-Preview"): model_runs.get_model_run(
        "gemini-3.1-flash-lite-preview-run-variant-01"
    ),
    ("Google", "Gemini-3.1-Flash-Lite"): model_runs.get_model_run(
        "gemini-3.1-flash-lite-run-variant-01"
    ),
    ("Google", "Gemini-3.1-Pro-Preview"): model_runs.get_model_run(
        "gemini-3.1-pro-preview-run-variant-01"
    ),
    ("Google", "Gemini-3.5-Flash"): model_runs.get_model_run("gemini-3.5-flash-run-variant-01"),
    ("Google", "Gemma-4-31B-It"): model_runs.get_model_run("gemma-4-31b-it-run-variant-01"),
    ("Meta", "Llama-2-70b-Chat-Hf"): model_runs.get_model_run("llama-2-70b-chat-hf-run-variant-01"),
    ("Meta", "Llama-3-70b-Chat-Hf"): model_runs.get_model_run("llama-3-70b-chat-hf-run-variant-01"),
    ("Meta", "Llama-3-8b-Chat-Hf"): model_runs.get_model_run("llama-3-8b-chat-hf-run-variant-01"),
    ("Meta", "Llama-3.2-3B-Instruct-Turbo"): model_runs.get_model_run(
        "llama-3.2-3b-instruct-turbo-run-variant-01"
    ),
    ("Meta", "Llama-3.3-70B-Instruct-Turbo"): model_runs.get_model_run(
        "llama-3.3-70b-instruct-turbo-run-variant-01"
    ),
    ("Meta", "Llama-4-Maverick-17B-128E-Instruct-FP8"): model_runs.get_model_run(
        "llama-4-maverick-17b-128e-instruct-fp8-run-variant-01"
    ),
    ("Meta", "Llama-4-Scout-17B-16E-Instruct"): model_runs.get_model_run(
        "llama-4-scout-17b-16e-instruct-run-variant-01"
    ),
    ("Meta", "Meta-Llama-3.1-405B-Instruct-Turbo"): model_runs.get_model_run(
        "meta-llama-3.1-405b-instruct-turbo-run-variant-01"
    ),
    ("Minimax", "MiniMax-M2.5"): model_runs.get_model_run("minimax-m2.5-run-variant-01"),
    ("Minimax", "MiniMax-M2.7"): model_runs.get_model_run("minimax-m2.7-run-variant-01"),
    ("Mistral", "Magistral-Medium-2506"): model_runs.get_model_run(
        "magistral-medium-2506-run-variant-01"
    ),
    ("Mistral", "Mistral-Large-2407"): model_runs.get_model_run(
        "mistral-large-2407-run-variant-01"
    ),
    ("Mistral", "Mistral-Large-2411"): model_runs.get_model_run(
        "mistral-large-2411-run-variant-01"
    ),
    ("Mistral AI", "Mistral-Large-Latest"): model_runs.get_model_run(
        "mistral-large-latest-run-variant-01"
    ),
    ("Mistral AI", "Mixtral-8x22B-Instruct-V0.1"): model_runs.get_model_run(
        "mixtral-8x22b-instruct-v0.1-run-variant-01"
    ),
    ("Mistral AI", "Mixtral-8x7B-Instruct-V0.1"): model_runs.get_model_run(
        "mixtral-8x7b-instruct-v0.1-run-variant-01"
    ),
    ("Moonshot", "Kimi-K2-Instruct"): model_runs.get_model_run("kimi-k2-instruct-run-variant-01"),
    ("Moonshot", "Kimi-K2-Instruct-0905"): model_runs.get_model_run(
        "kimi-k2-instruct-0905-run-variant-01"
    ),
    ("Moonshot", "Kimi-K2-Thinking"): model_runs.get_model_run("kimi-k2-thinking-run-variant-01"),
    ("Moonshot", "Kimi-K2.5"): model_runs.get_model_run("kimi-k2.5-run-variant-01"),
    ("Moonshot", "Kimi-K2.6"): model_runs.get_model_run("kimi-k2.6-run-variant-01"),
    ("OpenAI", "GPT-3.5-Turbo-0125"): model_runs.get_model_run("gpt-3.5-turbo-0125-run-variant-01"),
    ("OpenAI", "GPT-4-0613"): model_runs.get_model_run("gpt-4-0613-run-variant-01"),
    ("OpenAI", "GPT-4-Turbo-2024-04-09"): model_runs.get_model_run(
        "gpt-4-turbo-2024-04-09-run-variant-01"
    ),
    ("OpenAI", "GPT-4.1-2025-04-14"): model_runs.get_model_run("gpt-4.1-2025-04-14-run-variant-01"),
    ("OpenAI", "GPT-4.5-Preview-2025-02-27"): model_runs.get_model_run(
        "gpt-4.5-preview-2025-02-27-run-variant-01"
    ),
    ("OpenAI", "GPT-4o"): model_runs.get_model_run("gpt-4o-2024-05-13-run-variant-01"),
    ("OpenAI", "GPT-4o-2024-05-13"): model_runs.get_model_run("gpt-4o-2024-05-13-run-variant-01"),
    ("OpenAI", "GPT-4o-2024-11-20"): model_runs.get_model_run("gpt-4o-2024-11-20-run-variant-01"),
    ("OpenAI", "GPT-5-2025-08-07"): model_runs.get_model_run("gpt-5-2025-08-07-run-variant-01"),
    ("OpenAI", "GPT-5-Mini-2025-08-07"): model_runs.get_model_run(
        "gpt-5-mini-2025-08-07-run-variant-01"
    ),
    ("OpenAI", "GPT-5-Nano-2025-08-07"): model_runs.get_model_run(
        "gpt-5-nano-2025-08-07-run-variant-01"
    ),
    ("OpenAI", "GPT-5.1-2025-11-13"): model_runs.get_model_run("gpt-5.1-2025-11-13-run-variant-01"),
    ("OpenAI", "GPT-5.2-2025-12-11"): model_runs.get_model_run("gpt-5.2-2025-12-11-run-variant-01"),
    ("OpenAI", "GPT-5.4-2026-03-05"): model_runs.get_model_run("gpt-5.4-2026-03-05-run-variant-01"),
    ("OpenAI", "GPT-5.4-Mini-2026-03-17"): model_runs.get_model_run(
        "gpt-5.4-mini-2026-03-17-run-variant-01"
    ),
    ("OpenAI", "GPT-5.4-Nano-2026-03-17"): model_runs.get_model_run(
        "gpt-5.4-nano-2026-03-17-run-variant-01"
    ),
    ("OpenAI", "GPT-5.5-2026-04-23"): model_runs.get_model_run("gpt-5.5-2026-04-23-run-variant-01"),
    ("OpenAI", "O3-2025-04-16"): model_runs.get_model_run("o3-2025-04-16-run-variant-01"),
    ("OpenAI", "O3-Mini-2025-01-31"): model_runs.get_model_run("o3-mini-2025-01-31-run-variant-01"),
    ("OpenAI", "O4-Mini-2025-04-16"): model_runs.get_model_run("o4-mini-2025-04-16-run-variant-01"),
    ("Qwen", "Qwen1.5-110B-Chat"): model_runs.get_model_run("qwen1.5-110b-chat-run-variant-01"),
    ("Qwen", "Qwen2.5-72B-Instruct-Turbo"): model_runs.get_model_run(
        "qwen2.5-72b-instruct-turbo-run-variant-01"
    ),
    ("Qwen", "Qwen3-235B-A22B-Fp8-Tput"): model_runs.get_model_run(
        "qwen3-235b-a22b-fp8-tput-run-variant-01"
    ),
    ("Qwen", "Qwen3-235B-A22B-Thinking-2507"): model_runs.get_model_run(
        "qwen3-235b-a22b-thinking-2507-run-variant-01"
    ),
    ("Qwen", "QwQ-32B-Preview"): model_runs.get_model_run("qwq-32b-preview-run-variant-01"),
    ("xAI", "Grok-4-0709"): model_runs.get_model_run("grok-4-0709-run-variant-01"),
    ("xAI", "Grok-4-1-Fast-Non-Reasoning"): model_runs.get_model_run(
        "grok-4-1-fast-non-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4-1-Fast-Reasoning"): model_runs.get_model_run(
        "grok-4-1-fast-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4-Fast-Non-Reasoning"): model_runs.get_model_run(
        "grok-4-fast-non-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4-Fast-Reasoning"): model_runs.get_model_run(
        "grok-4-fast-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4.20-0309-Non-Reasoning"): model_runs.get_model_run(
        "grok-4.20-0309-non-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4.20-0309-Reasoning"): model_runs.get_model_run(
        "grok-4.20-0309-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4.20-Beta-0309-Non-Reasoning"): model_runs.get_model_run(
        "grok-4.20-beta-0309-non-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4.20-Beta-0309-Reasoning"): model_runs.get_model_run(
        "grok-4.20-beta-0309-reasoning-run-variant-01"
    ),
    ("xAI", "Grok-4.3"): model_runs.get_model_run("grok-4.3-run-variant-01"),
    ("xAI", "Grok-beta"): model_runs.get_model_run("grok-beta-run-variant-01"),
    ("Z.ai", "GLM-4.5-Air-FP8"): model_runs.get_model_run("glm-4.5-air-fp8-run-variant-01"),
    ("Z.ai", "GLM-4.6"): model_runs.get_model_run("glm-4.6-run-variant-01"),
    ("Z.ai", "GLM-4.7"): model_runs.get_model_run("glm-4.7-run-variant-01"),
    ("Z.ai", "GLM-5"): model_runs.get_model_run("glm-5-run-variant-01"),
    ("Z.ai", "GLM-5.1"): model_runs.get_model_run("glm-5.1-run-variant-01"),
}


def _model_run_uses_tools(model_run: model_runs.ModelRun) -> bool:
    """Return whether a model run declares provider tools."""
    return bool(model_run.options.get("tools"))


def _forecast_variant_for_key(
    forecast_variant_key: str,
) -> ForecastVariant:
    """Return normalized semantics for an explicit forecast variant key."""
    try:
        return get_known_variant(forecast_variant_key)
    except KeyError as exc:
        raise KeyError(
            f"Unknown ForecastBench LLM forecast_variant_key: {forecast_variant_key}"
        ) from exc


def _identity_for_explicit_keys(
    model_run_key: str,
    forecast_variant_key: str,
) -> ForecastBenchLLMIdentity:
    """Return normalized identity semantics for explicit new-file identity keys."""
    model_run = model_runs.get_model_run(model_run_key)
    forecast_variant = _forecast_variant_for_key(forecast_variant_key)
    return ForecastBenchLLMIdentity(
        model_run=model_run,
        forecast_variant=forecast_variant,
    )


def identity_for_model_run_and_forecast_variant(
    model_run_key: str,
    forecast_variant_key: str,
) -> ForecastBenchLLMIdentity:
    """Return normalized identity semantics for explicit model-run and variant keys."""
    return _identity_for_explicit_keys(model_run_key, forecast_variant_key)


LegacyModelRunDeclaration = tuple[model_runs.ModelRun, ForecastVariant]


def _legacy_model_name(base_model: str, variant: str) -> str:
    """Return a historical processed-file model name for a legacy variant.

    Example:
        "Claude-2.1", "zero shot" -> "Claude-2.1 (zero shot)"
    """
    return f"{base_model} ({variant})"


def _build_default_legacy_llm_model_runs() -> dict[tuple[str, str], LegacyModelRunDeclaration]:
    """Build exact legacy model-run rows that use the default base model run.

    Each row maps a historical processed-file display identity to the default
    model run for that base model plus the exact legacy variant metadata. For
    example:

        ("Anthropic", "Claude-2.1 (zero shot)") ->
            (ModelRun("claude-2.1-run-variant-01"), ZERO_SHOT)

        ("Anthropic", "Claude-2.1 (zero shot with freeze values)") ->
            (
                ModelRun("claude-2.1-run-variant-01"),
                ZERO_SHOT_WITH_FREEZE_VALUES,
            )

    Tool-labeled variants are skipped here because only specific historical rows
    used provider tools; those exact rows are declared in _LEGACY_LLM_MODEL_RUNS.
    """
    mapping = {}
    for legacy_identity, default_model_run in _LEGACY_LLM_BASE_MODEL_RUNS.items():
        legacy_model_organization, legacy_base_model = legacy_identity
        for variant, variant_options in LEGACY_FORECAST_VARIANTS_BY_LABEL.items():
            if variant in _LEGACY_PROVIDER_TOOL_VARIANT_LABELS:
                # Provider-tool runs are per-variant exceptions, not base-model defaults.
                continue
            mapping[(legacy_model_organization, _legacy_model_name(legacy_base_model, variant))] = (
                default_model_run,
                variant_options,
            )
    return mapping


# Exact historical processed-file display names mapped to the model run and
# forecast variant they used. Provider-tool runs are ordinary rows here.
_LEGACY_LLM_MODEL_RUNS: dict[tuple[str, str], LegacyModelRunDeclaration] = {
    **_build_default_legacy_llm_model_runs(),
    (
        "OpenAI",
        _legacy_model_name("GPT-4o-2024-11-20", "scratchpad with freeze values with web search"),
    ): (
        model_runs.get_model_run("gpt-4o-2024-11-20-run-variant-02"),
        LEGACY_FORECAST_VARIANTS_BY_LABEL["scratchpad with freeze values with web search"],
    ),
    (
        "OpenAI",
        _legacy_model_name("GPT-4o-2024-11-20", "scratchpad with web search"),
    ): (
        model_runs.get_model_run("gpt-4o-2024-11-20-run-variant-02"),
        LEGACY_FORECAST_VARIANTS_BY_LABEL["scratchpad with web search"],
    ),
    (
        "OpenAI",
        _legacy_model_name("GPT-4o-2024-11-20", "zero shot with freeze values with web search"),
    ): (
        model_runs.get_model_run("gpt-4o-2024-11-20-run-variant-02"),
        LEGACY_FORECAST_VARIANTS_BY_LABEL["zero shot with freeze values with web search"],
    ),
    (
        "OpenAI",
        _legacy_model_name("GPT-4o-2024-11-20", "zero shot with web search"),
    ): (
        model_runs.get_model_run("gpt-4o-2024-11-20-run-variant-02"),
        LEGACY_FORECAST_VARIANTS_BY_LABEL["zero shot with web search"],
    ),
}


def _build_legacy_llm_identity_map() -> dict[tuple[str, str], ForecastBenchLLMIdentity]:
    """Build exact legacy display-name mappings to normalized model-run identities.

    Example row:
        ("Anthropic", "Claude-Opus-4-7 (zero shot)")
        -> ForecastBenchLLMIdentity(
            model_run=ModelRun("claude-opus-4-7-run-variant-01"),
            forecast_variant=ZERO_SHOT,
        )
    """
    return {
        legacy_identity: ForecastBenchLLMIdentity(
            model_run=model_run,
            forecast_variant=variant_options,
        )
        for legacy_identity, (model_run, variant_options) in _LEGACY_LLM_MODEL_RUNS.items()
    }


# Exact mapping from historical processed-file display identities to normalized
# model-run identities. This is the source of truth for legacy ForecastBench LLM names.
LEGACY_LLM_IDENTITY_MAP = _build_legacy_llm_identity_map()


def _identity_for_legacy_forecastbench_llm(
    model_organization: str,
    model: str,
) -> ForecastBenchLLMIdentity | None:
    """Return normalized identity metadata for a legacy ForecastBench LLM display identity.

    Example:
        ("Anthropic", "Claude-Opus-4-7 (zero shot)")
        -> ForecastBenchLLMIdentity(
            model_run=ModelRun("claude-opus-4-7-run-variant-01"),
            forecast_variant=ZERO_SHOT,
        )
    """
    return LEGACY_LLM_IDENTITY_MAP.get((model_organization, model))


def normalize_llm_identity(data: dict) -> dict:
    """Return a validated copy of a ForecastBench LLM model identity.

    Legacy names are fixed in processed files. This maps them to the same
    canonical identity fields written by the current LLM forecaster.

    Example:
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (zero shot)",
            "model_organization": "Anthropic",
        }
        -> {
            "organization": "ForecastBench",
            "model": "claude-opus-4-7-1024",
            "model_organization": "Anthropic",
            "model_key": "claude-opus-4-7",
            "model_run_key": "claude-opus-4-7-run-variant-01",
            "model_run_slug": "claude-opus-4-7-1024",
            "forecast_variant_key": "zero-shot",
            "uses_freeze_values": False,
            "uses_tools": False,
        }
    """
    normalized = deepcopy(data)
    if normalized.get("organization") != constants.BENCHMARK_NAME:
        return normalized

    model = normalized.get("model")
    model_organization = normalized.get("model_organization")
    if model_organization == constants.BENCHMARK_NAME:
        return normalized

    model_run_key = normalized.get("model_run_key")
    forecast_variant_key = normalized.get("forecast_variant_key")
    if isinstance(model_run_key, str) and isinstance(forecast_variant_key, str):
        identity = _identity_for_explicit_keys(model_run_key, forecast_variant_key)
        normalized.update(identity.as_normalized_fields())
        return normalized

    identity = _identity_for_legacy_forecastbench_llm(model_organization, model)
    if identity is None:
        raise KeyError(f"Unmapped ForecastBench LLM model: {(model_organization, model)}")

    normalized.update(identity.as_normalized_fields())
    return normalized
