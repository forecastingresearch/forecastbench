"""ForecastBench LLM forecast variant declarations."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ForecastVariant:
    """ForecastBench LLM prompt and output variant."""

    key: str
    market_prompt_uses_freeze_values: bool
    active: bool = True


@dataclass(frozen=True)
class DatasetForecastSharingVariantGroup:
    """Variants that share one dataset forecast pass."""

    dataset_prompt_variant: ForecastVariant
    output_variants: tuple[ForecastVariant, ...]


ZERO_SHOT = ForecastVariant(
    key="zero-shot",
    market_prompt_uses_freeze_values=False,
)
ZERO_SHOT_WITH_FREEZE_VALUES = ForecastVariant(
    key="zero-shot-with-freeze-values",
    market_prompt_uses_freeze_values=True,
)
SCRATCHPAD = ForecastVariant(
    key="scratchpad",
    market_prompt_uses_freeze_values=False,
    active=False,
)
SCRATCHPAD_WITH_FREEZE_VALUES = ForecastVariant(
    key="scratchpad-with-freeze-values",
    market_prompt_uses_freeze_values=True,
    active=False,
)
SCRATCHPAD_WITH_NEWS = ForecastVariant(
    key="scratchpad-with-news",
    market_prompt_uses_freeze_values=False,
    active=False,
)
SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES = ForecastVariant(
    key="scratchpad-with-news-with-freeze-values",
    market_prompt_uses_freeze_values=True,
    active=False,
)
SCRATCHPAD_WITH_SECOND_NEWS = ForecastVariant(
    key="scratchpad-with-second-news",
    market_prompt_uses_freeze_values=False,
    active=False,
)
SUPERFORECASTER_WITH_NEWS_1 = ForecastVariant(
    key="superforecaster-with-news-1",
    market_prompt_uses_freeze_values=False,
    active=False,
)
SUPERFORECASTER_WITH_NEWS_2 = ForecastVariant(
    key="superforecaster-with-news-2",
    market_prompt_uses_freeze_values=False,
    active=False,
)
SUPERFORECASTER_WITH_NEWS_3 = ForecastVariant(
    key="superforecaster-with-news-3",
    market_prompt_uses_freeze_values=False,
    active=False,
)

ALL_FORECAST_VARIANTS = (
    ZERO_SHOT,
    ZERO_SHOT_WITH_FREEZE_VALUES,
    SCRATCHPAD,
    SCRATCHPAD_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_NEWS,
    SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_SECOND_NEWS,
    SUPERFORECASTER_WITH_NEWS_1,
    SUPERFORECASTER_WITH_NEWS_2,
    SUPERFORECASTER_WITH_NEWS_3,
)
FORECAST_VARIANTS = tuple(variant for variant in ALL_FORECAST_VARIANTS if variant.active)

ALL_FORECAST_VARIANTS_WITH_CONTEXT = (
    ZERO_SHOT_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_NEWS,
    SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES,
    SCRATCHPAD_WITH_SECOND_NEWS,
    SUPERFORECASTER_WITH_NEWS_1,
    SUPERFORECASTER_WITH_NEWS_2,
    SUPERFORECASTER_WITH_NEWS_3,
)

ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT = tuple(
    variant
    for variant in ALL_FORECAST_VARIANTS
    if variant not in ALL_FORECAST_VARIANTS_WITH_CONTEXT
)

# Variants in one group produce separate forecast files but share dataset
# forecasts because their dataset prompts are identical. If scratchpad variants
# become active again, scratchpad and scratchpad-with-freeze-values belong in
# their own group for the same reason.
DATASET_FORECAST_SHARING_VARIANT_GROUPS = (
    DatasetForecastSharingVariantGroup(
        dataset_prompt_variant=ZERO_SHOT,
        output_variants=(
            ZERO_SHOT,
            ZERO_SHOT_WITH_FREEZE_VALUES,
        ),
    ),
)
FORECAST_VARIANTS_BY_KEY = {variant.key: variant for variant in FORECAST_VARIANTS}
KNOWN_FORECAST_VARIANTS_BY_KEY = {variant.key: variant for variant in ALL_FORECAST_VARIANTS}
SUPPORTED_FORECAST_VARIANT_KEYS = frozenset(KNOWN_FORECAST_VARIANTS_BY_KEY)
ALL_FORECAST_VARIANT_KEYS_WITH_CONTEXT = frozenset(
    variant.key for variant in ALL_FORECAST_VARIANTS_WITH_CONTEXT
)
ALL_FORECAST_VARIANT_KEYS_WITHOUT_CONTEXT = frozenset(
    variant.key for variant in ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT
)


def get_variant(key: str) -> ForecastVariant:
    """Return an active forecast variant by key."""
    try:
        return FORECAST_VARIANTS_BY_KEY[key]
    except KeyError as exc:
        raise KeyError(f"Unknown ForecastBench LLM forecast variant: {key}") from exc


def get_known_variant(key: str) -> ForecastVariant:
    """Return an active or inactive forecast variant by key."""
    try:
        return KNOWN_FORECAST_VARIANTS_BY_KEY[key]
    except KeyError as exc:
        raise KeyError(f"Unknown ForecastBench LLM forecast variant: {key}") from exc
