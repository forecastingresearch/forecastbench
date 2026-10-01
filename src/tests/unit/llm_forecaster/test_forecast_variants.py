from llm_forecaster import forecast_variants

EXPECTED_FORECAST_VARIANT_DECLARATIONS = (
    {
        "name": "ZERO_SHOT",
        "key": "zero-shot",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "ZERO_SHOT_WITH_FREEZE_VALUES",
        "key": "zero-shot-with-freeze-values",
        "market_prompt_uses_freeze_values": True,
        "active": True,
    },
    {
        "name": "SCRATCHPAD",
        "key": "scratchpad",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "SCRATCHPAD_WITH_FREEZE_VALUES",
        "key": "scratchpad-with-freeze-values",
        "market_prompt_uses_freeze_values": True,
        "active": False,
    },
    {
        "name": "SCRATCHPAD_WITH_NEWS",
        "key": "scratchpad-with-news",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES",
        "key": "scratchpad-with-news-with-freeze-values",
        "market_prompt_uses_freeze_values": True,
        "active": False,
    },
    {
        "name": "SCRATCHPAD_WITH_SECOND_NEWS",
        "key": "scratchpad-with-second-news",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "SUPERFORECASTER_WITH_NEWS_1",
        "key": "superforecaster-with-news-1",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "SUPERFORECASTER_WITH_NEWS_2",
        "key": "superforecaster-with-news-2",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
    {
        "name": "SUPERFORECASTER_WITH_NEWS_3",
        "key": "superforecaster-with-news-3",
        "market_prompt_uses_freeze_values": False,
        "active": False,
    },
)


def _forecast_variant_declarations() -> tuple[dict[str, str | bool], ...]:
    return tuple(
        {
            "name": name,
            "key": variant.key,
            "market_prompt_uses_freeze_values": variant.market_prompt_uses_freeze_values,
            "active": variant.active,
        }
        for name, variant in vars(forecast_variants).items()
        if isinstance(variant, forecast_variants.ForecastVariant)
    )


def test_forecast_variant_registry_is_explicit_and_stable():
    assert _forecast_variant_declarations() == EXPECTED_FORECAST_VARIANT_DECLARATIONS, (
        "existing forecast variants should not be modified. "
        "Add new variants as new permanent entries; mark old variants inactive "
        "instead of deleting, renaming, or changing existing keys."
    )
    expected_registry = tuple(
        getattr(forecast_variants, variant["name"])
        for variant in EXPECTED_FORECAST_VARIANT_DECLARATIONS
    )
    assert forecast_variants.ALL_FORECAST_VARIANTS == expected_registry, (
        "existing forecast variants should not be modified. "
        "ALL_FORECAST_VARIANTS must contain every permanent forecast variant "
        "in declaration order."
    )


def test_only_zero_shot_with_freeze_values_is_active():
    assert [variant.key for variant in forecast_variants.FORECAST_VARIANTS] == [
        "zero-shot-with-freeze-values",
    ]


def test_context_variant_groups_partition_all_forecast_variants():
    assert forecast_variants.ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT == (
        forecast_variants.ZERO_SHOT,
        forecast_variants.SCRATCHPAD,
    )
    assert set(forecast_variants.ALL_FORECAST_VARIANTS_WITH_CONTEXT).isdisjoint(
        forecast_variants.ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT
    )
    assert (
        set(forecast_variants.ALL_FORECAST_VARIANTS_WITH_CONTEXT)
        | set(forecast_variants.ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT)
    ) == set(forecast_variants.ALL_FORECAST_VARIANTS)
    assert forecast_variants.ALL_FORECAST_VARIANT_KEYS_WITH_CONTEXT == frozenset(
        variant.key for variant in forecast_variants.ALL_FORECAST_VARIANTS_WITH_CONTEXT
    )
    assert forecast_variants.ALL_FORECAST_VARIANT_KEYS_WITHOUT_CONTEXT == frozenset(
        variant.key for variant in forecast_variants.ALL_FORECAST_VARIANTS_WITHOUT_CONTEXT
    )


def test_legacy_variants_are_known_but_inactive():
    assert forecast_variants.SCRATCHPAD.active is False
    assert forecast_variants.SCRATCHPAD_WITH_FREEZE_VALUES.active is False
    assert forecast_variants.SCRATCHPAD_WITH_NEWS.active is False
    assert forecast_variants.SCRATCHPAD_WITH_NEWS_WITH_FREEZE_VALUES.active is False
    assert forecast_variants.SCRATCHPAD_WITH_SECOND_NEWS.active is False
    assert forecast_variants.SUPERFORECASTER_WITH_NEWS_1.active is False
    assert forecast_variants.SUPERFORECASTER_WITH_NEWS_2.active is False
    assert forecast_variants.SUPERFORECASTER_WITH_NEWS_3.active is False
    assert forecast_variants.KNOWN_FORECAST_VARIANTS_BY_KEY["scratchpad"] is (
        forecast_variants.SCRATCHPAD
    )


def test_get_variant_rejects_inactive_legacy_variant():
    try:
        forecast_variants.get_variant("scratchpad")
    except KeyError as exc:
        assert "scratchpad" in str(exc)
    else:
        raise AssertionError("expected KeyError")


def test_get_known_variant_returns_inactive_legacy_variant():
    assert forecast_variants.get_known_variant("scratchpad") is forecast_variants.SCRATCHPAD
