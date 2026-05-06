"""Tests for strict ForecastBench LLM identities."""

import importlib
import os
import sys
import types
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]


@contextmanager
def _patched_import_environment():
    stubs = {
        "pyfixest": types.SimpleNamespace(),
        "jinja2": types.SimpleNamespace(Template=object),
        "joblib": types.SimpleNamespace(Parallel=object, delayed=lambda fn: fn),
        "scipy": types.SimpleNamespace(),
        "scipy.stats": types.SimpleNamespace(norm=object()),
        "statsmodels": types.SimpleNamespace(),
        "statsmodels.stats": types.SimpleNamespace(),
        "statsmodels.stats.multitest": types.SimpleNamespace(
            multipletests=lambda *args, **kwargs: None
        ),
        "termcolor": types.SimpleNamespace(colored=lambda text, *args, **kwargs: text),
        "git": types.SimpleNamespace(
            Actor=object,
            Repo=object,
        ),
        "helpers.git": types.SimpleNamespace(),
        "helpers.slack": types.SimpleNamespace(),
    }
    previous_modules = {name: sys.modules.get(name) for name in stubs}
    previous_parent_attrs = {}
    for name in stubs:
        if "." not in name:
            continue
        parent_name, attr = name.rsplit(".", maxsplit=1)
        parent = sys.modules.get(parent_name)
        if parent is not None:
            previous_parent_attrs[(parent, attr)] = (
                hasattr(parent, attr),
                getattr(parent, attr, None),
            )
    previous_leaderboard_main = sys.modules.pop("leaderboard.main", None)
    previous_top_level_llm_identities = sys.modules.pop("llm_identities", None)
    previous_cwd = Path.cwd()
    try:
        sys.modules.update(stubs)
        os.chdir(ROOT / "src" / "leaderboard")
        yield
    finally:
        os.chdir(previous_cwd)
        sys.modules.pop("leaderboard.main", None)
        if previous_leaderboard_main is not None:
            sys.modules["leaderboard.main"] = previous_leaderboard_main
        sys.modules.pop("llm_identities", None)
        if previous_top_level_llm_identities is not None:
            sys.modules["llm_identities"] = previous_top_level_llm_identities
        for name, previous in previous_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
        for (parent, attr), (had_attr, previous_attr) in previous_parent_attrs.items():
            if had_attr:
                setattr(parent, attr, previous_attr)
            elif hasattr(parent, attr):
                delattr(parent, attr)


def _import_leaderboard_main():
    with _patched_import_environment():
        return importlib.import_module("leaderboard.main")


CANONICAL_FORECASTBENCH_LLM = {
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
CANONICAL_FORECASTBENCH_LLM_WITH_FREEZE_VALUES = {
    "organization": "ForecastBench",
    "model": "claude-opus-4-7-1024-zero-shot-with-freeze-values",
    "model_organization": "Anthropic",
    "model_key": "claude-opus-4-7",
    "model_run_key": "claude-opus-4-7-run-variant-01",
    "model_run_slug": "claude-opus-4-7-1024",
    "forecast_variant_key": "zero-shot-with-freeze-values",
    "uses_freeze_values": True,
    "uses_tools": False,
}


def test_current_canonical_forecastbench_llm_identity_normalizes_with_keys():
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        **CANONICAL_FORECASTBENCH_LLM,
        "extra": {"nested": ["value"]},
    }

    normalized = normalize_llm_identity(original)

    assert normalized == {
        **original,
        "model": "claude-opus-4-7-1024",
        "uses_freeze_values": False,
        "uses_tools": False,
    }
    assert normalized is not original
    assert normalized["extra"] is not original["extra"]


def test_leaderboard_org_logo_lookup_uses_shared_lab_and_provider_names():
    from utils.llm.lab_registry import LABS
    from utils.llm.provider_registry import PROVIDERS

    main = _import_leaderboard_main()

    assert main.get_org_logo(LABS["MiniMax"].name) == "minimax.svg"
    assert main.get_org_logo(LABS["Moonshot"].name) == "moonshot.svg"
    assert main.get_org_logo(LABS["Google DeepMind"].name) == "deepmind.svg"
    assert main.get_org_logo(PROVIDERS["Google"].name) == "deepmind.svg"


def test_leaderboard_org_logo_lookup_keeps_legacy_and_external_names():
    main = _import_leaderboard_main()

    assert main.get_org_logo("Moonshot") == "moonshot.svg"
    assert main.get_org_logo("Minimax") == "minimax.svg"
    assert main.get_org_logo("Mistral") == "mistral.svg"
    assert main.get_org_logo("Cassi-AI") == "cassi-ai.png"
    assert main.get_org_logo("anonymous 4") == "anonymous_4.svg"
    assert main.get_org_logo("Unknown Org") == "default.svg"


def test_legacy_variant_metadata_tracks_freeze_values_not_tools():
    from leaderboard import llm_identities
    from llm_forecaster import forecast_variants

    options = llm_identities.LEGACY_FORECAST_VARIANTS_BY_LABEL
    assert not hasattr(forecast_variants, "LEGACY_FORECAST_VARIANTS_BY_LABEL")
    assert not hasattr(llm_identities, "_LEGACY_LLM_VARIANT_OPTIONS")
    assert not hasattr(llm_identities, "_LegacyVariantOptions")
    assert not hasattr(llm_identities, "ForecastVariantMetadata")
    assert not hasattr(llm_identities, "_display_name_for_variant")
    assert options["zero shot"] is forecast_variants.ZERO_SHOT
    assert options["zero shot with freeze values"] is (
        forecast_variants.ZERO_SHOT_WITH_FREEZE_VALUES
    )
    assert options["scratchpad"] is forecast_variants.SCRATCHPAD
    assert options["scratchpad with web search"] is forecast_variants.SCRATCHPAD

    for variant in (
        "scratchpad with freeze values",
        "scratchpad with freeze values with web search",
        "scratchpad with news with freeze values",
        "zero shot with freeze values",
        "zero shot with freeze values with web search",
    ):
        assert options[variant].market_prompt_uses_freeze_values is True

    assert not hasattr(options["zero shot"], "uses_tools")
    assert not hasattr(options["zero shot with web search"], "uses_tools")
    assert options["scratchpad"].active is False
    assert options["zero shot"].active is True


def test_uses_tools_is_derived_from_model_run_options():
    from utils.llm import model_runs

    from leaderboard import llm_identities
    from llm_forecaster.forecast_variants import ForecastVariant

    tool_run = model_runs.get_model_run("gpt-5.4-2026-03-05-run-variant-03")
    no_tool_run = model_runs.get_model_run("gpt-5.4-mini-2026-03-17-run-variant-01")

    tool_identity = llm_identities.ForecastBenchLLMIdentity(
        model_run=tool_run,
        forecast_variant=ForecastVariant(
            key="zero-shot",
            market_prompt_uses_freeze_values=False,
        ),
    )
    no_tool_identity = llm_identities.ForecastBenchLLMIdentity(
        model_run=no_tool_run,
        forecast_variant=ForecastVariant(
            key="zero-shot",
            market_prompt_uses_freeze_values=False,
        ),
    )

    assert tool_identity.uses_tools is True
    assert no_tool_identity.uses_tools is False
    assert tool_identity.as_normalized_fields()["uses_tools"] is True
    assert no_tool_identity.as_normalized_fields()["uses_tools"] is False


def test_legacy_model_run_map_is_generated_from_base_defaults_with_exact_provider_tool_rows():
    from leaderboard import llm_identities

    assert (
        "Anthropic",
        "Claude-2.1",
    ) in llm_identities._LEGACY_LLM_BASE_MODEL_RUNS

    default_rows = llm_identities._build_default_legacy_llm_model_runs()
    assert (
        default_rows[("Anthropic", "Claude-2.1 (zero shot)")][0].model_run_key
        == "claude-2.1-run-variant-01"
    )
    assert (
        "OpenAI",
        "GPT-4o-2024-11-20 (zero shot with web search)",
    ) not in default_rows
    assert (
        llm_identities._LEGACY_LLM_MODEL_RUNS[
            (
                "OpenAI",
                "GPT-4o-2024-11-20 (zero shot with web search)",
            )
        ][0].model_run_key
        == "gpt-4o-2024-11-20-run-variant-02"
    )


def test_model_run_identity_construction_is_inlined():
    from leaderboard import llm_identities

    assert not hasattr(llm_identities, "_identity_for_model_run")


def test_legacy_llm_identity_map_only_collapses_expected_model_aliases():
    from collections import defaultdict

    from leaderboard import llm_identities

    legacy_identities_by_target = defaultdict(list)
    for legacy_identity, identity in llm_identities.LEGACY_LLM_IDENTITY_MAP.items():
        target = (identity.model_run_key, identity.forecast_variant_key)
        legacy_identities_by_target[target].append(legacy_identity)

    collisions = {
        target: sorted(legacy_identities)
        for target, legacy_identities in legacy_identities_by_target.items()
        if len(legacy_identities) > 1
    }
    expected_alias_collisions = {}
    for variant_label, forecast_variant in llm_identities.LEGACY_FORECAST_VARIANTS_BY_LABEL.items():
        if variant_label in llm_identities._LEGACY_PROVIDER_TOOL_VARIANT_LABELS:
            continue
        expected_alias_collisions[("gpt-4o-2024-05-13-run-variant-01", forecast_variant.key)] = [
            ("OpenAI", f"GPT-4o ({variant_label})"),
            ("OpenAI", f"GPT-4o-2024-05-13 ({variant_label})"),
        ]

    assert collisions == expected_alias_collisions


def test_current_identity_lookup_is_not_built_for_keyless_current_rows():
    from leaderboard import llm_identities

    assert not hasattr(llm_identities, "identity_for_forecastbench_llm")
    assert not hasattr(llm_identities, "_build_current_llm_identity_map")
    assert not hasattr(llm_identities, "CURRENT_LLM_IDENTITY_MAP")
    with pytest.raises(KeyError, match="Unmapped ForecastBench LLM model"):
        llm_identities.normalize_llm_identity(
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
            }
        )


def test_forecastbench_llm_identity_stores_model_run_object():
    from utils.llm import model_runs

    from leaderboard import llm_identities
    from llm_forecaster.forecast_variants import ForecastVariant

    model_run = model_runs.get_model_run("claude-opus-4-7-run-variant-01")
    forecast_variant = ForecastVariant(
        key="zero-shot",
        market_prompt_uses_freeze_values=False,
    )

    identity = llm_identities.ForecastBenchLLMIdentity(
        model_run=model_run,
        forecast_variant=forecast_variant,
    )

    assert identity.model_run is model_run
    assert identity.forecast_variant is forecast_variant
    assert identity.model_run_key == "claude-opus-4-7-run-variant-01"
    assert identity.model_run_slug == "claude-opus-4-7-1024"
    assert identity.display_name == "claude-opus-4-7-1024"
    assert not hasattr(identity, "model")
    assert identity.as_normalized_fields()["model_run_key"] == identity.model_run_key
    assert identity.as_normalized_fields()["model_run_slug"] == identity.model_run_slug


def test_forecastbench_llm_identity_exports_variant_semantics():
    from utils.llm import model_runs

    from leaderboard import llm_identities
    from llm_forecaster.forecast_variants import ForecastVariant

    model_run = model_runs.get_model_run("claude-opus-4-7-run-variant-01")
    forecast_variant = ForecastVariant(
        key="zero-shot-with-freeze-values",
        market_prompt_uses_freeze_values=True,
    )

    identity = llm_identities.ForecastBenchLLMIdentity(
        model_run=model_run,
        forecast_variant=forecast_variant,
    )

    assert identity.model_run is model_run
    assert identity.forecast_variant is forecast_variant
    assert identity.forecast_variant_key == "zero-shot-with-freeze-values"
    assert identity.model_run_key == "claude-opus-4-7-run-variant-01"
    assert identity.model_key == "claude-opus-4-7"
    assert identity.display_name == "claude-opus-4-7-1024†"
    assert identity.uses_freeze_values is True
    assert identity.as_normalized_fields() == {
        "model": "claude-opus-4-7-1024†",
        "model_organization": "Anthropic",
        "model_key": "claude-opus-4-7",
        "model_run_key": "claude-opus-4-7-run-variant-01",
        "model_run_slug": "claude-opus-4-7-1024",
        "forecast_variant_key": "zero-shot-with-freeze-values",
        "uses_freeze_values": True,
        "uses_tools": False,
    }


def test_legacy_zero_shot_with_freeze_values_identity_normalizes_with_keys():
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        "organization": "ForecastBench",
        "model": "Claude-Opus-4-7 (zero shot with freeze values)",
        "model_organization": "Anthropic",
    }

    normalized = normalize_llm_identity(original)

    assert normalized == {
        "organization": "ForecastBench",
        "model": "claude-opus-4-7-1024†",
        "model_organization": "Anthropic",
        "model_key": "claude-opus-4-7",
        "model_run_key": "claude-opus-4-7-run-variant-01",
        "model_run_slug": "claude-opus-4-7-1024",
        "forecast_variant_key": "zero-shot-with-freeze-values",
        "uses_freeze_values": True,
        "uses_tools": False,
    }
    assert normalized is not original


@pytest.mark.parametrize(
    ("legacy_model", "expected_forecast_variant_key"),
    [
        ("Grok-4.3 (zero shot)", "zero-shot"),
        ("Grok-4.3 (zero shot with freeze values)", "zero-shot-with-freeze-values"),
    ],
)
def test_legacy_grok_4_3_identity_normalizes_with_keys(
    legacy_model,
    expected_forecast_variant_key,
):
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        "organization": "ForecastBench",
        "model": legacy_model,
        "model_organization": "xAI",
    }

    expected_model = "grok-4.3"
    if expected_forecast_variant_key == "zero-shot-with-freeze-values":
        expected_model = "grok-4.3†"

    uses_freeze_values = expected_forecast_variant_key == "zero-shot-with-freeze-values"
    assert normalize_llm_identity(original) == {
        "organization": "ForecastBench",
        "model": expected_model,
        "model_organization": "xAI",
        "model_key": "grok-4.3",
        "model_run_key": "grok-4.3-run-variant-01",
        "model_run_slug": "grok-4.3",
        "forecast_variant_key": expected_forecast_variant_key,
        "uses_freeze_values": uses_freeze_values,
        "uses_tools": False,
    }


def test_legacy_minimax_m2_7_identity_preserves_original_variant():
    from leaderboard.llm_identities import normalize_llm_identity

    normalized = normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "MiniMax-M2.7 (zero shot)",
            "model_organization": "Minimax",
        }
    )

    assert normalized == {
        "organization": "ForecastBench",
        "model": "minimax-m2.7",
        "model_organization": "MiniMax",
        "model_key": "minimax-m2.7",
        "model_run_key": "minimax-m2.7-run-variant-01",
        "model_run_slug": "minimax-m2.7",
        "forecast_variant_key": "zero-shot",
        "uses_freeze_values": False,
        "uses_tools": False,
    }


def test_legacy_news_prompt_identity_exports_variant_key_without_extra_context_column():
    from leaderboard.llm_identities import normalize_llm_identity

    normalized = normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (scratchpad with news)",
            "model_organization": "Anthropic",
        }
    )

    assert normalized["forecast_variant_key"] == "scratchpad-with-news"
    assert normalized["uses_freeze_values"] is False
    assert normalized["uses_tools"] is False
    assert "uses_extra_context" not in normalized


def test_new_file_identity_uses_explicit_keys_for_display_and_semantics():
    from leaderboard.llm_identities import normalize_llm_identity

    normalized = normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "new-file-display-name",
            "model_organization": "Anthropic",
            "model_run_key": "claude-opus-4-7-run-variant-01",
            "forecast_variant_key": "zero-shot-with-freeze-values",
        }
    )

    assert normalized == {
        "organization": "ForecastBench",
        "model": "claude-opus-4-7-1024†",
        "model_organization": "Anthropic",
        "model_key": "claude-opus-4-7",
        "model_run_key": "claude-opus-4-7-run-variant-01",
        "model_run_slug": "claude-opus-4-7-1024",
        "forecast_variant_key": "zero-shot-with-freeze-values",
        "uses_freeze_values": True,
        "uses_tools": False,
    }


def test_leaderboard_filters_use_precomputed_selection_flags():
    main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                **CANONICAL_FORECASTBENCH_LLM,
                "model": "flagged-for-tournament",
                "baseline_model": False,
                "tournament_model": True,
            },
            {
                **CANONICAL_FORECASTBENCH_LLM_WITH_FREEZE_VALUES,
                "model": "flagged-for-baseline",
                "baseline_model": True,
                "tournament_model": False,
            },
        ]
    )

    assert main.filter_to_baseline_leaderboard_models(df)["model"].tolist() == [
        "flagged-for-baseline"
    ]
    assert main.filter_to_tournament_leaderboard_models(df)["model"].tolist() == [
        "flagged-for-tournament"
    ]


def test_baseline_filter_keeps_baseline_llm_variants_and_drops_tournament_variants():
    main = _import_leaderboard_main()
    normalize = main.llm_identities.normalize_llm_identity
    df = pd.DataFrame(
        [
            CANONICAL_FORECASTBENCH_LLM,
            CANONICAL_FORECASTBENCH_LLM_WITH_FREEZE_VALUES,
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "Claude-Opus-4-7 (zero shot)",
                    "model_organization": "Anthropic",
                }
            ),
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "Claude-Opus-4-7 (scratchpad)",
                    "model_organization": "Anthropic",
                }
            ),
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "Claude-Opus-4-7 (scratchpad with freeze values)",
                    "model_organization": "Anthropic",
                }
            ),
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "GPT-4o-2024-11-20 (zero shot with web search)",
                    "model_organization": "OpenAI",
                }
            ),
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024-zero-shot-external-context",
                "model_organization": "Anthropic",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "forecast_variant_key": "zero-shot",
                "uses_freeze_values": False,
                "uses_tools": True,
            },
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "new-file-display-name",
                    "model_organization": "OpenAI",
                    "model_run_key": "gpt-4o-2024-11-20-run-variant-02",
                    "forecast_variant_key": "zero-shot",
                }
            ),
            {
                "organization": "External Team",
                "model": "external-model",
                "model_organization": "External Lab",
            },
        ]
    )
    df["baseline_model"] = [
        True,
        False,
        True,
        True,
        False,
        False,
        False,
        False,
        False,
    ]
    df["tournament_model"] = [
        False,
        True,
        False,
        False,
        True,
        True,
        True,
        True,
        True,
    ]

    filtered = main.filter_to_baseline_leaderboard_models(df)

    assert filtered["model"].tolist() == [
        "claude-opus-4-7-1024",
        "claude-opus-4-7-1024",
        "claude-opus-4-7-1024-scratchpad",
    ]


def test_tournament_filter_keeps_tournament_llm_variants_and_drops_baseline_variants():
    main = _import_leaderboard_main()
    normalize = main.llm_identities.normalize_llm_identity
    df = pd.DataFrame(
        [
            CANONICAL_FORECASTBENCH_LLM,
            CANONICAL_FORECASTBENCH_LLM_WITH_FREEZE_VALUES,
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "Claude-Opus-4-7 (zero shot)",
                    "model_organization": "Anthropic",
                }
            ),
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "GPT-4o-2024-11-20 (zero shot with web search)",
                    "model_organization": "OpenAI",
                }
            ),
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024-zero-shot-external-context",
                "model_organization": "Anthropic",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "forecast_variant_key": "zero-shot",
                "uses_freeze_values": False,
                "uses_tools": True,
            },
            normalize(
                {
                    "organization": "ForecastBench",
                    "model": "new-file-display-name",
                    "model_organization": "OpenAI",
                    "model_run_key": "gpt-4o-2024-11-20-run-variant-02",
                    "forecast_variant_key": "zero-shot",
                }
            ),
            {
                "organization": "External Team",
                "model": "external-model",
                "model_organization": "External Lab",
            },
        ]
    )
    df["baseline_model"] = [
        True,
        False,
        True,
        True,
        True,
        False,
        False,
    ]
    df["tournament_model"] = [
        False,
        True,
        False,
        True,
        True,
        True,
        True,
    ]

    filtered = main.filter_to_tournament_leaderboard_models(df)

    assert filtered["model"].tolist() == [
        "claude-opus-4-7-1024-zero-shot-with-freeze-values",
        "gpt-4o-2024-11-20-web-search",
        "claude-opus-4-7-1024-zero-shot-external-context",
        "gpt-4o-2024-11-20-web-search",
        "external-model",
    ]


def test_leaderboard_filters_require_selection_flag_columns():
    main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "Claude-Opus-4-7 (zero shot)",
                "model_organization": "Anthropic",
            }
        ]
    )

    with pytest.raises(KeyError, match="baseline_model"):
        main.filter_to_baseline_leaderboard_models(df)


def test_preliminary_leaderboard_filters_to_tournament_models_before_scoring(monkeypatch):
    main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model_organization": "Anthropic",
                "model": "baseline-llm",
                "model_pk": "baseline-llm",
                "baseline_model": True,
                "tournament_model": False,
            },
            {
                "organization": "ForecastBench",
                "model_organization": "Anthropic",
                "model": "tournament-llm",
                "model_pk": "tournament-llm",
                "baseline_model": False,
                "tournament_model": True,
            },
        ]
    )
    captured_model_pks = []

    monkeypatch.setattr(main, "combine_forecasting_rounds", lambda entries: df.copy())
    monkeypatch.setattr(main, "get_model_release_date_info", lambda df, **kwargs: df)

    def fake_score_models(df, **kwargs):
        captured_model_pks.append(("score", df["model_pk"].tolist()))
        return df.copy(), {}

    def fake_generate_simulated_leaderboards(df, **kwargs):
        captured_model_pks.append(("bootstrap", df["model_pk"].tolist()))
        simulated = pd.DataFrame(index=df["model_pk"].drop_duplicates())
        return simulated, simulated, simulated

    monkeypatch.setattr(main, "score_models", fake_score_models)
    monkeypatch.setattr(
        main, "generate_simulated_leaderboards", fake_generate_simulated_leaderboards
    )
    monkeypatch.setattr(
        main, "get_confidence_interval", lambda df_leaderboard, **kwargs: df_leaderboard
    )
    monkeypatch.setattr(
        main, "get_comparison_p_val", lambda df_leaderboard, **kwargs: df_leaderboard
    )
    monkeypatch.setattr(
        main,
        "get_simulation_performance_metrics",
        lambda df_leaderboard, **kwargs: df_leaderboard,
    )
    monkeypatch.setattr(main, "write_preliminary_leaderboard", lambda **kwargs: None)

    main.make_preliminary_leaderboard(leaderboard_entries=[])

    assert captured_model_pks == [
        ("score", ["tournament-llm"]),
        ("bootstrap", ["tournament-llm"]),
    ]


def test_two_way_fixed_effects_excludes_external_submission_flag_from_fit(monkeypatch):
    main = _import_leaderboard_main()
    fitted_model_pks = []
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model_organization": "Anthropic",
                "model": "included-llm",
                "model_pk": "included-llm",
                "question_pk": "question-1",
                "brier_score": 0.25,
                "model_age_at_due_date": 1,
                "external_submission": False,
            },
            {
                "organization": "ForecastBench",
                "model_organization": "Anthropic",
                "model": "flagged-external",
                "model_pk": "flagged-external",
                "question_pk": "question-1",
                "brier_score": 0.20,
                "model_age_at_due_date": 1,
                "external_submission": True,
            },
        ]
    )

    class FakeFixedEffectsModel:
        def fixef(self):
            return {"C(question_pk)": {"question-1": 0.10}}

    def fake_feols(_formula, data):
        fitted_model_pks.extend(data["model_pk"].tolist())
        return FakeFixedEffectsModel()

    monkeypatch.setattr(main.pf, "feols", fake_feols, raising=False)

    result = main.two_way_fixed_effects(
        df=df,
        question_type="dataset",
        market_question_adjustment=main.MarketQuestionAdjustment.TWO_WAY_FIXED_EFFECTS,
    )

    assert fitted_model_pks == ["included-llm"]
    assert result["model_pk"].tolist() == ["included-llm", "flagged-external"]


def test_two_way_fixed_effects_excludes_llm_crowd_comparison_models_from_fit(monkeypatch):
    main = _import_leaderboard_main()
    fitted_model_pks = []
    crowd_models = [
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) "
        "geometric mean log odds with news",
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) geometric mean with news",
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) median with news",
    ]
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model_organization": "Anthropic",
                "model": "included-llm",
                "model_pk": "included-llm",
                "question_pk": "question-1",
                "brier_score": 0.25,
                "model_age_at_due_date": 1,
                "external_submission": False,
            },
            *[
                {
                    "organization": "ForecastBench",
                    "model_organization": "ForecastBench",
                    "model": model,
                    "model_pk": model,
                    "question_pk": "question-1",
                    "brier_score": 0.20,
                    "model_age_at_due_date": pd.NA,
                    "external_submission": False,
                }
                for model in crowd_models
            ],
        ]
    )

    class FakeFixedEffectsModel:
        def fixef(self):
            return {"C(question_pk)": {"question-1": 0.10}}

    def fake_feols(_formula, data):
        fitted_model_pks.extend(data["model_pk"].tolist())
        return FakeFixedEffectsModel()

    monkeypatch.setattr(main.pf, "feols", fake_feols, raising=False)

    result = main.two_way_fixed_effects(
        df=df,
        question_type="dataset",
        market_question_adjustment=main.MarketQuestionAdjustment.TWO_WAY_FIXED_EFFECTS,
    )

    assert fitted_model_pks == ["included-llm"]
    assert result["model_pk"].tolist() == ["included-llm", *crowd_models]


def test_unmapped_forecastbench_llm_legacy_variant_raises_key_error():
    from leaderboard.llm_identities import normalize_llm_identity

    with pytest.raises(
        KeyError,
        match="Unmapped ForecastBench LLM model",
    ):
        normalize_llm_identity(
            {
                "organization": "ForecastBench",
                "model": "Unmapped Model (zero shot)",
                "model_organization": "Unknown Lab",
            }
        )


def test_non_exact_forecastbench_llm_parenthetical_variant_raises_key_error():
    from leaderboard.llm_identities import normalize_llm_identity

    with pytest.raises(KeyError, match="Unmapped ForecastBench LLM model"):
        normalize_llm_identity(
            {
                "organization": "ForecastBench",
                "model": "Unmapped Model (zero shot but not a real variant)",
                "model_organization": "Unknown Lab",
            }
        )


def test_non_forecastbench_identity_passes_through_unchanged():
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        "organization": "External Team",
        "model": "Claude-Opus-4-7 (zero shot with freeze values)",
        "model_organization": "Anthropic",
    }

    assert normalize_llm_identity(original) == original


@pytest.mark.parametrize(
    "legacy_model",
    [
        "Claude-Opus-4-7 (scratchpad)",
        "Claude-Opus-4-7 (scratchpad with freeze values)",
    ],
)
def test_scratchpad_legacy_variants_normalize_with_keys(legacy_model):
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        "organization": "ForecastBench",
        "model": legacy_model,
        "model_organization": "Anthropic",
    }

    normalized = normalize_llm_identity(original)

    expected_suffix = normalized["forecast_variant_key"]
    assert normalized["model_run_key"] == "claude-opus-4-7-run-variant-01"
    assert normalized["model_run_slug"] == "claude-opus-4-7-1024"
    assert normalized["forecast_variant_key"] in {
        "scratchpad",
        "scratchpad-with-freeze-values",
    }
    assert normalized["model"] == f"claude-opus-4-7-1024-{expected_suffix}"


def test_legacy_openai_web_search_identity_normalizes_to_web_search_model_run():
    from leaderboard.llm_identities import normalize_llm_identity

    original = {
        "organization": "ForecastBench",
        "model": "GPT-4o-2024-11-20 (scratchpad with freeze values with web search)",
        "model_organization": "OpenAI",
    }

    assert normalize_llm_identity(original) == {
        "organization": "ForecastBench",
        "model": "gpt-4o-2024-11-20-web-search-scratchpad-with-freeze-values",
        "model_organization": "OpenAI",
        "model_key": "gpt-4o-2024-11-20",
        "model_run_key": "gpt-4o-2024-11-20-run-variant-02",
        "model_run_slug": "gpt-4o-2024-11-20-web-search",
        "forecast_variant_key": "scratchpad-with-freeze-values",
        "uses_freeze_values": True,
        "uses_tools": True,
    }


def test_explicit_new_identity_sets_uses_tools_from_model_run_key():
    from leaderboard.llm_identities import normalize_llm_identity

    tool_run_normalized = normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "gpt-5.4-2026-03-05-high-web-search",
            "model_organization": "OpenAI",
            "model_run_key": "gpt-5.4-2026-03-05-run-variant-03",
            "forecast_variant_key": "zero-shot",
        }
    )
    no_tool_run_normalized = normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "gpt-5.4-mini-2026-03-17",
            "model_organization": "OpenAI",
            "model_run_key": "gpt-5.4-mini-2026-03-17-run-variant-01",
            "forecast_variant_key": "zero-shot",
        }
    )

    assert tool_run_normalized["uses_tools"] is True
    assert tool_run_normalized["model_key"] == "gpt-5.4-2026-03-05"
    assert tool_run_normalized["model_run_slug"] == "gpt-5.4-2026-03-05-high-web-search"
    assert no_tool_run_normalized["uses_tools"] is False
    assert no_tool_run_normalized["model_key"] == "gpt-5.4-mini-2026-03-17"
    assert no_tool_run_normalized["model_run_slug"] == "gpt-5.4-mini-2026-03-17"


def test_leaderboard_integration_uses_model_run_and_forecast_variant_for_model_pk():
    leaderboard_main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    org_and_model = leaderboard_main.llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (zero shot with freeze values)",
            "model_organization": "Anthropic",
        }
    )

    processed = leaderboard_main.get_df_info(
        df=df,
        org_and_model=org_and_model,
        forecast_due_date="2026-05-07",
    )

    assert processed is not None
    assert processed["model"].unique().tolist() == ["claude-opus-4-7-1024†"]
    assert processed["model_key"].unique().tolist() == ["claude-opus-4-7"]
    assert processed["model_run_key"].unique().tolist() == ["claude-opus-4-7-run-variant-01"]
    assert processed["model_run_slug"].unique().tolist() == ["claude-opus-4-7-1024"]
    assert processed["forecast_variant_key"].unique().tolist() == ["zero-shot-with-freeze-values"]
    assert processed["uses_freeze_values"].unique().tolist() == [True]
    assert processed["uses_tools"].unique().tolist() == [False]
    assert processed["model_pk"].unique().tolist() == [
        "ForecastBench_Anthropic_claude-opus-4-7-run-variant-01_zero-shot-with-freeze-values"
    ]


def test_set_model_pk_errors_when_forecastbench_llm_identity_columns_are_missing():
    leaderboard_main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "forecastbench_llm": True,
            }
        ]
    )

    with pytest.raises(KeyError, match="model_run_key"):
        leaderboard_main.set_model_pk(df)


def test_set_model_pk_uses_forecastbench_llm_flag():
    leaderboard_main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "reference-model",
                "model_organization": "Anthropic",
                "forecastbench_llm": False,
            }
        ]
    )

    processed = leaderboard_main.set_model_pk(df)

    assert processed["model_pk"].tolist() == ["ForecastBench_Anthropic_reference-model"]


def test_get_df_info_handles_forecastbench_comparison_model_without_llm_identity():
    leaderboard_main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    processed = leaderboard_main.get_df_info(
        df=df,
        org_and_model={
            "organization": "ForecastBench",
            "model": "Always 0.5",
            "model_organization": "ForecastBench",
        },
        forecast_due_date="2026-05-07",
    )

    assert processed is not None
    assert processed["forecastbench_llm"].unique().tolist() == [False]
    assert processed["forecastbench_comparison_model"].unique().tolist() == [True]
    assert processed["baseline_model"].unique().tolist() == [True]
    assert processed["tournament_model"].unique().tolist() == [True]
    assert processed["model_pk"].unique().tolist() == ["ForecastBench_ForecastBench_Always 0.5"]


def test_get_df_info_handles_llm_crowd_comparison_models_without_llm_identity(monkeypatch):
    leaderboard_main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(leaderboard_main.slack, "send_message", messages.append, raising=False)
    comparison_models = [
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) "
        "geometric mean log odds with news",
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) geometric mean with news",
        "LLM Crowd (gpt-4o, claude-3.5-sonnet, gemini-1.5-pro) median with news",
    ]
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    for model in comparison_models:
        processed = leaderboard_main.get_df_info(
            df=df,
            org_and_model={
                "organization": "ForecastBench",
                "model": model,
                "model_organization": "ForecastBench",
            },
            forecast_due_date="2026-05-07",
        )

        assert processed is not None
        assert processed["forecastbench_llm"].unique().tolist() == [False]
        assert processed["forecastbench_comparison_model"].unique().tolist() == [True]
        assert processed["baseline_model"].unique().tolist() == [True]
        assert processed["tournament_model"].unique().tolist() == [True]
        assert processed["model_pk"].unique().tolist() == [f"ForecastBench_ForecastBench_{model}"]

    assert messages == []


def test_get_df_info_raises_and_sends_slack_for_unclassified_forecastbench_model(monkeypatch):
    leaderboard_main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(leaderboard_main.slack, "send_message", messages.append, raising=False)
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    with pytest.raises(ValueError, match="Unclassified ForecastBench Model"):
        leaderboard_main.get_df_info(
            df=df,
            org_and_model={
                "organization": "ForecastBench",
                "model": "Unclassified ForecastBench Model",
                "model_organization": "ForecastBench",
            },
            forecast_due_date="2026-05-07",
        )

    assert len(messages) == 1
    assert "Unclassified ForecastBench Model" in messages[0]


def test_get_df_info_uses_pre_normalized_llm_identity(monkeypatch):
    leaderboard_main = _import_leaderboard_main()
    org_and_model = leaderboard_main.llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (zero shot with freeze values)",
            "model_organization": "Anthropic",
        }
    )
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("identity should already be normalized")

    monkeypatch.setattr(
        leaderboard_main.llm_identities,
        "normalize_llm_identity",
        fail_if_called,
    )

    processed = leaderboard_main.get_df_info(
        df=df,
        org_and_model=org_and_model,
        forecast_due_date="2026-05-07",
    )

    assert processed is not None
    assert processed["model"].unique().tolist() == ["claude-opus-4-7-1024†"]
    assert processed["model_key"].unique().tolist() == ["claude-opus-4-7"]
    assert processed["model_run_key"].unique().tolist() == ["claude-opus-4-7-run-variant-01"]
    assert processed["forecast_variant_key"].unique().tolist() == ["zero-shot-with-freeze-values"]


def test_legacy_and_new_model_run_identity_share_model_pk():
    leaderboard_main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "id": "fred-question",
                "source": "fred",
                "resolved": True,
                "resolution_date": "2026-05-10",
                "imputed": False,
            }
        ]
    )

    legacy_org_and_model = leaderboard_main.llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (zero shot with freeze values)",
            "model_organization": "Anthropic",
        }
    )
    current_org_and_model = leaderboard_main.llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "claude-opus-4-7-1024†",
            "model_organization": "Anthropic",
            "model_run_key": "claude-opus-4-7-run-variant-01",
            "forecast_variant_key": "zero-shot-with-freeze-values",
        }
    )

    legacy = leaderboard_main.get_df_info(
        df=df,
        org_and_model=legacy_org_and_model,
        forecast_due_date="2026-05-07",
    )
    current = leaderboard_main.get_df_info(
        df=df,
        org_and_model=current_org_and_model,
        forecast_due_date="2026-05-21",
    )

    assert legacy is not None
    assert current is not None
    assert legacy["model"].unique().tolist() == current["model"].unique().tolist()
    assert legacy["model_run_key"].unique().tolist() == current["model_run_key"].unique().tolist()
    assert (
        legacy["forecast_variant_key"].unique().tolist()
        == current["forecast_variant_key"].unique().tolist()
    )
    assert legacy["model_pk"].unique().tolist() == current["model_pk"].unique().tolist()


def test_leaderboard_deploy_stages_llm_identity_dependencies():
    makefile = (ROOT / "src" / "leaderboard" / "Makefile").read_text()

    assert "LEADERBOARD_DEPENDENCIES = main.py llm_identities.py" in makefile
    assert "deploy-tournament : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "deploy-baseline : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "deploy-preliminary : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "model_release_dates.csv" not in makefile
    assert "cp -r $(ROOT_DIR)src/llm_forecaster $1/" in makefile
    assert (
        "cat $(ROOT_DIR)requirements.runtime.txt requirements.txt > $1/requirements.txt" in makefile
    )


def test_model_release_dates_include_canonical_active_llm_model_keys():
    from utils.llm import model_registry

    from llm_forecaster.fb_model_runs import FB_MODEL_RUNS

    release_date_model_keys = set(model_registry.model_release_dates_by_key())
    active_model_keys = {model_run.model_key for model_run in FB_MODEL_RUNS}

    assert active_model_keys <= release_date_model_keys


def test_kimi_k2_6_release_date_uses_official_moonshot_date():
    from utils.llm import model_registry

    release_dates = model_registry.model_release_dates_by_key()

    assert release_dates["kimi-k2.6"] == date(2026, 4, 21)
    assert "kimi-k2.6†" not in release_dates


def test_grok_4_3_release_date_is_recorded_once_by_model_key():
    from utils.llm import model_registry

    release_dates = model_registry.model_release_dates_by_key()

    assert release_dates["grok-4.3"] == date(2026, 4, 17)
    assert "grok-4.3†" not in release_dates
