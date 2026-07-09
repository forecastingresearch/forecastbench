"""Tests for ForecastBench model identity release-date joins."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]

CLAUDE_OPUS_ZERO_SHOT_MODEL_PK = "ForecastBench_Anthropic_claude-opus-4-7-run-variant-01_zero-shot"
CLAUDE_OPUS_FREEZE_VALUES_MODEL_PK = (
    "ForecastBench_Anthropic_" "claude-opus-4-7-run-variant-01_zero-shot-with-freeze-values"
)


def _release_dates_by_model_key() -> dict[str, date]:
    from utils.llm import model_registry

    return model_registry.model_release_dates_by_key()


def test_model_release_dates_come_from_utils_not_csv():
    release_dates = _release_dates_by_model_key()

    assert release_dates["gpt-5.4-2026-03-05"] == date(2026, 3, 5)
    assert release_dates["gemini-3.1-flash-lite"] == date(2026, 5, 7)
    assert release_dates["gpt-4-0613"] == date(2023, 6, 13)


def test_forecastbench_model_release_dates_csv_is_removed():
    assert not (ROOT / "src" / "leaderboard" / "model_release_dates.csv").exists()


def test_active_llm_display_names_round_trip_to_model_runs_and_variants():
    from llm_forecaster.fb_model_runs import FB_MODEL_RUNS
    from llm_forecaster.forecast_variants import FORECAST_VARIANTS
    from llm_forecaster.output import display_model_name, parse_display_model_name

    for model_run in FB_MODEL_RUNS:
        for variant in FORECAST_VARIANTS:
            display_name = display_model_name(model_run, variant)

            parsed_model_run, parsed_variant = parse_display_model_name(display_name)

            assert parsed_model_run is model_run
            assert parsed_variant is variant


def test_active_llm_release_dates_are_keyed_by_model_key_not_display_name():
    from llm_forecaster.fb_model_runs import get_model_run_by_slug
    from llm_forecaster.forecast_variants import FORECAST_VARIANTS
    from llm_forecaster.output import display_model_name

    run = get_model_run_by_slug("claude-opus-4-7-1024")
    release_dates = _release_dates_by_model_key()
    display_names = {display_model_name(run, variant) for variant in FORECAST_VARIANTS}

    assert display_names == {
        "claude-opus-4-7-1024",
        "claude-opus-4-7-1024†",
    }
    assert run.model_key == "claude-opus-4-7"
    assert release_dates["claude-opus-4-7"] == date(2026, 4, 16)
    assert "claude-opus-4-7-1024" not in release_dates
    assert "claude-opus-4-7-1024†" not in release_dates


def test_normalized_legacy_names_use_model_run_key_for_release_dates():
    from leaderboard import llm_identities

    release_dates = _release_dates_by_model_key()

    legacy_zero_shot = llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "GPT-4-0613 (zero shot)",
            "model_organization": "OpenAI",
        }
    )
    legacy_freeze_values = llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "GPT-4-0613 (zero shot with freeze values)",
            "model_organization": "OpenAI",
        }
    )

    assert legacy_zero_shot["model_key"] == "gpt-4-0613"
    assert legacy_freeze_values["model_key"] == "gpt-4-0613"
    assert release_dates["gpt-4-0613"] == date(2023, 6, 13)
    assert "gpt-4-0613-with-freeze-values" not in release_dates
    assert "gpt-4-0613†" not in release_dates


def test_model_key_is_returned_by_normalized_identity_not_added_later():
    from leaderboard import llm_identities

    normalized = llm_identities.normalize_llm_identity(
        {
            "organization": "ForecastBench",
            "model": "Claude-Opus-4-7 (zero shot)",
            "model_organization": "Anthropic",
        }
    )

    assert normalized["model_key"] == "claude-opus-4-7"
    assert not hasattr(llm_identities, "add_model_key")


def test_forecastbench_model_run_keys_map_raw_legacy_current_and_tool_runs():
    from leaderboard import llm_identities

    cases = [
        (
            "Anthropic",
            "Claude-2.1 (superforecaster with news 1)",
            "claude-2.1",
            "claude-2.1-run-variant-01",
            "superforecaster-with-news-1",
        ),
        (
            "OpenAI",
            "GPT-4o (zero shot)",
            "gpt-4o-2024-05-13",
            "gpt-4o-2024-05-13-run-variant-01",
            "zero-shot",
        ),
        (
            "OpenAI",
            "GPT-4o-2024-11-20 (zero shot with web search)",
            "gpt-4o-2024-11-20",
            "gpt-4o-2024-11-20-run-variant-02",
            "zero-shot",
        ),
        (
            "OpenAI",
            "GPT-4o-2024-11-20 (scratchpad with freeze values with web search)",
            "gpt-4o-2024-11-20",
            "gpt-4o-2024-11-20-run-variant-02",
            "scratchpad-with-freeze-values",
        ),
        (
            "OpenAI",
            "GPT-5.5-2026-04-23 (zero shot with freeze values)",
            "gpt-5.5-2026-04-23",
            "gpt-5.5-2026-04-23-run-variant-01",
            "zero-shot-with-freeze-values",
        ),
    ]

    for (
        model_organization,
        model,
        expected_model_key,
        expected_model_run_key,
        expected_forecast_variant_key,
    ) in cases:
        identity = {
            "organization": "ForecastBench",
            "model": model,
            "model_organization": model_organization,
        }

        normalized = llm_identities.normalize_llm_identity(identity)
        assert normalized["model_key"] == expected_model_key
        assert normalized["model_run_key"] == expected_model_run_key
        assert normalized["forecast_variant_key"] == expected_forecast_variant_key


def test_explicit_model_run_and_forecast_variant_keys_are_validated():
    from leaderboard import llm_identities

    with pytest.raises(KeyError, match="Unknown LLM model_run_key"):
        llm_identities.normalize_llm_identity(
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_run_key": "not-a-real-model-run-key",
                "forecast_variant_key": "zero-shot",
            }
        )

    with pytest.raises(KeyError, match="Unknown ForecastBench LLM forecast_variant_key"):
        llm_identities.normalize_llm_identity(
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "forecast_variant_key": "not-a-real-variant",
            }
        )


def test_leaderboard_release_date_uses_model_run_key(monkeypatch):
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(main.slack, "send_message", messages.append, raising=False)
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024†",
                "model_organization": "Anthropic",
                "model_key": "not-used-for-release-date",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "forecast_variant_key": "zero-shot-with-freeze-values",
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "forecast_due_date": "2026-05-07",
                "first_forecast_due_date": "2026-05-07",
            }
        ]
    )

    with_release_dates = main.get_model_release_date_info(
        df,
        add_model_age_at_due_date=True,
        add_model_release_date=True,
    )

    assert with_release_dates["model"].tolist() == ["claude-opus-4-7-1024†"]
    assert with_release_dates["model_release_date"].astype(str).tolist() == ["2026-04-16"]
    assert with_release_dates["model_age_at_due_date"].tolist() == [21]
    assert messages == []


def test_model_age_can_be_added_without_returning_model_release_date():
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "forecast_due_date": "2026-05-07",
                "first_forecast_due_date": "2026-05-07",
            }
        ]
    )

    with_model_age = main.get_model_release_date_info(
        df,
        add_model_age_at_due_date=True,
        add_model_release_date=False,
    )

    assert "model_release_date" not in with_model_age.columns
    assert with_model_age["model_age_at_due_date"].tolist() == [21]


def test_scored_leaderboard_preserves_llm_identity_for_release_dates(monkeypatch):
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(main.slack, "send_message", messages.append, raising=False)
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024†",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "model_run_slug": "claude-opus-4-7-1024",
                "forecast_variant_key": "zero-shot-with-freeze-values",
                "uses_freeze_values": True,
                "uses_tools": False,
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "model_pk": CLAUDE_OPUS_FREEZE_VALUES_MODEL_PK,
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "fred",
                "resolved": True,
                "forecast": 0.75,
                "resolved_to": 1,
            },
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024†",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "model_run_slug": "claude-opus-4-7-1024",
                "forecast_variant_key": "zero-shot-with-freeze-values",
                "uses_freeze_values": True,
                "uses_tools": False,
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "model_pk": CLAUDE_OPUS_FREEZE_VALUES_MODEL_PK,
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "manifold",
                "resolved": True,
                "forecast": 0.25,
                "resolved_to": 0,
            },
        ]
    )

    df_leaderboard, _ = main.score_models(
        df=df,
        scoring_funcs=[main.brier_score],
        market_question_adjustment=main.MarketQuestionAdjustment.MARKET_BRIER,
    )

    assert df_leaderboard["model_key"].tolist() == ["claude-opus-4-7"]
    assert df_leaderboard["model_run_slug"].tolist() == ["claude-opus-4-7-1024"]
    assert df_leaderboard["uses_freeze_values"].tolist() == [True]
    assert df_leaderboard["uses_tools"].tolist() == [False]
    with_release_dates = main.get_model_release_date_info(
        df_leaderboard,
        add_model_age_at_due_date=False,
        add_model_release_date=True,
    )

    assert with_release_dates["model_release_date"].astype(str).tolist() == ["2026-04-16"]
    assert messages == []


def test_score_models_keeps_reference_models_when_llm_identity_columns_exist():
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "Always 0.5",
                "model_organization": "ForecastBench",
                "external_submission": False,
                "forecastbench_llm": False,
                "forecastbench_comparison_model": True,
                "model_pk": "ForecastBench_Always 0.5",
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "fred",
                "resolved": True,
                "forecast": 0.5,
                "resolved_to": 1,
            },
            {
                "organization": "ForecastBench",
                "model": "Always 0.5",
                "model_organization": "ForecastBench",
                "external_submission": False,
                "forecastbench_llm": False,
                "forecastbench_comparison_model": True,
                "model_pk": "ForecastBench_Always 0.5",
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "manifold",
                "resolved": True,
                "forecast": 0.5,
                "resolved_to": 0,
            },
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "model_run_slug": "claude-opus-4-7-1024",
                "forecast_variant_key": "zero-shot",
                "uses_freeze_values": False,
                "uses_tools": False,
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "model_pk": CLAUDE_OPUS_ZERO_SHOT_MODEL_PK,
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "fred",
                "resolved": True,
                "forecast": 0.75,
                "resolved_to": 1,
            },
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "model_run_slug": "claude-opus-4-7-1024",
                "forecast_variant_key": "zero-shot",
                "uses_freeze_values": False,
                "uses_tools": False,
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "model_pk": CLAUDE_OPUS_ZERO_SHOT_MODEL_PK,
                "first_forecast_due_date": "2026-05-07",
                "forecast_due_date": "2026-05-07",
                "source": "manifold",
                "resolved": True,
                "forecast": 0.25,
                "resolved_to": 0,
            },
        ]
    )

    df_leaderboard, _ = main.score_models(
        df=df,
        scoring_funcs=[main.brier_score],
        market_question_adjustment=main.MarketQuestionAdjustment.MARKET_BRIER,
    )

    assert set(df_leaderboard["model"]) == {"Always 0.5", "claude-opus-4-7-1024"}


def test_release_date_info_preserves_forecastbench_created_reference_models(monkeypatch):
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(main.slack, "send_message", messages.append, raising=False)
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "Naive Forecaster",
                "model_organization": "ForecastBench",
                "external_submission": False,
                "forecastbench_llm": False,
                "forecastbench_comparison_model": True,
                "forecast_due_date": "2026-05-07",
                "first_forecast_due_date": "2026-05-07",
            },
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "claude-opus-4-7-run-variant-01",
                "forecast_variant_key": "zero-shot",
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "forecast_due_date": "2026-05-07",
                "first_forecast_due_date": "2026-05-07",
            },
        ]
    )

    with_release_dates = main.get_model_release_date_info(
        df,
        add_model_age_at_due_date=True,
        add_model_release_date=True,
    )

    assert with_release_dates["model"].tolist() == [
        "Naive Forecaster",
        "claude-opus-4-7-1024",
    ]
    naive_row = with_release_dates[with_release_dates["model"] == "Naive Forecaster"].iloc[0]
    assert pd.isna(naive_row["model_release_date"])
    assert pd.isna(naive_row["model_age_at_due_date"])
    assert messages == []


def test_release_date_info_errors_when_forecastbench_llm_model_run_key_is_unknown(monkeypatch):
    from tests.leaderboard.test_llm_identities import _import_leaderboard_main

    main = _import_leaderboard_main()
    messages = []
    monkeypatch.setattr(main.slack, "send_message", messages.append, raising=False)
    df = pd.DataFrame(
        [
            {
                "organization": "ForecastBench",
                "model": "claude-opus-4-7-1024",
                "model_organization": "Anthropic",
                "model_key": "claude-opus-4-7",
                "model_run_key": "unknown-run",
                "forecast_variant_key": "zero-shot",
                "external_submission": False,
                "forecastbench_llm": True,
                "forecastbench_comparison_model": False,
                "forecast_due_date": "2026-05-07",
                "first_forecast_due_date": "2026-05-07",
            }
        ]
    )

    with pytest.raises(KeyError, match="Unknown LLM model_run_key"):
        main.get_model_release_date_info(
            df,
            add_model_age_at_due_date=True,
            add_model_release_date=True,
        )

    assert messages == []
