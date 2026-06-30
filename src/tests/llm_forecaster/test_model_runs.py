import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from utils.llm import model_registry as shared_model_registry
from utils.llm import model_runs as shared_model_runs
from utils.llm.provider_registry import PROVIDERS

from llm_forecaster import fb_model_runs

LEGACY_MODEL_RUN_KEY_HELPER = "_".join(("", "CANONICAL", "MODEL", "RUN", "KEYS"))
LEGACY_MODEL_RUN_OPTIONS_HELPER = "_".join(("", "options", "for", "model", "run"))
LEGACY_CONFLICTING_RUN_OPTIONS = "_".join(("conflicting", "options"))
LEGACY_MODEL_RUNS_MAP = "_".join(("MODELS", "TO", "RUN"))
LEGACY_MODEL_RUN_CONSTRUCTOR = "Model" + "Run("


def test_forecastbench_selects_active_shared_model_run_objects():
    selected_runs = shared_model_runs.select_model_runs(fb_model_runs.FB_MODEL_RUN_KEYS)

    assert fb_model_runs.FB_MODEL_RUNS == selected_runs
    assert [run.model_run_key for run in fb_model_runs.FB_MODEL_RUNS] == (
        fb_model_runs.FB_MODEL_RUN_KEYS
    )
    for model_run_key, run in zip(
        fb_model_runs.FB_MODEL_RUN_KEYS,
        fb_model_runs.FB_MODEL_RUNS,
    ):
        assert shared_model_runs.get_model_run(model_run_key) == run
        assert isinstance(run, shared_model_runs.ModelRun)
        assert run.model.active


def test_forecastbench_selected_model_run_indexes_use_prefixed_names():
    assert hasattr(fb_model_runs, "FB_MODEL_RUNS")
    assert hasattr(fb_model_runs, "FB_MODEL_RUNS_BY_KEY")
    assert hasattr(fb_model_runs, "FB_MODEL_RUNS_BY_SLUG")
    assert not hasattr(fb_model_runs, "FORECASTBENCH_MODEL_RUN_KEYS")
    assert not hasattr(fb_model_runs, "FORECASTBENCH_MODEL_RUNS")
    assert not hasattr(fb_model_runs, "FORECASTBENCH_MODEL_RUNS_BY_KEY")
    assert not hasattr(fb_model_runs, "FORECASTBENCH_MODEL_RUNS_BY_SLUG")
    assert not hasattr(fb_model_runs, "_".join(("MODEL", "RUNS")))
    assert not hasattr(fb_model_runs, "_".join(("MODEL", "RUNS", "BY", "KEY")))
    assert not hasattr(fb_model_runs, "_".join(("MODEL", "RUNS", "BY", "SLUG")))


def test_model_run_calls_utils_with_provider_model_id_and_options():
    run = fb_model_runs.get_model_run_by_slug("minimax-m2.7")

    with patch("utils.llm.model_registry.get_response", return_value="0.61") as mock_call:
        assert run.get_response("prompt", max_tokens=128) == "0.61"

    mock_call.assert_called_once_with(
        provider=PROVIDERS["Together"],
        model_id="MiniMaxAI/MiniMax-M2.7",
        prompt="prompt",
        options={"temperature": 0, "max_tokens": 128},
    )


def test_model_run_slugs_are_unique_and_file_safe():
    slugs = [run.slug for run in fb_model_runs.FB_MODEL_RUNS]

    assert slugs
    assert len(slugs) == len(set(slugs))
    assert all(slug == slug.lower() for slug in slugs)
    assert all(" " not in slug and "/" not in slug and "_" not in slug for slug in slugs)


def test_labs_and_providers_are_shared_registry_objects():
    runs = {run.slug: run for run in fb_model_runs.FB_MODEL_RUNS}

    assert "kimi-k2.6" not in runs
    assert runs["minimax-m2.7"].lab == shared_model_registry.MODELS_BY_KEY["minimax-m2.7"].lab
    assert runs["minimax-m2.7"].provider == PROVIDERS["Together"]
    assert runs["minimax-m2.7-12000"].lab == shared_model_registry.MODELS_BY_KEY["minimax-m2.7"].lab
    assert runs["minimax-m2.7-12000"].provider == PROVIDERS["Together"]
    assert runs["kimi-k2.6-16000"].lab == shared_model_registry.MODELS_BY_KEY["kimi-k2.6"].lab
    assert runs["gemma-4-31b-it"].lab == shared_model_registry.MODELS_BY_KEY["gemma-4-31b-it"].lab
    assert runs["gemma-4-31b-it"].provider == PROVIDERS["Together"]


def test_model_organization_uses_lab_name():
    run = fb_model_runs.get_model_run_by_slug("minimax-m2.7")

    assert run.lab.name == shared_model_registry.MODELS_BY_KEY["minimax-m2.7"].lab.name


def test_options_are_declared_on_model_runs_not_inferred_by_helpers():
    source = inspect.getsource(fb_model_runs)

    assert LEGACY_MODEL_RUN_KEY_HELPER not in source
    assert LEGACY_MODEL_RUN_OPTIONS_HELPER not in source
    assert LEGACY_CONFLICTING_RUN_OPTIONS not in source
    assert LEGACY_MODEL_RUNS_MAP not in source
    assert "from helpers import constants" not in source
    assert "google.genai" not in source

    runs = {run.slug: run for run in fb_model_runs.FB_MODEL_RUNS}
    anthropic_runs = [
        run for run in fb_model_runs.FB_MODEL_RUNS if run.provider == PROVIDERS["Anthropic"]
    ]
    assert anthropic_runs
    assert all("max_tokens" in run.options for run in anthropic_runs)
    assert runs["minimax-m2.7"].options == {"temperature": 0}
    assert runs["minimax-m2.7-12000"].options == {
        "temperature": 1.0,
        "top_p": 0.95,
        "top_k": 40,
        "max_tokens": 12000,
    }
    assert runs["minimax-m3-adaptive-thinking-12000"].options == {
        "temperature": 1.0,
        "top_p": 0.95,
        "top_k": 40,
        "chat_template_kwargs": {"thinking_mode": "adaptive"},
        "max_tokens": 12000,
    }
    assert runs["kimi-k2.6-16000"].options == {"max_tokens": 16000}
    assert runs["glm-5.2"].options == {"temperature": 0}
    assert runs["glm-5.2-12000"].options == {"temperature": 0, "max_tokens": 12000}
    assert runs["grok-4.3"].options == {"temperature": 0}
    assert runs["gemini-3.5-flash"].options == {
        "candidate_count": 1,
        "temperature": 0,
        "automatic_function_calling": {"disable": True},
    }
    assert runs["claude-opus-4-7-1024"].options == {"max_tokens": 1024}
    assert runs["claude-opus-4-8-1024"].options == {"max_tokens": 1024}
    assert runs["claude-opus-4-8-adaptive-thinking-max-web-search-128000"].options == {
        "max_tokens": 128000,
        "output_config": {"effort": "max"},
        "thinking": {"type": "adaptive"},
        "tools": [
            {
                "type": "web_search_20260209",
                "name": "web_search",
            }
        ],
    }
    together_runs_without_zero_temperature = {
        "kimi-k2.6-16000",
        "minimax-m2.7-12000",
        "minimax-m3-adaptive-thinking-12000",
    }
    for run in fb_model_runs.FB_MODEL_RUNS:
        if run.provider in (PROVIDERS["Together"], PROVIDERS["xAI"]):
            if run.slug in together_runs_without_zero_temperature:
                continue
            assert run.options.get("temperature") == 0
    for run in fb_model_runs.FB_MODEL_RUNS:
        if run.provider == PROVIDERS["Google"]:
            if "tools" in run.options or "thinking_config" in run.options:
                assert "temperature" not in run.options
            else:
                assert run.options.get("temperature") == 0
    anthropic_runs_without_temperature = {
        "claude-sonnet-4-6-adaptive-thinking-16000",
        "claude-opus-4-7-1024",
        "claude-opus-4-8-1024",
        "claude-opus-4-8-adaptive-thinking-high-24000",
        "claude-opus-4-8-adaptive-thinking-high-web-search-64000",
        "claude-opus-4-8-adaptive-thinking-max-web-search-128000",
    }
    for run in anthropic_runs:
        if run.slug in anthropic_runs_without_temperature:
            assert "temperature" not in run.options
        else:
            assert run.options.get("temperature") == 0


def test_forecastbench_does_not_declare_local_model_runs():
    source = inspect.getsource(fb_model_runs)

    assert "@dataclass" not in source
    assert LEGACY_MODEL_RUN_CONSTRUCTOR not in source
    assert "OPENAI_MODEL_RUNS" not in source
    assert "TOGETHER_MODEL_RUNS" not in source
    assert "ANTHROPIC_MODEL_RUNS" not in source
    assert "XAI_MODEL_RUNS" not in source
    assert "GOOGLE_MODEL_RUNS" not in source


def test_forecastbench_model_run_imports_are_top_level():
    tree = ast.parse(inspect.getsource(fb_model_runs))
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for child in ast.walk(node):
                if isinstance(child, (ast.Import, ast.ImportFrom)):
                    offenders.append(node.name)

    assert offenders == []


def test_forecastbench_keys_select_real_shared_options():
    for key in fb_model_runs.FB_MODEL_RUN_KEYS:
        assert fb_model_runs.get_model_run(key) == shared_model_runs.get_model_run(key)

    for run in fb_model_runs.FB_MODEL_RUNS:
        if run.provider == PROVIDERS["Together"]:
            if run.slug in {
                "kimi-k2.6-16000",
                "minimax-m2.7-12000",
                "minimax-m3-adaptive-thinking-12000",
            }:
                continue
            assert run.options.get("temperature") == 0
    for run in fb_model_runs.FB_MODEL_RUNS:
        if run.provider == PROVIDERS["xAI"]:
            assert run.options.get("temperature") == 0
    for run in fb_model_runs.FB_MODEL_RUNS:
        if run.provider == PROVIDERS["Google"]:
            if "tools" in run.options or "thinking_config" in run.options:
                assert "temperature" not in run.options
            else:
                assert run.options.get("temperature") == 0


def test_model_run_lookup_raises_for_missing_key():
    with pytest.raises(
        KeyError,
        match="Unknown ForecastBench LLM model run key: unknown-model",
    ):
        fb_model_runs.get_model_run("unknown-model")


def test_model_run_slug_lookup_raises_for_missing_slug():
    with pytest.raises(
        KeyError,
        match="Unknown ForecastBench LLM model run slug: unknown-model",
    ):
        fb_model_runs.get_model_run_by_slug("unknown-model")


def test_providers_for_model_runs_includes_selected_and_forecast_extraction_models():
    anthropic_run = SimpleNamespace(provider=PROVIDERS["Anthropic"])

    providers = fb_model_runs.providers_for_model_runs([anthropic_run])

    assert providers == [PROVIDERS["Anthropic"], PROVIDERS["OpenAI"]]
    assert (
        fb_model_runs.FORECAST_EXTRACTION_MODEL.model_run_key
        == "gpt-5-mini-2025-08-07-run-variant-01"
    )


def test_configure_and_validate_provider_keys_uses_shared_gcp_loader():
    selected = (fb_model_runs.get_model_run_by_slug("claude-opus-4-7-1024"),)
    forecast_extraction_run = SimpleNamespace(
        provider=PROVIDERS["OpenAI"],
        options={},
    )

    with (
        patch("llm_forecaster.fb_model_runs.FORECAST_EXTRACTION_MODEL", forecast_extraction_run),
        patch("llm_forecaster.fb_model_runs.configure_api_keys") as configure,
        patch("llm_forecaster.fb_model_runs.validate_provider_keys") as validate,
        patch(
            "llm_forecaster.fb_model_runs.get_openai_safety_identifier",
            return_value="forecastbench-safety-id",
        ),
    ):
        fb_model_runs.configure_and_validate_provider_keys(selected)

    configure.assert_called_once_with(from_gcp=True)
    providers = validate.call_args.args[0]
    assert providers == [selected[0].provider, PROVIDERS["OpenAI"]]


def test_configure_and_validate_provider_keys_adds_project_openai_safety_identifier():
    first_openai_run = SimpleNamespace(
        provider=PROVIDERS["OpenAI"],
        options={"reasoning": {"effort": "low"}},
    )
    second_openai_run = SimpleNamespace(
        provider=PROVIDERS["OpenAI"],
        options={"safety_identifier": "stale-id"},
    )
    forecast_extraction_run = SimpleNamespace(
        provider=PROVIDERS["OpenAI"],
        options={},
    )

    with (
        patch("llm_forecaster.fb_model_runs.FORECAST_EXTRACTION_MODEL", forecast_extraction_run),
        patch("llm_forecaster.fb_model_runs.configure_api_keys") as configure,
        patch("llm_forecaster.fb_model_runs.validate_provider_keys") as validate,
        patch(
            "llm_forecaster.fb_model_runs.get_openai_safety_identifier",
            return_value="forecastbench-safety-id",
        ) as get_openai_safety_identifier,
    ):
        fb_model_runs.configure_and_validate_provider_keys((first_openai_run, second_openai_run))

    configure.assert_called_once_with(from_gcp=True)
    get_openai_safety_identifier.assert_called_once_with()
    assert first_openai_run.options == {
        "reasoning": {"effort": "low"},
        "safety_identifier": "forecastbench-safety-id",
    }
    assert second_openai_run.options == {
        "safety_identifier": "forecastbench-safety-id",
    }
    assert forecast_extraction_run.options == {
        "safety_identifier": "forecastbench-safety-id",
    }
    assert validate.call_args.args[0] == [PROVIDERS["OpenAI"]]


def test_configure_and_validate_provider_keys_keeps_non_openai_options_unchanged():
    selected = SimpleNamespace(
        provider=PROVIDERS["Anthropic"],
        options={"max_tokens": 1024},
    )
    forecast_extraction_run = SimpleNamespace(
        provider=PROVIDERS["OpenAI"],
        options={},
    )

    with (
        patch("llm_forecaster.fb_model_runs.FORECAST_EXTRACTION_MODEL", forecast_extraction_run),
        patch("llm_forecaster.fb_model_runs.configure_api_keys"),
        patch("llm_forecaster.fb_model_runs.validate_provider_keys"),
        patch(
            "llm_forecaster.fb_model_runs.get_openai_safety_identifier",
            return_value="forecastbench-safety-id",
        ) as get_openai_safety_identifier,
    ):
        fb_model_runs.configure_and_validate_provider_keys((selected,))

    get_openai_safety_identifier.assert_called_once_with()
    assert selected.options == {"max_tokens": 1024}
    assert forecast_extraction_run.options == {
        "safety_identifier": "forecastbench-safety-id",
    }


def test_forecastbench_model_runs_do_not_declare_local_api_key_config():
    source = inspect.getsource(fb_model_runs)

    assert "PROVIDER_API_KEY_CONFIG" not in source
    assert "_api_key_kwargs_for_providers" not in source
    assert "OPENAI_API_KEY_SECRET_NAME" not in source
    assert "os.getenv" not in source
    assert "gcp.get_secret" not in source


def test_provider_max_workers_matches_plan_exactly():
    assert fb_model_runs.DEFAULT_PROVIDER_MAX_WORKERS == 4
    assert fb_model_runs.PROVIDER_MAX_WORKERS == {
        PROVIDERS["OpenAI"]: 50,
        PROVIDERS["Anthropic"]: 50,
        PROVIDERS["Google"]: 50,
        PROVIDERS["xAI"]: 50,
        PROVIDERS["Together"]: 4,
    }


def test_provider_max_workers_covers_all_shared_providers():
    assert set(fb_model_runs.PROVIDER_MAX_WORKERS) == set(PROVIDERS.values())


def test_llm_forecaster_files_do_not_use_future_annotations():
    src_root = Path(__file__).resolve().parents[2]
    forbidden_import = "from __future__ import " + "annotations"
    paths = [
        *sorted((src_root / "llm_forecaster").glob("**/*.py")),
        *sorted((src_root / "tests" / "llm_forecaster").glob("**/*.py")),
    ]

    assert paths
    offenders = [path for path in paths if forbidden_import in path.read_text()]
    assert offenders == []
