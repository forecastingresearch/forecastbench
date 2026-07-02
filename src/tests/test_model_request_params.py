"""Tests for metadata model request routing."""

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from utils.llm.provider_registry import PROVIDERS

from helpers import constants

ROOT = Path(__file__).resolve().parents[2]
LEGACY_MODEL_RUNS_MAP = "_".join(("MODELS", "TO", "RUN"))
LEGACY_MODEL_RUNS_BY_SOURCE_MAP = "_".join(("MODELS", "TO", "RUN", "BY", "SOURCE"))
LEGACY_CONSTANTS_MODEL_RUNS = ".".join(("constants", LEGACY_MODEL_RUNS_MAP))


def import_metadata_llm_without_secret_fetch(monkeypatch):
    """Import metadata_llm with a clear cached metadata model run."""
    sys.modules.pop("helpers.metadata_llm", None)
    metadata_llm = importlib.import_module("helpers.metadata_llm")
    if hasattr(metadata_llm, "_get_metadata_model_run"):
        metadata_llm._get_metadata_model_run.cache_clear()
    return metadata_llm


def test_constants_do_not_expose_legacy_llm_model_maps():
    legacy_constants = {
        LEGACY_MODEL_RUNS_MAP,
        LEGACY_MODEL_RUNS_BY_SOURCE_MAP,
        "MODEL_NAME_TO_SOURCE",
        "MODEL_TOKEN_LIMITS",
    }

    for name in legacy_constants:
        assert not hasattr(constants, name), name


def test_metadata_callers_use_metadata_model_response_helper():
    metadata_files = [
        ROOT / "src" / "metadata" / "tag_questions" / "main.py",
        ROOT / "src" / "metadata" / "validate_questions" / "main.py",
    ]

    for path in metadata_files:
        source = path.read_text()
        assert "metadata_llm.get_metadata_model_response" in source, path
        assert "model_eval" not in source, path


def test_metadata_llm_no_longer_contains_legacy_provider_routing():
    source = (ROOT / "src" / "helpers" / "metadata_llm.py").read_text()
    legacy_fragments = [
        "get_response_from_model",
        "infer_model_source",
        "get_model_org",
        "get_response_from_oai_model",
        "get_response_from_anthropic_model",
        "get_response_from_together_ai_model",
        "get_response_from_google_model",
        "get_response_from_xai_model",
        LEGACY_CONSTANTS_MODEL_RUNS,
    ]

    for fragment in legacy_fragments:
        assert fragment not in source, fragment
    assert "question_curation.METADATA_MODEL_NAME" not in source
    assert "gpt-5-mini-2025-08-07-run-variant-01" in source


def test_project_openai_safety_identifier_comes_from_secret_manager(monkeypatch):
    from helpers import openai_safety

    calls = []

    def fake_get_secret(secret_name):
        calls.append(secret_name)
        return " forecastbench-safety-id\n"

    openai_safety.get_openai_safety_identifier.cache_clear()
    monkeypatch.setattr(openai_safety, "get_secret", fake_get_secret)

    assert openai_safety.get_openai_safety_identifier() == "forecastbench-safety-id"
    assert calls == ["OPENAI_SAFETY_IDENTIFIER"]

    openai_safety.get_openai_safety_identifier.cache_clear()


def test_metadata_model_response_routes_through_shared_model_run(monkeypatch):
    metadata_llm = import_metadata_llm_without_secret_fetch(monkeypatch)
    calls = []

    class FakeModelRun:
        provider = PROVIDERS["OpenAI"]

        def get_response(self, prompt, **kwargs):
            calls.append(("get_response", prompt, kwargs))
            return "metadata response"

    def fake_get_model_run(model_run_key):
        calls.append(("get_model_run", model_run_key))
        return FakeModelRun()

    monkeypatch.setattr(metadata_llm, "configure_api_keys", lambda **kwargs: None, raising=False)
    monkeypatch.setattr(
        metadata_llm, "validate_provider_keys", lambda providers: None, raising=False
    )
    monkeypatch.setattr(
        metadata_llm,
        "model_runs",
        SimpleNamespace(get_model_run=fake_get_model_run),
        raising=False,
    )
    monkeypatch.setattr(
        metadata_llm,
        "get_openai_safety_identifier",
        lambda: "forecastbench-safety-id",
        raising=False,
    )
    monkeypatch.setattr(
        metadata_llm,
        "get_response",
        lambda *args, **kwargs: pytest.fail("metadata should use a ModelRun"),
        raising=False,
    )

    response = metadata_llm.get_metadata_model_response(
        prompt="Classify this question.",
        max_output_tokens=123,
    )

    assert response == "metadata response"
    assert calls == [
        ("get_model_run", "gpt-5-mini-2025-08-07-run-variant-01"),
        (
            "get_response",
            "Classify this question.",
            {
                "max_output_tokens": 123,
                "reasoning": {"effort": "minimal"},
                "safety_identifier": "forecastbench-safety-id",
            },
        ),
    ]


def test_metadata_model_response_configures_model_run_provider_before_request(monkeypatch):
    metadata_llm = import_metadata_llm_without_secret_fetch(monkeypatch)
    calls = []

    class FakeModelRun:
        provider = PROVIDERS["OpenAI"]

        def get_response(self, prompt, **kwargs):
            calls.append(("get_response", self.provider))
            return "metadata response"

    def fake_get_model_run(model_run_key):
        calls.append(("get_model_run", model_run_key))
        return FakeModelRun()

    def fake_configure_api_keys(**kwargs):
        calls.append(("configure", kwargs))

    def fake_validate_provider_keys(providers):
        calls.append(("validate", providers))

    monkeypatch.setattr(metadata_llm, "configure_api_keys", fake_configure_api_keys, raising=False)
    monkeypatch.setattr(
        metadata_llm, "validate_provider_keys", fake_validate_provider_keys, raising=False
    )
    monkeypatch.setattr(
        metadata_llm,
        "model_runs",
        SimpleNamespace(get_model_run=fake_get_model_run),
        raising=False,
    )
    monkeypatch.setattr(
        metadata_llm,
        "get_openai_safety_identifier",
        lambda: "forecastbench-safety-id",
        raising=False,
    )

    assert metadata_llm.get_metadata_model_response("Prompt", 50) == "metadata response"

    assert calls == [
        ("get_model_run", "gpt-5-mini-2025-08-07-run-variant-01"),
        ("configure", {"from_gcp": True}),
        ("validate", [PROVIDERS["OpenAI"]]),
        ("get_response", PROVIDERS["OpenAI"]),
    ]
