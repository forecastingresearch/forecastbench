"""Tests for metadata model request routing."""

from types import SimpleNamespace

import pytest
from utils.llm.provider_registry import PROVIDERS


def import_metadata_llm_without_secret_fetch(monkeypatch):
    """Return the shared metadata_llm module with a clear cached metadata model run.

    The module object must stay the one the metadata jobs imported: re-importing a fresh copy
    here would leave ``metadata._tag``/``metadata._validate`` bound to the old one, which the
    offline fixtures no longer patch.
    """
    from helpers import metadata_llm

    metadata_llm._get_metadata_model_run.cache_clear()
    return metadata_llm


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
