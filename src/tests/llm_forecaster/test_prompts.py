import hashlib
from string import Formatter

import pytest

from llm_forecaster import prompts

PROMPT_DIGESTS = {
    "FORECAST_EXTRACTION_PROMPT": (
        "4a820cd3c4b59237091466e7e921716c4df3ca7f8c0622c11d539dbdcbf093f5"
    ),
    "ZERO_SHOT_MARKET_PROMPT": ("3e9773f317df0975010da625bbce26c20089df55d6066be43c2a539423db499f"),
    "ZERO_SHOT_MARKET_WITH_FREEZE_VALUE_PROMPT": (
        "d75d7e41749303a6f87bbac272b26393df905c7f839ea52a245b5c4a0bc0e188"
    ),
    "ZERO_SHOT_DATASET_PROMPT": (
        "df152631446743c078615a74b9ea57b7eca79c27f7357a74459e38911dc52ff5"
    ),
}


def test_forecasting_prompt_text_matches_snapshots():
    for prompt_name, expected_digest in PROMPT_DIGESTS.items():
        prompt_text = getattr(prompts, prompt_name)

        assert hashlib.sha256(prompt_text.encode()).hexdigest() == expected_digest


def test_prompt_module_exports_expected_forecasting_prompts():
    prompt_names = {
        name for name, value in vars(prompts).items() if name.isupper() and isinstance(value, str)
    }

    assert prompt_names == set(PROMPT_DIGESTS)


def test_prompt_field_parser_is_private():
    assert not hasattr(prompts, "template_field_names")


def test_render_template_reuses_parsed_template_fields(monkeypatch):
    prompts._template_field_names.cache_clear()

    class CountingFormatter:
        def __init__(self) -> None:
            self.calls = 0

        def parse(self, template):
            self.calls += 1
            return Formatter().parse(template)

    formatter = CountingFormatter()
    monkeypatch.setattr(prompts, "_FORMATTER", formatter)
    template = "Question: {question}"

    try:
        for _ in range(2):
            prompts.render_template(
                template=template,
                params={"question": "Will this work?"},
            )

        assert formatter.calls == 1
    finally:
        prompts._template_field_names.cache_clear()


def test_render_template_validates_fields_and_preserves_literal_braces():
    rendered = prompts.render_template(
        template="Question: {question}\nAnswer: {{ Insert answer here }}",
        params={"question": "Will this work?"},
    )

    assert rendered == "Question: Will this work?\nAnswer: { Insert answer here }"


def test_render_template_rejects_missing_fields():
    with pytest.raises(ValueError, match="Missing prompt fields: question"):
        prompts.render_template(
            template="Question: {question}",
            params={},
        )


def test_render_template_rejects_extra_fields():
    with pytest.raises(ValueError, match="Unexpected prompt fields: unused"):
        prompts.render_template(
            template="Question: {question}",
            params={"question": "Will this work?", "unused": "value"},
        )
