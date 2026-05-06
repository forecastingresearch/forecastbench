from pathlib import Path

from helpers import metadata_prompts

FORECASTING_PREFIXES = ("ZERO_SHOT_", "REFORMAT_")
LEGACY_BASELINE_ROOT = Path("src") / "base_eval" / "llm_baselines"


def test_legacy_llm_baselines_directory_is_removed():
    repo_root = Path(__file__).resolve().parents[2]

    assert not (repo_root / LEGACY_BASELINE_ROOT).exists()


def test_helper_prompts_only_keep_metadata_prompts():
    assert isinstance(metadata_prompts.ASSIGN_CATEGORY_PROMPT, str)
    assert isinstance(metadata_prompts.VALIDATE_QUESTION_PROMPT, str)

    helper_prompt_names = {
        name
        for name, value in vars(metadata_prompts).items()
        if name.isupper() and isinstance(value, str)
    }

    assert helper_prompt_names == {
        "ASSIGN_CATEGORY_PROMPT",
        "VALIDATE_QUESTION_PROMPT",
    }
    assert not any(name.startswith(FORECASTING_PREFIXES) for name in helper_prompt_names)

    helper_prompt_text = "\n".join(
        value for value in metadata_prompts.__dict__.values() if isinstance(value, str)
    )
    assert "expert superforecaster" not in helper_prompt_text
    assert "probabilistic forecasts" not in helper_prompt_text
