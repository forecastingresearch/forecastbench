"""Anti-drift tests for shared LLM model-run declarations."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ALLOWLIST = {
    ROOT / "src" / "tests" / "contract" / "test_shared_llm_model_runs.py",
}


def iter_python_files():
    """Yield ForecastBench Python files that should not declare model runs."""
    for path in sorted((ROOT / "src").rglob("*.py")):
        if "upload" in path.parts or path in ALLOWLIST:
            continue
        yield path


def test_forecastbench_does_not_declare_local_model_runs():
    forbidden_model_run_list_names = {
        "OPENAI_MODEL_RUNS",
        "TOGETHER_MODEL_RUNS",
        "ANTHROPIC_MODEL_RUNS",
        "XAI_MODEL_RUNS",
        "GOOGLE_MODEL_RUNS",
    }

    offenders = []
    for path in iter_python_files():
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "ModelRun":
                offenders.append(f"{path.relative_to(ROOT)} declares class ModelRun")
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "ModelRun":
                    offenders.append(f"{path.relative_to(ROOT)} calls ModelRun")
                if isinstance(func, ast.Attribute) and func.attr == "ModelRun":
                    offenders.append(f"{path.relative_to(ROOT)} calls ModelRun")
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and target.id in forbidden_model_run_list_names:
                        offenders.append(f"{path.relative_to(ROOT)} declares {target.id}")

    assert offenders == []


def test_forecastbench_declares_selected_shared_model_run_keys_only():
    from utils.llm import model_runs as shared_model_runs

    from llm_forecaster import fb_model_runs

    assert fb_model_runs.FB_MODEL_RUNS == shared_model_runs.select_model_runs(
        fb_model_runs.FB_MODEL_RUN_KEYS
    )


def test_provider_max_workers_covers_all_shared_providers():
    from utils.llm.provider_registry import PROVIDERS

    from llm_forecaster import fb_model_runs

    assert set(fb_model_runs.PROVIDER_MAX_WORKERS) == set(PROVIDERS.values())


def test_model_release_dates_include_canonical_active_llm_model_keys():
    from utils.llm import model_registry

    from llm_forecaster.fb_model_runs import FB_MODEL_RUNS

    release_date_model_keys = set(model_registry.model_release_dates_by_key())
    active_model_keys = {model_run.model_key for model_run in FB_MODEL_RUNS}

    assert active_model_keys <= release_date_model_keys
