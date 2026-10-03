"""Convention contracts for the ``llm_forecaster`` package: layering and import style."""

import ast
import inspect
from pathlib import Path

from llm_forecaster import fb_model_runs

ROOT = Path(__file__).resolve().parents[3]


def _imports_orchestration(node: ast.AST) -> bool:
    """Whether an import node names the ``orchestration`` package or anything inside it."""
    if isinstance(node, ast.ImportFrom):
        return (node.module or "").split(".")[0] == "orchestration"
    if isinstance(node, ast.Import):
        return any(alias.name.split(".")[0] == "orchestration" for alias in node.names)
    return False


def test_llm_forecaster_core_modules_do_not_import_orchestration_io():
    # The whole package, except the smoke-test CLI, whose lazy orchestration import is sanctioned
    # by ``test_smoke_test_tooling_imports_without_orchestration_io`` in the offline-import contract.
    checked_files = sorted(
        path for path in (ROOT / "src/llm_forecaster").glob("*.py") if path.name != "smoke_test.py"
    )
    assert checked_files
    offenders = []

    for path in checked_files:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if _imports_orchestration(node):
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")

    assert offenders == []


def test_forecastbench_model_run_imports_are_top_level():
    tree = ast.parse(inspect.getsource(fb_model_runs))
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for child in ast.walk(node):
                if isinstance(child, (ast.Import, ast.ImportFrom)):
                    offenders.append(node.name)

    assert offenders == []


def test_llm_forecaster_files_do_not_use_future_annotations():
    src_root = Path(__file__).resolve().parents[2]
    forbidden_import = "from __future__ import " + "annotations"
    paths = [
        *sorted((src_root / "llm_forecaster").glob("**/*.py")),
        *sorted((src_root / "tests" / "unit" / "llm_forecaster").glob("**/*.py")),
    ]

    assert paths
    offenders = [path for path in paths if forbidden_import in path.read_text()]
    assert offenders == []
