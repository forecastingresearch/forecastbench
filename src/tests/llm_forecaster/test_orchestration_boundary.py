import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_llm_forecaster_core_modules_do_not_import_orchestration_io():
    checked_files = [
        ROOT / "src/llm_forecaster/runner.py",
        ROOT / "src/llm_forecaster/question_set.py",
        ROOT / "src/llm_forecaster/model_run_transcripts.py",
    ]
    offenders = []

    for path in checked_files:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "orchestration":
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "orchestration" or alias.name.startswith("orchestration."):
                        offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")

    assert offenders == []
