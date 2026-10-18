import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ROOT_UTILS_COPY_COMMAND = " ".join(("cp", "-r", "$(ROOT_DIR)" + "utils"))


def test_root_utils_submodule_metadata_removed():
    gitmodules = (ROOT / ".gitmodules").read_text()
    assert '[submodule "utils"]' not in gitmodules
    assert "\tpath = utils" not in gitmodules

    tracked = subprocess.run(
        ["git", "ls-files", "-s", "utils"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    assert tracked.stdout == ""


def test_formatters_no_longer_exclude_root_utils():
    ruff_config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["ruff"]
    exclude_patterns = (
        ruff_config.get("exclude", [])
        + ruff_config.get("extend-exclude", [])
        + ruff_config.get("format", {}).get("exclude", [])
    )

    assert not [pattern for pattern in exclude_patterns if "utils" in pattern]


def test_deploy_makefiles_do_not_copy_root_utils():
    offenders = [
        path for path in ROOT.glob("src/**/Makefile") if ROOT_UTILS_COPY_COMMAND in path.read_text()
    ]
    assert offenders == []


def test_no_root_only_sys_path_hacks_for_utils_imports():
    offenders = []
    root_relative = "../../" + "../.."
    for path in ROOT.glob("src/**/*.py"):
        text = path.read_text()
        if f'{root_relative}"' in text or f"{root_relative}')" in text:
            offenders.append(path)
    assert offenders == []


def test_no_repo_root_sys_path_hacks_before_utils_imports():
    path_append = re.compile(
        r"sys\.path\.append\(os\.path\.join\(os\.path\.dirname\(__file__\),"
        r" [\"']([^\"']+)[\"']\)\)"
    )
    offenders = []

    for path in ROOT.glob("src/**/*.py"):
        text = path.read_text()
        if "from utils import" not in text:
            continue

        utils_import_index = text.index("from utils import")
        for match in path_append.finditer(text[:utils_import_index]):
            appended_path = (path.parent / match.group(1)).resolve()
            if appended_path == ROOT:
                offenders.append(path)

    assert offenders == []
