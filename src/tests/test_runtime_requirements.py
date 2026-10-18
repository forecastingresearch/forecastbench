import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_REQUIREMENT = "git+https://github.com/forecastingresearch/utils@"
LEGACY_BASELINE_ROOT = "/".join(("src", "base_eval", "llm_baselines"))
SHARED_LLM_PROVIDER_REQUIREMENT_NAMES = {
    "anthropic",
    "google-genai",
    "openai",
    "together",
}


def iter_deploy_requirements():
    for requirements_path in sorted(ROOT.glob("src/**/requirements.txt")):
        if any(part == "upload" or part.startswith("upload-") for part in requirements_path.parts):
            continue
        makefile_path = requirements_path.with_name("Makefile")
        if makefile_path.exists():
            yield requirements_path, makefile_path


def test_shared_utils_pin_only_lives_in_root_runtime_requirements():
    root_runtime_requirements = (ROOT / "requirements.runtime.txt").read_text()

    assert RUNTIME_REQUIREMENT in root_runtime_requirements
    for requirements_path, _makefile_path in iter_deploy_requirements():
        assert RUNTIME_REQUIREMENT not in requirements_path.read_text(), requirements_path


STAGED_RUNTIME_REQUIREMENT = re.compile(
    rf"cat (?:\$\(ROOT_DIR\)|{re.escape(str(ROOT))}/)"
    r"requirements\.runtime\.txt requirements\.txt"
    r" > (?P<upload_dir>\$1|\$\([^)]+\)|\S+)/requirements\.txt"
)


def deploy_recipe_for(makefile_path: Path) -> str:
    return subprocess.check_output(
        ["make", "-n", "-C", str(makefile_path.parent), "deploy"],
        text=True,
    )


def staging_text_for(makefile_path: Path) -> str:
    makefile = makefile_path.read_text()
    if "$(stage-orchestration-upload)" in makefile:
        return deploy_recipe_for(makefile_path=makefile_path)
    return makefile


def test_deploy_makefiles_stage_shared_runtime_requirements():
    for _requirements_path, makefile_path in iter_deploy_requirements():
        staging_text = staging_text_for(makefile_path=makefile_path)

        assert STAGED_RUNTIME_REQUIREMENT.search(staging_text), makefile_path


def test_deploy_makefiles_clear_upload_dir_before_staging_requirements():
    for _requirements_path, makefile_path in iter_deploy_requirements():
        staging_text = staging_text_for(makefile_path=makefile_path)
        for match in STAGED_RUNTIME_REQUIREMENT.finditer(staging_text):
            upload_dir = match.group("upload_dir")
            assert f"rm -rf {upload_dir}" in staging_text[: match.start()], makefile_path


def test_deploy_requirements_do_not_duplicate_utils_owned_llm_provider_deps():
    for requirements_path, _makefile_path in iter_deploy_requirements():
        requirements = requirements_path.read_text().splitlines()
        requirement_names = {
            re.split(r"[<>=!~]", requirement, maxsplit=1)[0].strip() for requirement in requirements
        }
        assert requirement_names.isdisjoint(SHARED_LLM_PROVIDER_REQUIREMENT_NAMES), (
            requirements_path
        )


def test_metadata_deploy_requirements_keep_direct_gcp_deps():
    deploy_dirs = [
        ROOT / "src/metadata/tag_questions",
        ROOT / "src/metadata/validate_questions",
    ]

    for deploy_dir in deploy_dirs:
        requirements = (deploy_dir / "requirements.txt").read_text().splitlines()

        assert "google-cloud-storage" in requirements
        assert "google-cloud-secret-manager" in requirements


def test_dbnomics_update_declares_slack_runtime_dependency():
    """A fresh update-job deployment must support stale-series Slack notifications."""
    requirements_path = ROOT / "src/orchestration/func_dbnomics_update/requirements.txt"
    requirement_names = {
        re.split(r"[<>=!~\[;]", line, maxsplit=1)[0].strip().lower().replace("_", "-")
        for line in requirements_path.read_text().splitlines()
    }

    assert "slack-sdk" in requirement_names


def test_root_makefile_routes_llm_baseline_targets_to_refactored_jobs():
    makefile = (ROOT / "Makefile").read_text()

    assert "$(MAKE) -C src/orchestration/func_llm_forecaster_manager" in makefile
    assert "$(MAKE) -C src/orchestration/func_llm_forecaster_worker" in makefile
    assert f"{LEGACY_BASELINE_ROOT}/manager" not in makefile
    assert f"{LEGACY_BASELINE_ROOT}/worker" not in makefile


def test_root_makefile_exports_google_cloud_project_to_deployments():
    makefile = (ROOT / "Makefile").read_text()

    assert "GOOGLE_CLOUD_PROJECT=$(CLOUD_PROJECT)" in makefile


def test_root_makefile_exports_transcript_bucket_to_deployments():
    makefile = (ROOT / "Makefile").read_text()

    assert "export FORECAST_SETS_TRANSCRIPTS_BUCKET" in makefile
    assert "FORECAST_SETS_TRANSCRIPTS_BUCKET=$(FORECAST_SETS_TRANSCRIPTS_BUCKET)" in makefile


def test_root_makefile_does_not_ship_random_seed_to_deployments():
    """`RANDOM_SEED` is for testing/reproducibility only and must never reach a Cloud Run job."""
    makefile = (ROOT / "Makefile").read_text()

    assert "RANDOM_SEED" not in makefile


def test_root_make_test_bootstraps_python_env_before_pytest():
    result = subprocess.run(
        ["make", "--dry-run", "--always-make", "test", "ARGS=--version"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    dry_run_output = result.stdout + result.stderr

    sync = dry_run_output.index("uv sync")
    pytest_invocation = dry_run_output.index("python -m pytest src/tests/ --version")

    assert sync < pytest_invocation


def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def requirement_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith(("#", "-r"))
    ]


def dependency_group_for(requirements_path: Path) -> str:
    job_dir = requirements_path.parent
    name = job_dir.name.replace("_", "-")
    if job_dir.parent.name == "nightly_update_workflow":
        name = f"nightly-{name}"
    return name


def test_shared_utils_is_pinned_to_a_git_revision():
    makefile = (ROOT / "Makefile").read_text()
    fri_utils = pyproject()["tool"]["uv"]["sources"]["fri-utils"]

    assert fri_utils["git"] == "https://github.com/forecastingresearch/utils"
    assert re.fullmatch(r"[0-9a-f]{40}", fri_utils["rev"])
    assert "path" not in fri_utils
    assert "editable" not in fri_utils
    assert "LOCAL_UTILS_PATH" not in makefile
    assert "install -e" not in makefile
    assert "--editable" not in makefile


def test_runtime_and_dev_groups_match_root_requirements():
    """Until deployment reads `uv.lock`, the staged pin must equal the locked one."""
    groups = pyproject()["dependency-groups"]
    rev = pyproject()["tool"]["uv"]["sources"]["fri-utils"]["rev"]

    assert groups["runtime"] == ["fri-utils"]
    assert (ROOT / "requirements.runtime.txt").read_text().strip() == (
        f"git+https://github.com/forecastingresearch/utils@{rev}#egg=fri-utils"
    )
    assert [entry for entry in groups["dev"] if isinstance(entry, str)] == requirement_lines(
        ROOT / "requirements.txt"
    )


def test_dependency_groups_mirror_job_requirements():
    """Until deployment reads `uv.lock`, each job group must equal its `requirements.txt`."""
    groups = pyproject()["dependency-groups"]

    for requirements_path, _makefile_path in iter_deploy_requirements():
        group = groups[dependency_group_for(requirements_path)]

        assert {"include-group": "runtime"} in group, requirements_path
        assert [entry for entry in group if isinstance(entry, str)] == requirement_lines(
            requirements_path
        ), requirements_path
