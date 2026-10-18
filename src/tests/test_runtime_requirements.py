import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEGACY_BASELINE_ROOT = "/".join(("src", "base_eval", "llm_baselines"))
SHARED_LLM_PROVIDER_REQUIREMENT_NAMES = {
    "anthropic",
    "google-genai",
    "openai",
    "together",
}
EXPORTED_REQUIREMENTS = re.compile(
    r"uv export --project (?P<root>\S+) --frozen --no-hashes --no-emit-project --quiet"
    r" --only-group (?P<group>\S+) --output-file (?P<upload_dir>\S+)/requirements\.txt"
)


def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def job_dependency_groups() -> dict[str, list]:
    groups = pyproject()["dependency-groups"]
    return {name: group for name, group in groups.items() if name not in {"dev", "runtime"}}


def requirement_name(requirement: str) -> str:
    return re.split(r"[<>=!~\[;]", requirement, maxsplit=1)[0].strip().lower().replace("_", "-")


def iter_deploy_makefiles():
    for makefile_path in sorted(ROOT.glob("src/**/Makefile")):
        if any(part.startswith("upload") for part in makefile_path.parts):
            continue
        if (makefile_path.parent / "main.py").exists():
            yield makefile_path


def dependency_group_for(makefile_path: Path) -> str:
    job_dir = makefile_path.parent
    name = job_dir.name.replace("_", "-")
    if job_dir.parent.name == "nightly_update_workflow":
        name = f"nightly-{name}"
    return name


def deploy_recipe_for(makefile_path: Path) -> str:
    return subprocess.check_output(
        ["make", "-n", "-C", str(makefile_path.parent), "deploy", f"ROOT_DIR={ROOT}/"],
        text=True,
    )


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


def test_shared_utils_pin_only_lives_in_the_runtime_group():
    groups = pyproject()["dependency-groups"]

    assert groups["runtime"] == ["fri-utils"]
    for name, group in groups.items():
        if name == "runtime":
            continue
        assert "fri-utils" not in [requirement_name(e) for e in group if isinstance(e, str)], name
        assert {"include-group": "runtime"} in group, name


def test_no_requirements_files_remain():
    """A rebased branch that re-adds a `requirements.txt` would be silently ignored by deploys."""
    tracked = subprocess.check_output(
        ["git", "ls-files", "--", "*requirements*.txt"], cwd=ROOT, text=True
    )

    assert tracked == ""


def test_every_deploy_makefile_declares_its_dependency_group():
    groups = job_dependency_groups()
    declared = set()

    for makefile_path in iter_deploy_makefiles():
        expected = dependency_group_for(makefile_path)
        assert f"DEPENDENCY_GROUP = {expected}\n" in makefile_path.read_text(), makefile_path
        assert expected in groups, makefile_path
        declared.add(expected)

    assert declared == set(groups), "every job group must belong to a deploy Makefile"


def test_deploy_makefiles_stage_locked_requirements():
    for makefile_path in iter_deploy_makefiles():
        recipe = deploy_recipe_for(makefile_path=makefile_path)
        matches = list(EXPORTED_REQUIREMENTS.finditer(recipe))

        assert matches, makefile_path
        for match in matches:
            assert match.group("group") == dependency_group_for(makefile_path), makefile_path
            assert Path(match.group("root")) == ROOT, makefile_path
            upload_dir = Path(match.group("upload_dir"))
            assert f"rm -rf {upload_dir.name}" in recipe[: match.start()], makefile_path


def test_job_groups_do_not_duplicate_utils_owned_llm_provider_deps():
    for name, group in job_dependency_groups().items():
        requirement_names = {requirement_name(e) for e in group if isinstance(e, str)}

        assert requirement_names.isdisjoint(SHARED_LLM_PROVIDER_REQUIREMENT_NAMES), name


def test_metadata_groups_keep_direct_gcp_deps():
    groups = job_dependency_groups()

    for name in ("tag-questions", "validate-questions"):
        requirement_names = {requirement_name(e) for e in groups[name] if isinstance(e, str)}

        assert "google-cloud-storage" in requirement_names
        assert "google-cloud-secret-manager" in requirement_names


def test_dbnomics_update_declares_slack_runtime_dependency():
    """A fresh update-job deployment must support stale-series Slack notifications."""
    group = job_dependency_groups()["func-dbnomics-update"]

    assert "slack-sdk" in {requirement_name(e) for e in group if isinstance(e, str)}


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
