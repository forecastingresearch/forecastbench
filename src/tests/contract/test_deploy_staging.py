"""Deploy-staging contracts for the LLM-forecaster + leaderboard jobs.

Job-specific packaging guarantees for the Cloud Run deploys touched by the LLM-forecaster rewrite.
The *generic* cross-job staging invariants (the shared ``utils`` pin lives only in the root runtime
requirements; ``cat requirements.runtime.txt requirements.txt > <upload>/requirements.txt``) are
asserted for *every* deploy dir in ``test_runtime_requirements.py`` and are intentionally not
repeated here — this file keeps only the per-job specifics.
"""

import subprocess
from pathlib import Path

# src/tests/contract/ -> repo root is three parents up.
ROOT = Path(__file__).resolve().parents[3]

RAW_QUESTION_SET_REQUIREMENTS_WITHOUT_GITPYTHON = [
    ROOT / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_manager" / "requirements.txt",
    ROOT / "src" / "orchestration" / "func_llm_forecaster_worker" / "requirements.txt",
    ROOT / "src" / "base_eval" / "naive_and_dummy_forecasters" / "requirements.txt",
]


def test_worker_deploy_stages_runtime_requirements_and_shared_code():
    deploy_dir = ROOT / "src/orchestration/func_llm_forecaster_worker"
    makefile = (deploy_dir / "Makefile").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(deploy_dir), "deploy"],
        text=True,
    )

    assert "func-llm-forecaster-worker" in makefile
    assert "--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT)" in makefile
    assert "include $(ROOT_DIR)orchestration_upload.mk" in makefile
    assert "ORCHESTRATION_EXTRA_PACKAGES = llm_forecaster" in makefile
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/sources upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/llm_forecaster upload/llm_forecaster" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_source_io.py upload/orchestration/" in deploy_recipe
    assert (
        f"cp {ROOT}/src/orchestration/_llm_forecaster_io.py upload/orchestration/" in deploy_recipe
    )
    assert f"cp {ROOT}/src/_fb_types.py upload/" in deploy_recipe
    assert f"cp {ROOT}/src/_schemas.py upload/" in deploy_recipe


def test_manager_deploy_stages_runtime_requirements_and_shared_code():
    deploy_dir = ROOT / "src/orchestration/func_llm_forecaster_manager"
    makefile = (deploy_dir / "Makefile").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(deploy_dir), "deploy"],
        text=True,
    )

    assert "func-llm-forecaster-manager" in makefile
    assert "--service-account $(WORKFLOW_SERVICE_ACCOUNT)" in makefile
    assert 's/"main.py"/"main.py",' not in makefile
    assert "TEST_OR_PROD=$(if $(filter $(BUILD_ENV),prod),PROD,TEST)" in makefile
    assert "include $(ROOT_DIR)orchestration_upload.mk" in makefile
    assert "ORCHESTRATION_EXTRA_PACKAGES = llm_forecaster" in makefile
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/sources upload/" in deploy_recipe
    assert f"cp -r {ROOT}/src/llm_forecaster upload/llm_forecaster" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_source_io.py upload/orchestration/" in deploy_recipe
    assert f"cp {ROOT}/src/_fb_types.py upload/" in deploy_recipe
    assert f"cp {ROOT}/src/_schemas.py upload/" in deploy_recipe


def test_leaderboard_deploy_stages_llm_identity_dependencies():
    makefile = (ROOT / "src" / "leaderboard" / "Makefile").read_text()

    assert "LEADERBOARD_DEPENDENCIES = main.py llm_identities.py" in makefile
    assert "deploy-tournament : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "deploy-baseline : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "deploy-preliminary : $(LEADERBOARD_DEPENDENCIES)" in makefile
    assert "model_release_dates.csv" not in makefile
    assert "cp -r $(ROOT_DIR)src/llm_forecaster $1/" in makefile


def test_raw_question_set_readers_do_not_require_gitpython():
    for requirements_path in RAW_QUESTION_SET_REQUIREMENTS_WITHOUT_GITPYTHON:
        requirements = requirements_path.read_text()
        assert "GitPython" not in requirements, requirements_path
