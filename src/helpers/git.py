"""Functions to interact with git."""

import logging
import os
import shutil
import sys
import tempfile
from typing import Dict, List, Optional, Tuple

from git import Actor, Repo

from . import constants, keys, slack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clone(repo_url: str) -> Optional[Tuple[Repo, str, str]]:
    """Clone a Git repository into a temporary directory with a temporary SSH key.

    Args:
        repo_url (str): The SSH URL of the repository to clone.

    Returns:
        Optional[Tuple[Repo, str, str]]: None if the SSH key secret is not set, otherwise:
            - repo (Repo): The cloned GitPython Repo object.
            - local_repo_dir (str): The temporary directory where the repository was cloned.
            - tmp_key_file_path (str): Path to the temporary SSH private key used for cloning.
    """
    """Clone the git repository given by repo_url."""
    github_ssh_id_rsa = keys.get_secret_that_may_not_exist("API_GITHUB_SSH_ID_RSA")
    if github_ssh_id_rsa is None:
        logger.info(f"Not pushing to {repo_url}, ssh id_rsa not set.")
        return

    with tempfile.NamedTemporaryFile(delete=False, dir="/tmp") as tmp_key_file:
        tmp_key_file.write(github_ssh_id_rsa.encode())
        tmp_key_file_path = tmp_key_file.name
    os.chmod(tmp_key_file_path, 0o600)

    local_repo_dir = tempfile.mkdtemp(prefix="forecastbench-git-repo-", dir="/tmp")
    if os.path.exists(local_repo_dir):
        shutil.rmtree(local_repo_dir)

    logger.info(f"Cloning {repo_url}...")
    repo = Repo.clone_from(
        repo_url,
        local_repo_dir,
        branch="main",
        env={"GIT_SSH_COMMAND": f"ssh -i {tmp_key_file_path} -o StrictHostKeyChecking=no"},
    )
    return repo, local_repo_dir, tmp_key_file_path


def clone_and_push_files(
    repo_url: str,
    files: Dict[str, str],
    commit_message: str,
    mirrors: Optional[List[str]] = None,
) -> bool:
    """Clone a Git repository, add/update files, commit, and push to origin and optional mirrors.

    Args:
        repo_url (str): SSH URL of the repository to clone (e.g., "git@github.com:org/repo.git").
        files (Dict[str, str]): Mapping of local file paths to destination paths inside the repo.
        commit_message (str): Commit message to use when pushing changes.
        mirrors (Optional[List[str]]): List of additional repository URLs to push to as mirrors.
                                       If None, attempts to load from secrets.

    Returns:
        bool: True if a new commit was created, False if HEAD was unchanged or if the SSH key
              secret is not set, in which case nothing is cloned or pushed. Origin and mirrors
              are pushed either way. Exits with status 1 if the push to origin fails; a failed
              mirror push is logged and sent to Slack as a warning.
    """
    if not mirrors:
        mirrors = keys.get_secret_that_may_not_exist("HUGGING_FACE_REPO_URL")
        mirrors = [mirrors] if mirrors else []

    cloned = clone(repo_url=repo_url)
    if cloned is None:
        return False
    repo, local_repo_dir, tmp_key_file_path = cloned

    for source, destination in files.items():
        full_destination_path = f"{local_repo_dir}/{destination}"
        os.makedirs(os.path.dirname(full_destination_path), exist_ok=True)
        if os.path.exists(full_destination_path):
            os.remove(full_destination_path)
        shutil.copy(source, full_destination_path, follow_symlinks=False)
        repo.index.add([destination])

    # Skip empty commits, but always push so a lagging mirror catches up.
    has_changes = bool(repo.index.diff(repo.head.commit))
    if not has_changes:
        logger.info(f"No new commit for {repo_url}; syncing remotes to HEAD.")

    error_encountered = False
    author = Actor("ForecastBench bot", constants.BENCHMARK_EMAIL)
    committer = Actor("ForecastBench bot", constants.BENCHMARK_EMAIL)
    ssh_env = {"GIT_SSH_COMMAND": f"ssh -i {tmp_key_file_path} -o StrictHostKeyChecking=no"}
    try:
        if has_changes:
            repo.index.commit(commit_message, author=author, committer=committer)
        origin = repo.remote(name="origin")
        # A rejected push does not raise in GitPython; surface it explicitly.
        origin.push(env=ssh_env).raise_if_error()
    except Exception as e:
        error_encountered = True
        message = e.message if hasattr(e, "message") else str(e)
        logger.error(f"encountered error when pushing to git: {message}")

    # Mirrors are convenience copies; the repo at `repo_url` is the source of truth. A mirror
    # that's down or has diverged must not fail the nightly run, so warn on Slack and carry on.
    if not error_encountered:
        for index, mirror_url in enumerate(mirrors):
            try:
                mirror = repo.create_remote(f"mirror_{index}", url=mirror_url)
                mirror.push(env=ssh_env).raise_if_error()
                repo.delete_remote(mirror.name)
                logger.info(f"Pushed to {mirror_url} (mirror)")
            except Exception as e:
                error = e.message if hasattr(e, "message") else str(e)
                message = f"Could not push to {mirror_url} (mirror): {error}"
                logger.warning(message)
                slack.send_message(message=f"*MIRROR PUSH FAILED*\n{message}")

    os.remove(tmp_key_file_path)
    shutil.rmtree(local_repo_dir, ignore_errors=True)

    if error_encountered:
        sys.exit(1)

    if has_changes:
        logger.info(f"Pushed to {repo_url} with commit message: {commit_message}")
    return has_changes
