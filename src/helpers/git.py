"""Functions to interact with git."""

import logging
import os
import shutil
import sys
import tempfile
from typing import Dict, List, Optional, Tuple

from git import Actor, Repo

from . import env, keys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clone(repo_url: str) -> Tuple[Repo, str, str]:
    """Clone a Git repository into a temporary directory with a temporary SSH key.

    Args:
        repo_url (str): The SSH URL of the repository to clone.

    Returns:
        Tuple[Repo, str, str]:
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
) -> None:
    """Clone a Git repository, add/update files, commit, and push to origin and optional mirrors.

    Args:
        repo_url (str): SSH URL of the repository to clone (e.g., "git@github.com:org/repo.git").
        files (Dict[str, str]): Mapping of local file paths to destination paths inside the repo.
        commit_message (str): Commit message to use when pushing changes.
        mirrors (Optional[List[str]]): List of additional repository URLs to push to as mirrors.
                                       If None, attempts to load from secrets.

    Returns:
        None. Exits with status 1 if an error is encountered while pushing.
    """
    if not mirrors:
        mirrors = keys.get_secret_that_may_not_exist("HUGGING_FACE_REPO_URL")
        mirrors = [mirrors] if mirrors else []

    repo, local_repo_dir, tmp_key_file_path = clone(repo_url=repo_url)

    for source, destination in files.items():
        full_destination_path = f"{local_repo_dir}/{destination}"
        os.makedirs(os.path.dirname(full_destination_path), exist_ok=True)
        if os.path.exists(full_destination_path):
            os.remove(full_destination_path)
        shutil.copy(source, full_destination_path, follow_symlinks=False)
        repo.index.add([destination])

    error_encountered = False
    author = Actor("ForecastBench bot", "benchmark@forecastingresearch.org")
    committer = Actor("ForecastBench bot", "benchmark@forecastingresearch.org")
    ssh_env = {"GIT_SSH_COMMAND": f"ssh -i {tmp_key_file_path} -o StrictHostKeyChecking=no"}
    try:
        repo.index.commit(commit_message, author=author, committer=committer)
        origin = repo.remote(name="origin")
        origin.push(env=ssh_env)
        for index, mirror_url in enumerate(mirrors):
            mirror = repo.create_remote(f"mirror_{index}", url=mirror_url)
            mirror.push(env=ssh_env)
            repo.delete_remote(mirror.name)
            logger.info(f"Pushed to {mirror_url} (mirror) with commit message: {commit_message}")
    except Exception as e:
        error_encountered = True
        message = e.message if hasattr(e, "message") else str(e)
        logger.error(f"encountered error when pushing to git: {message}")

    os.remove(tmp_key_file_path)
    shutil.rmtree(local_repo_dir, ignore_errors=True)

    if error_encountered:
        sys.exit(1)

    logger.info(f"Pushed to {repo_url} with commit message: {commit_message}")


def clone_commit_and_push(
    files: Dict[str, str],
    commit_message: str,
) -> None:
    """Upload files files to Cloud Storage and push updates to Git.

    Args:
        files (Dict[str, str]): Mapping of local file paths to their git location.

    Returns:
        None
    """
    if env.RUNNING_LOCALLY:
        return

    clone_and_push_files(
        repo_url=keys.API_GITHUB_DATASET_REPO_URL,
        files=files,
        commit_message=commit_message,
    )
