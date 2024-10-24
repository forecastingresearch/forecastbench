"""Functions to interact with git."""

import logging
import os
import shutil
import sys
import tempfile

from git import Actor, Repo

from . import keys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clone_and_push_files(repo_url, files, commit_message):
    """Clone a repository and push the files to it.

    Params:
    - repo_url: something like `git@github.com:forecastingresearch/...`
    - files: a dict of files to push; the key is the local path, the value is the repository path
    - commit_message: commit message
    """
    github_ssh_id_rsa = keys.get_secret_that_may_not_exist("API_GITHUB_SSH_ID_RSA")
    if github_ssh_id_rsa is None:
        logger.info(f"Not pushing to {repo_url}, ssh id_rsa not set.")
        return

    with tempfile.NamedTemporaryFile(delete=False) as tmp_key_file:
        tmp_key_file.write(github_ssh_id_rsa.encode())
        tmp_key_file_path = tmp_key_file.name
    os.chmod(tmp_key_file_path, 0o600)

    local_repo_dir = "/tmp/forecastbench-datasets"
    if os.path.exists(local_repo_dir):
        shutil.rmtree(local_repo_dir)

    repo = Repo.clone_from(
        repo_url,
        local_repo_dir,
        branch="main",
        env={"GIT_SSH_COMMAND": f"ssh -i {tmp_key_file_path} -o StrictHostKeyChecking=no"},
    )

    for source, destination in files.items():
        full_destination_path = f"{local_repo_dir}/{destination}"
        os.makedirs(os.path.dirname(full_destination_path), exist_ok=True)
        shutil.copy(source, full_destination_path)
        repo.index.add([destination])

    error_encountered = False
    author = Actor("ForecastBench bot", "benchmark@forecastingresearch.org")
    committer = Actor("ForecastBench bot", "benchmark@forecastingresearch.org")
    try:
        repo.index.commit(commit_message, author=author, committer=committer)
        origin = repo.remote(name="origin")
        origin.push(env={"GIT_SSH_COMMAND": f"ssh -i {tmp_key_file_path}"})
    except Exception as e:
        error_encountered = True
        logger.error(f"encountered error when pushing to git: {e.message}")

    os.remove(tmp_key_file_path)
    shutil.rmtree(local_repo_dir, ignore_errors=True)

    if error_encountered:
        sys.exit(1)

    logger.info(f"Pushed to {repo_url} with commit message: {commit_message}")
