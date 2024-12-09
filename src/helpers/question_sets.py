"""Helpful functions for dealing with question sets."""

import json
import os
import shutil
import sys
from contextlib import contextmanager

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))
from . import git, keys, resolution  # noqa: E402

LATEST_QUESTION_SET_FILENAME = "latest-llm.json"


@contextmanager
def temp_download_dataset_repo():
    """Context manager to ensure cleanup of gitrepo."""
    try:
        _, local_repo_dir, _ = git.clone(repo_url=keys.API_GITHUB_DATASET_REPO_URL)
        yield local_repo_dir
    finally:
        shutil.rmtree(local_repo_dir)


def get_field_from_question_set_file(filename, field):
    """Download value in `field` from question set `filename`."""
    with temp_download_dataset_repo() as local_repo_dir:
        latest_json_filename = f"{local_repo_dir}/datasets/question_sets/{filename}"
        with open(latest_json_filename, "r") as f:
            data = json.load(f)

        if field not in data:
            raise ValueError(f"Missing `{field}` key in question set file {filename}.")

        return data[field]


def get_field_from_latest_question_set_file(field):
    """Download value in `field` from `latest-llm.json`."""
    return get_field_from_question_set_file(LATEST_QUESTION_SET_FILENAME, field)


def download_and_read_question_set_file(filename):
    """Download questions from question set `filename`."""
    data = get_field_from_question_set_file(filename, "questions")
    return resolution.make_columns_hashable(pd.DataFrame(data))


def download_and_read_latest_question_set_file():
    """Download the questions from `latest-llm.json`."""
    return download_and_read_question_set_file(LATEST_QUESTION_SET_FILENAME)
