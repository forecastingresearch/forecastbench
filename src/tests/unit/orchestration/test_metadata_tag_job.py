"""The tag job: one upload per sampled source, and the deploy wiring."""

import importlib
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from _schemas import MetadataFrame
from helpers import constants, metadata_llm, question_curation
from metadata import _tag as metadata_tag
from tests.factories import FakeJevClient, make_question_df

ROOT = Path(__file__).resolve().parents[4]
DEPLOY_DIR = ROOT / "src/orchestration/func_metadata_tag"


@pytest.fixture()
def tag_job():
    return importlib.import_module("orchestration.func_metadata_tag.main")


def _question_file(source):
    df = make_question_df(
        [{"id": f"{source}-1", "question": f"{source} question", "background": "N/A"}]
    )
    return df[constants.QUESTION_FILE_COLUMNS]


def test_driver_tags_every_sampled_source_and_uploads_the_metadata_after_each_one(
    monkeypatch, tag_job
):
    """A crash part-way through loses at most the source being tagged."""
    monkeypatch.setattr(metadata_llm, "_get_metadata_model_run", lambda: None)
    monkeypatch.setattr(metadata_llm, "get_metadata_model_response", lambda **kwargs: "Sports")
    monkeypatch.setattr(metadata_tag, "get_client", lambda: FakeJevClient())
    monkeypatch.setattr(
        tag_job._io,
        "load_question_metadata",
        lambda: pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
            constants.META_DATA_FILE_COLUMN_DTYPE
        ),
    )
    monkeypatch.setattr(
        tag_job.data_utils,
        "get_data_from_cloud_storage",
        lambda source, **kwargs: _question_file(source),
    )
    uploads = []
    monkeypatch.setattr(
        tag_job._io, "upload_question_metadata", lambda df: uploads.append(df.copy())
    )
    sleeps = []
    monkeypatch.setattr(tag_job.time, "sleep", sleeps.append)

    tag_job.driver(None)

    sources = list(question_curation.FREEZE_QUESTION_SOURCES)
    assert [sorted(set(df["source"])) for df in uploads] == [
        sorted(sources[: n + 1]) for n in range(len(sources))
    ]
    # GCS allows one write per second to the same object.
    assert len(sleeps) == len(uploads) and all(seconds >= 1 for seconds in sleeps)
    final = uploads[-1]
    assert list(final.columns) == constants.META_DATA_FILE_COLUMNS
    assert final["id"].tolist() == [f"{source}-1" for source in sorted(sources)]
    assert final["category"].isin(constants.QUESTION_CATEGORIES).all()
    assert final["english"].tolist() == [True] * len(sources)
    assert final["valid_question"].tolist() == [""] * len(sources)
    MetadataFrame.validate(final)


def test_tag_job_deploys_the_metadata_package_under_the_name_the_nightly_worker_dispatches():
    worker = (ROOT / "src/nightly_update_workflow/worker/main.py").read_text()
    root_makefile = (ROOT / "Makefile").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(DEPLOY_DIR), "deploy"],
        text=True,
    )
    gcloud_command = " ".join(
        line.strip().rstrip("\\").strip() for line in deploy_recipe.splitlines()
    )

    assert '"func-metadata-tag-questions"' in worker
    assert "gcloud run jobs deploy func-metadata-tag-questions --project" in gcloud_command
    assert "$(MAKE) -C src/orchestration/func_metadata_tag" in root_makefile
    assert "src/metadata/tag_questions" not in root_makefile
    assert f"cp -r {ROOT}/src/metadata upload/metadata" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert (
        f"cat {ROOT}/requirements.runtime.txt requirements.txt > upload/requirements.txt"
        in deploy_recipe
    )
