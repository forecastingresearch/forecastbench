"""The validate job: one cumulative upload per source, and the deploy layout."""

import importlib
import re
import subprocess
from pathlib import Path

import pandas as pd

from helpers import constants, metadata_llm, metadata_prompts
from tests.factories import make_question_df

ROOT = Path(__file__).resolve().parents[4]
META_COLUMNS = constants.META_DATA_FILE_COLUMNS

main = importlib.import_module("orchestration.func_metadata_validate.main")


def _response(classification):
    return f"Insert thinking:\nThe question is clear.\nClassification:\n{classification}"


def test_driver_uploads_the_cumulative_metadata_after_every_source(monkeypatch):
    """A crash on a later source must not lose the verdicts of the sources already done."""
    loaded = pd.DataFrame(
        [
            ("manifold", "m1", "Sports", True, True),  # not sampled any more: stays as it is
            ("kalshi", "k1", "Politics & Governance", True, True),
        ],
        columns=META_COLUMNS,
    )
    questions = {
        "fred": make_question_df(
            [
                {"id": "DGS10", "question": "Will DGS10 increase?"},
                {"id": "UNRATE", "question": "Will UNRATE increase?"},
            ]
        ),
        "kalshi": make_question_df(
            [
                {"id": "k1", "question": "Will the Fed cut rates in 2027?"},
                {"id": "k2", "question": "Will I pass my exam?"},
                {"id": "k3", "question": "Will Israel invade Rafah in May 2027?"},
            ]
        ),
    }
    responses = {
        metadata_prompts.VALIDATE_QUESTION_PROMPT.format(question=question): response
        for question, response in [
            ("Will I pass my exam?", _response("flag")),
            ("Will Israel invade Rafah in May 2027?", _response("ok")),
        ]
    }
    monkeypatch.setattr(metadata_llm, "_get_metadata_model_run", lambda: None)
    monkeypatch.setattr(
        metadata_llm,
        "get_metadata_model_response",
        lambda prompt, max_output_tokens=None: responses[prompt],
    )
    monkeypatch.setattr(
        main.question_curation, "FREEZE_QUESTION_SOURCES", {"fred": {}, "kalshi": {}}
    )
    monkeypatch.setattr(main._io, "load_question_metadata", lambda: loaded.copy())

    def get_questions(source, return_question_data=False, **_):
        assert return_question_data
        return questions[source].copy()

    monkeypatch.setattr(main.data_utils, "get_data_from_cloud_storage", get_questions)
    events = []
    monkeypatch.setattr(
        main._io,
        "upload_question_metadata",
        lambda dfmeta: events.append(("upload", dfmeta.copy())),
    )
    monkeypatch.setattr(main.time, "sleep", lambda seconds: events.append(("sleep", seconds)))

    main.driver(None)

    # GCS allows one write per second to the same object.
    assert [kind for kind, _ in events] == ["upload", "sleep", "upload", "sleep"]
    assert [value for kind, value in events if kind == "sleep"] == [2, 2]
    after_fred, after_kalshi = [value for kind, value in events if kind == "upload"]
    assert after_fred[["source", "id", "valid_question"]].values.tolist() == [
        ["fred", "DGS10", True],
        ["fred", "UNRATE", True],
        ["kalshi", "k1", True],
        ["manifold", "m1", True],
    ]
    assert list(after_kalshi.columns) == META_COLUMNS
    assert list(after_kalshi.index) == list(range(6))
    assert after_kalshi.to_dict("records") == [
        {"source": "fred", "id": "DGS10", "category": "", "valid_question": True, "english": ""},
        {"source": "fred", "id": "UNRATE", "category": "", "valid_question": True, "english": ""},
        {
            "source": "kalshi",
            "id": "k1",
            "category": "Politics & Governance",
            "valid_question": True,
            "english": True,
        },
        {"source": "kalshi", "id": "k2", "category": "", "valid_question": False, "english": ""},
        {"source": "kalshi", "id": "k3", "category": "", "valid_question": True, "english": ""},
        {
            "source": "manifold",
            "id": "m1",
            "category": "Sports",
            "valid_question": True,
            "english": True,
        },
    ]


def test_validate_deploy_keeps_the_job_name_and_stages_the_metadata_package():
    deploy_dir = ROOT / "src/orchestration/func_metadata_validate"
    makefile = (deploy_dir / "Makefile").read_text()
    deploy_recipe = subprocess.check_output(
        ["make", "-n", "-C", str(deploy_dir), "deploy"],
        text=True,
    )
    root_makefile = (ROOT / "Makefile").read_text()

    # The nightly worker dispatches the job by this exact name.
    job_name = re.search(r"gcloud run jobs deploy \\\s+(\S+)", deploy_recipe).group(1)
    assert job_name == "func-metadata-validate-questions"
    assert "--service-account $(QUESTION_BANK_BUCKET_SERVICE_ACCOUNT)" in makefile
    assert "include $(ROOT_DIR)orchestration_upload.mk" in makefile
    assert "ORCHESTRATION_EXTRA_PACKAGES = metadata" in makefile
    assert f"cp -r {ROOT}/src/metadata upload/metadata" in deploy_recipe
    assert f"cp -r {ROOT}/src/helpers upload/" in deploy_recipe
    assert f"cp {ROOT}/src/orchestration/_io.py upload/orchestration/" in deploy_recipe
    assert f"cp {ROOT}/src/_schemas.py upload/" in deploy_recipe
    assert (
        f"cat {ROOT}/requirements.runtime.txt requirements.txt > upload/requirements.txt"
        in deploy_recipe
    )
    assert "$(MAKE) -C src/orchestration/func_metadata_validate" in root_makefile
    assert "src/metadata/validate_questions" not in root_makefile
