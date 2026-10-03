"""End-to-end: the question-SET creation pipeline (distinct from the forecast-resolution e2e).

Exercises the ``metadata`` -> ``curate_questions`` flow against a ``local_bucket``, with the LLM
and the Jev language classifier mocked. Three complementary views:

1. The metadata drivers + curate's intake helpers (``test_invalid_questions_*`` /
   ``*_is_deterministic``): the real ``tag.driver`` and ``validate.driver`` write
   ``question_metadata.jsonl``; the tests then call curate's filter/sampling helpers directly:

       seed banks -> tag.driver (categories) -> validate.driver (validity)
           -> question_metadata.jsonl -> drop_invalid -> drop_missing_freeze -> human_sample

2. The real ``curate.driver`` on hand-written metadata (``test_curate_driver_builds_question_set``):
   seed banks + ``question_metadata.jsonl``, run the whole driver (filter chain -> allocate ->
   seeded sample -> write_questions), read the published ``<date>-{llm,human}.json`` back and
   **golden** the produced rows. Deterministic via ``env.RANDOM_SEED``.

3. The whole flow, driver to driver (``test_metadata_drivers_feed_curate_driver``): seed banks
   only -> real ``tag.driver`` + ``validate.driver`` -> the ``question_metadata.jsonl`` they persist
   -> real ``curate.driver`` -> published question sets. No metadata is hand-written.

The key end-to-end property: a question the validator flags (or the tagger can't categorize, or the
language classifier marks as not English) never reaches the published set, and the human set is a
subset of the LLM set. Stage logic lives in
unit/; this asserts the stages compose across the two jobs.
"""

import contextlib
from datetime import date, datetime
from unittest.mock import patch

import pandas as pd

from curate_questions.create_question_set import main as curate
from curate_questions.create_question_set.main import (
    drop_invalid_questions,
    drop_missing_freeze_datetime,
    drop_questions_not_in_english,
    human_sample_questions,
)
from helpers import constants, question_curation
from metadata.tag_questions import main as tag
from metadata.validate_questions import main as validate
from tests._golden import check_golden
from tests.factories import FakeJevClient, make_question_df

_SOURCES = {"polymarket": {}, "fred": {}}
_MANIFOLD = [
    {"id": "good1", "question": "Public-interest Q1?", "freeze_datetime_value": "0.5"},
    {"id": "good2", "question": "Public-interest Q2?", "freeze_datetime_value": "0.4"},
    {"id": "bad1", "question": "Inappropriate personal Q?", "freeze_datetime_value": "0.3"},
]
_FRED = [
    {"id": "fred1", "question": "Will CPI rise?", "freeze_datetime_value": "100"},
    {"id": "fred2", "question": "Will GDP rise?", "freeze_datetime_value": "200"},
]


def _run_metadata_jobs(
    local_bucket,
    sources: dict = _SOURCES,
    flag: str = "Inappropriate",
    uncategorizable: str | None = None,
    not_english: str | None = None,
) -> pd.DataFrame:
    """Run the real tag then validate drivers (LLM + Jev mocked); return question_metadata.jsonl.

    Args:
      local_bucket (LocalBucket): The seeded temp bucket.
      sources (dict): Stand-in for ``FREEZE_QUESTION_SOURCES`` (the sources both drivers walk).
      flag (str): Questions whose text contains this get "Classification: flag" from the validator.
      uncategorizable (str | None): Questions whose text contains this get a non-category answer
        from the tagger (so the driver records ``Other``); every other LLM-tagged question gets
        "Politics & Governance".
      not_english (str | None): The one market question text the language classifier answers
        ``not_english`` for; every other market question is classified as English.
    """

    def fake_tag(prompt, max_output_tokens=None) -> str:
        if uncategorizable is not None and uncategorizable in prompt:
            return "Not a real category"
        return "Politics & Governance"

    def fake_validate(prompt, max_output_tokens=None) -> str:
        return "Classification: flag" if flag in prompt else "Classification: ok"

    jev = FakeJevClient({} if not_english is None else {not_english: "not_english"})
    with patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources), patch.object(
        tag.metadata_llm, "get_metadata_model_response", side_effect=fake_tag
    ), patch.object(tag, "get_client", return_value=jev), patch.object(tag.time, "sleep"):
        tag.driver(None)

    with patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources), patch.object(
        validate.metadata_llm, "get_metadata_model_response", side_effect=fake_validate
    ), patch.object(validate.time, "sleep"):
        validate.driver(None)

    return pd.read_json(local_bucket.question_bank_dir() / constants.META_DATA_FILENAME, lines=True)


def _valid_questions(local_bucket, meta: pd.DataFrame) -> pd.DataFrame:
    """Curate's intake: per source, drop invalid, non-English + freeze-less questions, then concat."""
    kept = []
    for source in _SOURCES:
        dfq = local_bucket.read_questions(source)
        dfq["source"] = source
        dfq = drop_invalid_questions(dfq, meta)
        dfq = drop_questions_not_in_english(dfq)
        dfq = drop_missing_freeze_datetime(dfq)
        kept.append(dfq)
    return pd.concat(kept, ignore_index=True)


def test_invalid_questions_never_reach_the_sampled_set(local_bucket):
    local_bucket.seed_metadata([])  # start from known-empty metadata (avoid stale /tmp)
    local_bucket.seed_questions("polymarket", make_question_df(_MANIFOLD).to_dict("records"))
    local_bucket.seed_questions("fred", make_question_df(_FRED).to_dict("records"))

    # --- Stage 1: metadata (tag + validate) produces the handoff artifact ---
    meta = _run_metadata_jobs(local_bucket)
    cat = dict(zip(meta["id"].astype(str), meta["category"]))
    valid = dict(zip(meta["id"].astype(str), meta["valid_question"]))
    assert cat["good1"] == "Politics & Governance"  # polymarket -> LLM (mocked)
    assert cat["fred1"] == "Economics & Business"  # fred -> hard-coded
    assert valid["good1"] is True and valid["bad1"] is False  # validator flagged the bad one
    assert meta["english"].all()  # markets classified (Jev mocked), datasets English by rule

    # --- Stage 2: curate's intake helpers consume the metadata; invalid is filtered out ---
    llm_pool = _valid_questions(local_bucket, meta)
    assert "bad1" not in set(llm_pool["id"])  # the flagged question is gone end to end
    assert {"good1", "good2", "fred1", "fred2"} == set(llm_pool["id"])

    # --- Stage 3: the human set is a deterministic subset of the LLM set ---
    human = human_sample_questions({"dfq": llm_pool}, 2, random_state=0)
    assert len(human) == 2
    assert set(human["id"]) <= set(llm_pool["id"])  # humans ⊆ LLM


def test_question_set_pipeline_is_deterministic(local_bucket):
    local_bucket.seed_metadata([])  # start from known-empty metadata (avoid stale /tmp)
    local_bucket.seed_questions("polymarket", make_question_df(_MANIFOLD).to_dict("records"))
    local_bucket.seed_questions("fred", make_question_df(_FRED).to_dict("records"))
    meta = _run_metadata_jobs(local_bucket)
    pool = _valid_questions(local_bucket, meta)

    first = human_sample_questions({"dfq": pool}, 2, random_state=7)
    second = human_sample_questions({"dfq": pool}, 2, random_state=7)
    pd.testing.assert_frame_equal(
        first.sort_values("id").reset_index(drop=True),
        second.sort_values("id").reset_index(drop=True),
    )


# ---------------------------------------------------------------------------
# The real curate.driver: build + publish a question set (golden), then the same driver fed by the
# metadata drivers' persisted artifact.
# ---------------------------------------------------------------------------

# Fixed dates so the published filename + resolution dates (and thus the golden) are deterministic.
_CURATE_FREEZE = datetime(2025, 1, 1)
_CURATE_FORECAST = datetime(2025, 1, 11)  # FREEZE + FREEZE_WINDOW_IN_DAYS (10)
_CURATE_FORECAST_DATE = date(2025, 1, 11)
_CURATE_CLOSE = "2025-03-12T00:00:00"  # FORECAST + 60d → a valid horizon bin; not "too soon"
# One source per question type, so each takes its type's whole sampling share.
_CURATE_SOURCES = {
    "polymarket": {
        "name": "Polymarket",
        "source_intro": "Polymarket intro.",
        "resolution_criteria": "Resolves per {url}.",
        "sampling_share": 1.0,
    },
    "fred": {
        "name": "FRED",
        "source_intro": "FRED intro.",
        "resolution_criteria": "Resolves per {url}.",
        "sampling_share": 1.0,
    },
}
# Per question type (market = polymarket, dataset = fred): 4 + 4 LLM questions, 2 + 2 for humans.
_CURATE_NUM_LLM_BY_TYPE = {"market": 4, "dataset": 4}
_CURATE_NUM_HUMAN_BY_TYPE = {"market": 2, "dataset": 2}
_CURATE_GOLDEN_COLS = [
    "source",
    "id",
    "question",
    "freeze_datetime_value",
    "freeze_datetime",
    "market_info_close_datetime",
    "resolution_criteria",
    "source_intro",
]


def _seed_curate_banks(local_bucket) -> None:
    """8 markets + 8 datasets; ``polymarket1`` is resolved, ``fred3`` has no freeze value."""
    polymarket = make_question_df(
        [
            {
                "id": f"polymarket{i}",
                "source": "polymarket",
                "question": f"Market question {i}?",
                "url": f"https://example.com/polymarket{i}",
                "resolved": i == 1,  # polymarket1 is resolved → excluded
                "freeze_datetime_value": "0.5",
                "market_info_close_datetime": _CURATE_CLOSE,
            }
            for i in range(8)
        ]
    ).to_dict("records")
    fred = make_question_df(
        [
            {
                "id": f"fred{i}",
                "source": "fred",
                "question": f"Data question {i}?",
                "url": f"https://example.com/fred{i}",
                "resolved": False,
                "freeze_datetime_value": "N/A" if i == 3 else str(100 + i),  # fred3 → excluded
                "forecast_horizons": [7, 30],  # non-empty → survives the resolve-too-soon filter
            }
            for i in range(8)
        ]
    ).to_dict("records")
    local_bucket.seed_questions("polymarket", polymarket)
    local_bucket.seed_questions("fred", fred)


def _seed_curate_metadata(local_bucket) -> None:
    """Hand-written metadata exercising every exclusion once per question type.

    Markets: ``polymarket0`` invalid, ``polymarket2`` not English, ``polymarket3`` category
    ``Other`` (plus ``polymarket1`` resolved in the bank). Datasets: ``fred0`` ``Other``, ``fred1``
    invalid, ``fred2`` with no language classification yet (``""``, a failed Jev call awaiting
    retry) (plus ``fred3`` without a freeze value in the bank). That leaves exactly the configured
    4 + 4 questions eligible, so the published set is fully determined and every exclusion is
    load-bearing: with slack in the pool a seeded draw could skip an excluded id by chance.
    """
    meta = [
        {
            "source": "polymarket",
            "id": f"polymarket{i}",
            "category": "Other" if i == 3 else "Politics & Governance",
            "valid_question": i != 0,
            "english": i != 2,
        }
        for i in range(8)
    ] + [
        {
            "source": "fred",
            "id": f"fred{i}",
            "category": "Other" if i == 0 else "Economics & Business",
            "valid_question": i != 1,
            "english": "" if i == 2 else True,
        }
        for i in range(8)
    ]
    local_bucket.seed_metadata(meta)


_CURATE_EXPECTED_LLM_IDS = {f"polymarket{i}" for i in range(4, 8)} | {
    f"fred{i}" for i in range(4, 8)
}


# What the mocked metadata LLM / language classifier do in the driver-to-driver test. Only
# polymarket questions go to them (fred's category, validity and language are set by rule), so all
# three are markets.
_FLAGGED_QUESTION = ("polymarket0", "Market question 0?")  # validator answers "flag"
_UNCATEGORIZED_QUESTION = ("polymarket2", "Market question 2?")  # tagger answers a non-category
_NOT_ENGLISH_QUESTION = ("polymarket3", "Market question 3?")  # classifier answers "not_english"


def _run_curate_driver(
    local_bucket, monkeypatch, metadata_from_drivers: bool = False
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Seed the banks, produce metadata, run the real curate driver; return (llm, human) sets.

    Args:
      local_bucket (LocalBucket): The temp bucket.
      monkeypatch (pytest.MonkeyPatch): For the env vars the driver reads.
      metadata_from_drivers (bool): If False, seed the hand-written ``_seed_curate_metadata``. If
        True, start from empty metadata and let the real tag + validate drivers write it (LLM
        mocked to flag ``_FLAGGED_QUESTION`` and leave ``_UNCATEGORIZED_QUESTION`` as ``Other``,
        Jev mocked to mark ``_NOT_ENGLISH_QUESTION`` as not English).
    """
    _seed_curate_banks(local_bucket)
    if metadata_from_drivers:
        local_bucket.seed_metadata([])  # known-empty start (the drivers read a /tmp copy)
        _run_metadata_jobs(
            local_bucket,
            sources=_CURATE_SOURCES,
            flag=_FLAGGED_QUESTION[1],
            uncategorizable=_UNCATEGORIZED_QUESTION[1],
            not_english=_NOT_ENGLISH_QUESTION[1],
        )
    else:
        _seed_curate_metadata(local_bucket)

    monkeypatch.delenv("RUNNING_LOCALLY", raising=False)  # so write_questions writes to the bucket
    monkeypatch.setenv("RANDOM_SEED", "0")  # deterministic sampling → stable golden
    qc = curate.question_curation
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(qc, "FREEZE_QUESTION_SOURCES", _CURATE_SOURCES))
        stack.enter_context(
            patch.object(qc, "FREEZE_NUM_LLM_QUESTIONS_BY_TYPE", _CURATE_NUM_LLM_BY_TYPE)
        )
        stack.enter_context(
            patch.object(qc, "FREEZE_NUM_HUMAN_QUESTIONS_BY_TYPE", _CURATE_NUM_HUMAN_BY_TYPE)
        )
        stack.enter_context(patch.object(qc, "FREEZE_DATETIME", _CURATE_FREEZE))
        stack.enter_context(patch.object(qc, "FORECAST_DATETIME", _CURATE_FORECAST))
        stack.enter_context(patch.object(qc, "FORECAST_DATE", _CURATE_FORECAST_DATE))
        stack.enter_context(patch.object(qc, "is_today_question_curation_date", return_value=True))
        curate.driver(None)

    def _read(target: str) -> pd.DataFrame:
        payload = local_bucket.read_json("QUESTION_SETS_BUCKET", f"2025-01-11-{target}.json")
        return pd.DataFrame(payload["questions"])

    return _read("llm"), _read("human")


def test_curate_driver_builds_question_set(local_bucket, monkeypatch):
    llm, human = _run_curate_driver(local_bucket, monkeypatch)

    # --- Anchors: the filter chain is exact ---
    # Exactly the eligible questions are published (see _seed_curate_metadata): the invalid,
    # resolved, not-English, unclassified, "Other"-category and freeze-less questions are all gone.
    assert len(llm) == 8  # 4 market + 4 data (FREEZE_NUM_LLM_QUESTIONS_BY_TYPE per question type)
    assert set(llm["id"]) == _CURATE_EXPECTED_LLM_IDS
    assert (llm["source"] == "polymarket").sum() == 4 and (llm["source"] == "fred").sum() == 4

    # Enrichment: resolution_criteria URL-templated, source_intro + freeze_datetime stamped.
    assert llm["resolution_criteria"].str.contains("https://example.com/").all()
    assert set(llm["source_intro"]) == {"Polymarket intro.", "FRED intro."}
    assert (llm["freeze_datetime"] == "2025-01-01T00:00:00").all()
    # Dataset horizons → resolution dates; markets carry "N/A".
    assert llm[llm["source"] == "fred"]["resolution_dates"].iloc[0] == ["2025-01-18", "2025-02-10"]
    assert (llm[llm["source"] == "polymarket"]["resolution_dates"] == "N/A").all()

    # The human set is a subset of the LLM set, allocated evenly per question type.
    assert len(human) == 4  # 2 market + 2 data
    assert (human["source"] == "polymarket").sum() == 2 and (human["source"] == "fred").sum() == 2
    assert set(human["id"]) <= set(llm["id"])

    # --- Golden: freeze the produced question-set rows (scalar columns only) ---
    check_golden("question_set_llm", llm, key=["source", "id"], cols=_CURATE_GOLDEN_COLS, rtol=0)
    check_golden(
        "question_set_human", human, key=["source", "id"], cols=_CURATE_GOLDEN_COLS, rtol=0
    )


def test_metadata_drivers_feed_curate_driver(local_bucket, monkeypatch):
    llm, human = _run_curate_driver(local_bucket, monkeypatch, metadata_from_drivers=True)

    # The handoff artifact curate consumed was written by the metadata drivers, not by the test.
    meta = pd.read_json(
        local_bucket.question_bank_dir() / constants.META_DATA_FILENAME, lines=True
    ).set_index("id")
    assert len(meta) == 16  # every banked question was tagged + validated + language-classified
    assert not meta.loc[_FLAGGED_QUESTION[0], "valid_question"]
    assert meta.loc[_UNCATEGORIZED_QUESTION[0], "category"] == "Other"
    assert not meta.loc[_NOT_ENGLISH_QUESTION[0], "english"]
    assert meta.drop(index=_NOT_ENGLISH_QUESTION[0])["english"].all()

    # Flagged / uncategorized / non-English (and resolved) questions never reach a published set.
    for question_set in (llm, human):
        ids = set(question_set["id"])
        assert _FLAGGED_QUESTION[0] not in ids
        assert _UNCATEGORIZED_QUESTION[0] not in ids
        assert _NOT_ENGLISH_QUESTION[0] not in ids
        assert "polymarket1" not in ids  # resolved
    assert set(llm[llm["source"] == "polymarket"]["id"]) == {f"polymarket{i}" for i in range(4, 8)}

    # Counts per source match the configured per-question-type totals.
    for question_set, totals in (
        (llm, _CURATE_NUM_LLM_BY_TYPE),
        (human, _CURATE_NUM_HUMAN_BY_TYPE),
    ):
        assert (question_set["source"] == "polymarket").sum() == totals["market"]
        assert (question_set["source"] == "fred").sum() == totals["dataset"]

    assert set(human["id"]) <= set(llm["id"])  # humans ⊆ LLM
