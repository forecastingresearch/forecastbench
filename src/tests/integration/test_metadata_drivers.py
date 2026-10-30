"""Integration: the ``metadata`` tag and validate drivers against a ``local_bucket``.

The LLM (``metadata_llm.get_metadata_model_response``), the Jev language classifier and the
per-source ``time.sleep`` are mocked; only the seeded sources contribute rows.
"""

from unittest.mock import patch

import pandas as pd

from helpers import constants, question_curation
from metadata import _tag as tag
from metadata import _validate as validate
from orchestration.func_metadata_tag import main as tag_job
from orchestration.func_metadata_validate import main as validate_job
from tests.factories import FakeJevClient, make_question_df


def _seed_bank(local_bucket, source: str, rows: list[dict]) -> None:
    local_bucket.seed_questions(source, make_question_df(rows).to_dict("records"))


def _read_metadata(local_bucket) -> pd.DataFrame:
    return pd.read_json(local_bucket.question_bank_dir() / constants.META_DATA_FILENAME, lines=True)


def _meta_row(
    source: str,
    id_: str,
    category: str = "",
    valid_question: object = "",
    english: object = "",
) -> dict:
    """A pre-existing question_metadata.jsonl row (the handoff artifact's full column set)."""
    return {
        "source": source,
        "id": id_,
        "category": category,
        "valid_question": valid_question,
        "english": english,
    }


class TestTagDriver:
    """Reads each source's bank, assigns categories (LLM or hard-coded), writes the metadata file."""

    def test_writes_categories_via_llm_and_hardcoded_sources(self, local_bucket):
        local_bucket.seed_metadata([])  # start from known-empty metadata (avoid stale /tmp)
        _seed_bank(
            local_bucket,
            "manifold",
            [{"id": "manifold1", "question": "Will X?", "background": "b"}],
        )
        _seed_bank(local_bucket, "fred", [{"id": "CPI", "question": "Will CPI rise?"}])
        _seed_bank(local_bucket, "acled", [{"id": "war", "question": "Battles in Y?"}])

        # Scope the driver to the seeded sources; it reads only the keys.
        sources = {"manifold": {}, "fred": {}, "acled": {}}
        jev = FakeJevClient({"Will X?": "not_english"})
        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources),
            patch.object(
                tag.metadata_llm, "get_metadata_model_response", return_value="Science & Tech"
            ),
            patch.object(tag, "get_client", return_value=jev),
            patch.object(tag_job.time, "sleep"),
        ):
            tag_job.driver(None)

        meta = _read_metadata(local_bucket)
        by_id = dict(zip(meta["id"].astype(str), meta["category"]))
        assert by_id["manifold1"] == "Science & Tech"  # manifold: LLM
        assert by_id["CPI"] == "Economics & Business"  # fred: hard-coded
        assert by_id["war"] == "Security & Defense"  # acled: hard-coded
        assert set(meta["category"]) <= set(constants.QUESTION_CATEGORIES)
        # The language flag: market questions are classified (Jev mocked), data sources are English.
        english = dict(zip(meta["id"].astype(str), meta["english"]))
        assert english == {"manifold1": False, "CPI": True, "war": True}
        assert jev.asked == ["Will X?"]


class TestValidateDriver:
    """Reads each source's bank, validates (LLM or auto), writes valid_question to the metadata file."""

    def test_writes_validity_via_llm_and_auto_sources(self, local_bucket):
        local_bucket.seed_metadata([])  # start from known-empty metadata (avoid stale /tmp)
        _seed_bank(
            local_bucket,
            "manifold",
            [
                {"id": "good", "question": "Reasonable public-interest question?"},
                {"id": "bad", "question": "Inappropriate personal question?"},
            ],
        )
        _seed_bank(local_bucket, "fred", [{"id": "CPI", "question": "Will CPI rise?"}])

        def fake_model(prompt, max_output_tokens=None) -> str:
            assert "Will CPI rise?" not in prompt, "fred (a data source) must bypass the LLM"
            return "Classification: flag" if "Inappropriate" in prompt else "Classification: ok"

        sources = {"manifold": {}, "fred": {}}
        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources),
            patch.object(
                validate.metadata_llm, "get_metadata_model_response", side_effect=fake_model
            ) as mock_model,
            patch.object(validate_job.time, "sleep"),
        ):
            validate_job.driver(None)

        meta = _read_metadata(local_bucket)
        by_id = dict(zip(meta["id"].astype(str), meta["valid_question"]))
        assert by_id["good"] is True
        assert by_id["bad"] is False
        assert by_id["CPI"] is True
        assert mock_model.call_count == 2  # only the manifold rows reach the LLM
        assert meta["valid_question"].dtype == bool


class TestTagDriverStatefulBehavior:
    """The tag driver tags only new questions and prunes rows whose question left the bank.

    The gate is ``category == ""``, so any category already set, ``"Other"`` included, is kept.
    """

    def _run_tag(self, local_bucket, sources: dict, **mock_kwargs: object) -> object:
        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources),
            patch.object(
                tag.metadata_llm, "get_metadata_model_response", **mock_kwargs
            ) as mock_llm,
            patch.object(tag, "get_client", return_value=FakeJevClient()),
            patch.object(tag_job.time, "sleep"),
        ):
            tag_job.driver(None)
        return mock_llm

    def test_existing_tags_kept_only_new_questions_hit_the_llm(self, local_bucket):
        local_bucket.seed_metadata([_meta_row("manifold", "manifold1", "Science & Tech", True)])
        _seed_bank(
            local_bucket,
            "manifold",
            [{"id": "manifold1", "question": "Q1?"}, {"id": "manifold2", "question": "Q2?"}],
        )

        mock_llm = self._run_tag(
            local_bucket, {"manifold": {}}, return_value="Politics & Governance"
        )

        meta = _read_metadata(local_bucket)
        cat = dict(zip(meta["id"].astype(str), meta["category"]))
        assert cat["manifold1"] == "Science & Tech"
        assert cat["manifold2"] == "Politics & Governance"
        assert mock_llm.call_count == 1
        valid = dict(zip(meta["id"].astype(str), meta["valid_question"]))
        assert valid["manifold1"] is True
        assert not meta.duplicated(subset=["source", "id"]).any()
        assert len(meta) == 2

    def test_drops_metadata_for_questions_removed_from_the_bank(self, local_bucket):
        local_bucket.seed_metadata(
            [
                _meta_row("manifold", "manifold1", "Science & Tech", True),
                _meta_row("manifold", "gone", "Sports", True),
            ]
        )
        _seed_bank(local_bucket, "manifold", [{"id": "manifold1", "question": "Q1?"}])

        mock_llm = self._run_tag(local_bucket, {"manifold": {}}, return_value="Other")

        meta = _read_metadata(local_bucket)
        ids = set(meta["id"].astype(str))
        assert ids == {"manifold1"}
        assert mock_llm.call_count == 0

    def test_other_category_persists_across_a_rerun(self, local_bucket):
        # "Other" is a real member of QUESTION_CATEGORIES, so it must not be re-tagged.
        local_bucket.seed_metadata([_meta_row("manifold", "manifold1", "Other", True)])
        _seed_bank(local_bucket, "manifold", [{"id": "manifold1", "question": "Q1?"}])

        mock_llm = self._run_tag(local_bucket, {"manifold": {}}, return_value="Science & Tech")

        meta = _read_metadata(local_bucket)
        assert dict(zip(meta["id"].astype(str), meta["category"]))["manifold1"] == "Other"
        assert mock_llm.call_count == 0


class TestTagDriverLanguageRetry:
    """A failed language classification is kept as "" for the next run, with its category intact."""

    def test_failed_classification_is_retried_without_repaying_the_category(self, local_bucket):
        local_bucket.seed_metadata([])
        _seed_bank(
            local_bucket,
            "manifold",
            [{"id": "m1", "question": "Q1?"}, {"id": "m2", "question": "Q2?"}],
        )
        sources = {"manifold": {}}

        failing = FakeJevClient({"Q1?": RuntimeError("Jev unavailable")})
        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources),
            patch.object(
                tag.metadata_llm, "get_metadata_model_response", return_value="Sports"
            ) as mock_llm,
            patch.object(tag, "get_client", return_value=failing),
            patch.object(tag_job.time, "sleep"),
        ):
            tag_job.driver(None)
        meta = _read_metadata(local_bucket).set_index("id")
        assert meta["english"].tolist() == ["", True]  # m1 left blank: to be retried
        assert meta.loc["m1", "category"] == "Sports"
        assert mock_llm.call_count == 2

        healthy = FakeJevClient()
        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", sources),
            patch.object(
                tag.metadata_llm, "get_metadata_model_response", return_value="Sports"
            ) as mock_llm,
            patch.object(tag, "get_client", return_value=healthy),
            patch.object(tag_job.time, "sleep"),
        ):
            tag_job.driver(None)
        meta = _read_metadata(local_bucket).set_index("id")
        assert healthy.asked == ["Q1?"]
        assert mock_llm.call_count == 0
        assert meta["english"].tolist() == [True, True]


class TestValidateDriverStatefulBehavior:
    """The validate driver is likewise incremental: an existing verdict is never re-validated."""

    def test_existing_verdicts_kept_only_new_questions_hit_the_llm(self, local_bucket):
        local_bucket.seed_metadata([_meta_row("manifold", "good", "Science & Tech", True)])
        _seed_bank(
            local_bucket,
            "manifold",
            [{"id": "good", "question": "Q1?"}, {"id": "fresh", "question": "Q2?"}],
        )

        with (
            patch.object(question_curation, "FREEZE_QUESTION_SOURCES", {"manifold": {}}),
            patch.object(
                validate.metadata_llm,
                "get_metadata_model_response",
                return_value="Classification: ok",
            ) as mock_llm,
            patch.object(validate_job.time, "sleep"),
        ):
            validate_job.driver(None)

        meta = _read_metadata(local_bucket)
        valid = dict(zip(meta["id"].astype(str), meta["valid_question"]))
        assert valid["good"] is True and valid["fresh"] is True
        assert mock_llm.call_count == 1
        assert dict(zip(meta["id"].astype(str), meta["category"]))["good"] == "Science & Tech"
