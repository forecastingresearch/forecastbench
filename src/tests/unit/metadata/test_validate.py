"""Question validation: which sources are auto-valid, how LLM verdicts map, how metadata is kept."""

import pandas as pd
import pandera.errors
import pytest

from _schemas import MetadataFrame
from helpers import constants, metadata_llm, metadata_prompts, question_curation
from metadata import validate
from tests.factories import make_question_df

META_COLUMNS = constants.META_DATA_FILE_COLUMNS

# The sampled sources that are judged by the LLM: every sampled source that is neither a dataset
# source nor metaculus.
LLM_VALIDATED_SOURCES = ["kalshi", "polymarket"]


class FakeLLM:
    """Stand-in for the metadata LLM.

    `responses` maps a question text to the response to return, or to an exception to raise.
    `asked` records the question every prompt was built for (or the whole prompt when it was
    not built for a known question). The prompt template quotes example questions, so the
    question is matched through the prompt built for it, not as a substring.
    """

    def __init__(self):
        self.responses = {}
        self.asked = []

    def __call__(self, prompt, max_output_tokens=None):
        by_prompt = {
            metadata_prompts.VALIDATE_QUESTION_PROMPT.format(question=q): q for q in self.responses
        }
        question = by_prompt.get(prompt)
        self.asked.append(prompt if question is None else question)
        response = self.responses[question]
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture()
def fake_llm(monkeypatch):
    """Route the metadata LLM, and the key prefetch that precedes it, to a FakeLLM."""
    fake = FakeLLM()
    monkeypatch.setattr(metadata_llm, "_get_metadata_model_run", lambda: None)
    monkeypatch.setattr(metadata_llm, "get_metadata_model_response", fake)
    return fake


def _response(classification):
    """A response in the structure the validation prompt asks for."""
    return f"Insert thinking:\nThe question is clear.\nClassification:\n{classification}"


def _questions(id_to_text):
    return make_question_df([{"id": qid, "question": text} for qid, text in id_to_text.items()])


def _metadata(rows):
    """A metadata frame from (source, id, category, valid_question, english) tuples."""
    return pd.DataFrame(rows, columns=META_COLUMNS)


def _empty_metadata():
    """The typed empty frame `load_question_metadata()` gives for an empty metadata file."""
    return pd.DataFrame(columns=META_COLUMNS).astype(constants.META_DATA_FILE_COLUMN_DTYPE)


def _rows_of(dfmeta, source):
    return dfmeta[dfmeta["source"] == source]


def _verdicts(dfmeta, source):
    rows = _rows_of(dfmeta, source)
    return dict(zip(rows["id"], rows["valid_question"]))


def _by_id(dfmeta):
    return {row["id"]: row for row in dfmeta.to_dict("records")}


def _assert_metadata_rows_of(dfmeta, source):
    """The output contract: the metadata columns in order, a verdict on every row of `source`."""
    assert list(dfmeta.columns) == META_COLUMNS
    assert _rows_of(dfmeta, source)["valid_question"].isin([True, False]).all()
    MetadataFrame.validate(dfmeta)


@pytest.mark.parametrize("source", question_curation.DATA_SOURCES + ["metaculus"])
def test_dataset_sources_and_metaculus_are_valid_without_an_llm_call(fake_llm, source):
    dfq = _questions({"q1": "Will the value increase?", "q2": "Will the value decrease?"})

    dfmeta = validate(_empty_metadata(), dfq, source)

    assert fake_llm.asked == []
    assert _verdicts(dfmeta, source) == {"q1": True, "q2": True}
    _assert_metadata_rows_of(dfmeta, source)


@pytest.mark.parametrize("source", LLM_VALIDATED_SOURCES)
def test_llm_verdicts_map_ok_to_valid_and_flag_to_invalid(fake_llm, source):
    questions = {
        "q1": "Will Israel invade Rafah in May 2027?",
        "q2": "Will the Fed cut rates in 2027?",
        "q3": "Will I finish my thesis by June 2027?",
        "q4": "Daily coinflip 2027-03-01",
        "q5": "Will the 2027 Oscars be held in March?",
        "q6": "Will Bitcoin reach $200K before 2028?",
    }
    fake_llm.responses = {
        questions["q1"]: _response('"ok"'),
        questions["q2"]: _response("ok"),
        questions["q3"]: _response('"flag"'),
        questions["q4"]: _response("flag"),
        # Neither verdict: the question is kept, as the prompt says to pass when in doubt.
        questions["q5"]: _response("unsure"),
        # A failed call keeps the question valid, so an LLM outage never flags questions.
        questions["q6"]: RuntimeError("rate limited"),
    }

    dfmeta = validate(_empty_metadata(), _questions(questions), source)

    assert _verdicts(dfmeta, source) == {
        "q1": True,
        "q2": True,
        "q3": False,
        "q4": False,
        "q5": True,
        "q6": True,
    }
    assert sorted(fake_llm.asked) == sorted(questions.values())
    _assert_metadata_rows_of(dfmeta, source)


def test_verdict_is_read_by_substring_from_the_text_after_the_first_marker(fake_llm):
    """The verdict is read by substring, quirks included.

    Only the text between the first and the second "Classification:" counts, and "ok" anywhere in
    it wins over "flag", even inside another word.
    """
    questions = {
        "q1": "Will Israel invade Rafah in May 2027?",
        "q2": "Will I finish my thesis by June 2027?",
        "q3": "Will the Fed cut rates in 2027?",
    }
    fake_llm.responses = {
        questions["q1"]: _response("flag\nThe question looks personal."),
        questions["q2"]: _response("flag\nOn reflection:\nClassification:\nok"),
        questions["q3"]: _response("ok\nOn reflection:\nClassification:\nflag"),
    }

    dfmeta = validate(_empty_metadata(), _questions(questions), "kalshi")

    assert _verdicts(dfmeta, "kalshi") == {"q1": True, "q2": False, "q3": True}


def test_a_verdict_belongs_to_its_source_and_id_not_to_the_id_alone(fake_llm):
    """Ids are only unique within a source: the same id in another source is another question."""
    dfmeta = _metadata(
        [
            ("metaculus", "1348", "Science & Tech", True, True),
            ("kalshi", "1348", "Politics & Governance", False, True),
        ]
    )
    questions = {"1348": "Will the Senate pass the bill?", "1385": "Will the FDA approve X?"}
    fake_llm.responses = {questions["1385"]: _response("ok")}

    updated = validate(dfmeta, _questions(questions), "kalshi")

    assert fake_llm.asked == [questions["1385"]]
    assert updated.to_dict("records") == [
        {
            "source": "kalshi",
            "id": "1348",
            "category": "Politics & Governance",
            "valid_question": False,
            "english": True,
        },
        {"source": "kalshi", "id": "1385", "category": "", "valid_question": True, "english": ""},
        {
            "source": "metaculus",
            "id": "1348",
            "category": "Science & Tech",
            "valid_question": True,
            "english": True,
        },
    ]


def test_question_without_a_verdict_is_left_out_and_asked_again_next_run(fake_llm):
    questions = {
        "q1": "Will Dwayne Johnson win the 2028 US Presidential Election?",
        "q2": "Will Taylor Swift get married before 2028?",
    }
    fake_llm.responses = {
        questions["q1"]: "I am not able to classify this question.",
        questions["q2"]: _response("ok"),
    }
    dfmeta = _empty_metadata()

    dfmeta = validate(dfmeta, _questions(questions), "kalshi")

    assert _verdicts(dfmeta, "kalshi") == {"q2": True}

    fake_llm.responses[questions["q1"]] = _response("ok")
    fake_llm.asked.clear()

    dfmeta = validate(dfmeta, _questions(questions), "kalshi")

    assert fake_llm.asked == [questions["q1"]]
    assert _verdicts(dfmeta, "kalshi") == {"q1": True, "q2": True}


def test_already_validated_rows_keep_their_verdict_and_are_not_asked_again(fake_llm):
    questions = {
        "q1": "Will Vladimir Putin declare martial law before 2028?",
        "q2": "Will my video reach 100k views by the end of the week?",
        "q3": "Will I get into Stanford?",
    }
    dfmeta = _metadata(
        [
            ("kalshi", "q1", "Politics & Governance", True, True),
            ("kalshi", "q2", "Arts & Recreation", False, True),
        ]
    )
    fake_llm.responses = {questions["q3"]: _response("flag")}

    dfmeta = validate(dfmeta, _questions(questions), "kalshi")

    assert fake_llm.asked == [questions["q3"]]
    assert _by_id(dfmeta) == {
        "q1": {
            "source": "kalshi",
            "id": "q1",
            "category": "Politics & Governance",
            "valid_question": True,
            "english": True,
        },
        "q2": {
            "source": "kalshi",
            "id": "q2",
            "category": "Arts & Recreation",
            "valid_question": False,
            "english": True,
        },
        # A question the metadata has not seen gets the placeholder in the tag job's columns.
        "q3": {
            "source": "kalshi",
            "id": "q3",
            "category": "",
            "valid_question": False,
            "english": "",
        },
    }
    # The invalid count the job reports includes the verdicts kept from earlier runs.
    assert _rows_of(dfmeta, "kalshi")["valid_question"].eq(False).sum() == 2


def test_validate_replaces_only_the_rows_of_the_source(fake_llm):
    dfmeta = _metadata(
        [
            ("polymarket", "p1", "Sports", True, True),
            ("kalshi", "q2", "Other", True, True),  # no longer in the kalshi question file
            ("kalshi", "q1", "Politics & Governance", False, True),
        ]
    )
    questions = {"q1": "Will Russia's territory shrink by 2030?", "q3": "Will the FDA approve X?"}
    fake_llm.responses = {questions["q3"]: _response("ok")}

    updated = validate(dfmeta, _questions(questions), "kalshi")

    assert list(updated.columns) == META_COLUMNS
    assert updated[["source", "id", "valid_question"]].values.tolist() == [
        ["kalshi", "q1", False],
        ["kalshi", "q3", True],
        ["polymarket", "p1", True],
    ]
    assert list(updated.index) == [0, 1, 2]
    assert _by_id(updated)["p1"] == {
        "source": "polymarket",
        "id": "p1",
        "category": "Sports",
        "valid_question": True,
        "english": True,
    }


def test_metadata_frame_accepts_the_placeholders_left_between_the_tag_and_validate_jobs():
    dfmeta = _metadata(
        [
            ("kalshi", "q1", "", "", ""),
            ("fred", "DGS10", "Economics & Business", True, True),
        ]
    )

    MetadataFrame.validate(dfmeta)


def test_metadata_frame_rejects_a_value_that_is_neither_a_verdict_nor_the_placeholder():
    dfmeta = _metadata([("kalshi", "q1", "", "maybe", "")])

    with pytest.raises(pandera.errors.SchemaError):
        MetadataFrame.validate(dfmeta)


def test_metadata_outside_the_contract_is_rejected_before_anything_is_validated(fake_llm):
    dfmeta = _metadata([("kalshi", "q1", "", "maybe", "")])

    with pytest.raises(pandera.errors.SchemaError):
        validate(dfmeta, _questions({"q1": "Will the FDA approve X?"}), "kalshi")
    assert fake_llm.asked == []
