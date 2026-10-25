"""Tagging one source: fixed or LLM categories, the English flag, and the metadata contract."""

import ssl
import time
import types

import pandas as pd
import pandera.pandas as pa
import pytest
import truststore
from utils.llm.provider_registry import PROVIDERS

from _schemas import MetadataFrame
from helpers import constants, keys, metadata_llm, question_curation
from metadata import _tag as metadata_tag
from tests.factories import FakeJevClient

FIXED_CATEGORIES = {
    "acled": "Security & Defense",
    "fred": "Economics & Business",
    "yfinance": "Economics & Business",
    "serpapi": "Economics & Business",
}

# The production category budget; the reasoning metadata model needs it to reach an answer.
CATEGORY_OUTPUT_TOKENS = 512


def _empty_metadata():
    return pd.DataFrame(columns=constants.META_DATA_FILE_COLUMNS).astype(
        constants.META_DATA_FILE_COLUMN_DTYPE
    )


def _metadata(rows):
    return pd.DataFrame(rows, columns=constants.META_DATA_FILE_COLUMNS)


def _questions(ids):
    """A question file with one row per id; the question text is `question <id>`."""
    df = pd.DataFrame(
        {
            "id": ids,
            "question": [f"question {i}" for i in ids],
            "background": [f"background {i}" for i in ids],
        }
    )
    for column in constants.QUESTION_FILE_COLUMNS:
        if column not in df.columns:
            df[column] = "N/A"
    return df[constants.QUESTION_FILE_COLUMNS]


class _FakeLLM:
    """Stand-in for the metadata model. `responses` maps question text to a reply or an error.

    Like the real reasoning model, it gives up with an incomplete response when the output budget
    is below `min_output_tokens`, because the reasoning uses the budget up before the answer.
    """

    def __init__(self, responses, min_output_tokens=0):
        self.responses = responses
        self.min_output_tokens = min_output_tokens
        self.asked = []

    def __call__(self, prompt, max_output_tokens):
        question = next(q for q in self.responses if f"Question: {q}\n" in prompt)
        self.asked.append(question)
        if max_output_tokens < self.min_output_tokens:
            raise RuntimeError("response incomplete (status=incomplete), reason=max_output_tokens")
        outcome = self.responses[question]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture()
def fake_llm(monkeypatch):
    """Install a fake metadata model; the key prefetch must not reach Secret Manager."""

    def install(responses, min_output_tokens=0):
        llm = _FakeLLM(responses, min_output_tokens)
        monkeypatch.setattr(metadata_llm, "_get_metadata_model_run", lambda: None)
        monkeypatch.setattr(metadata_llm, "get_metadata_model_response", llm)
        return llm

    return install


@pytest.fixture()
def fake_jev(monkeypatch):
    def install(answer):
        jev = FakeJevClient(answer)
        monkeypatch.setattr(metadata_tag, "get_client", lambda: jev)
        return jev

    return install


@pytest.fixture()
def no_llm(monkeypatch):
    monkeypatch.setattr(metadata_llm, "_get_metadata_model_run", lambda: None)
    monkeypatch.setattr(
        metadata_llm,
        "get_metadata_model_response",
        lambda **kwargs: pytest.fail("the metadata model must not be called"),
    )


@pytest.fixture()
def no_jev(monkeypatch):
    monkeypatch.setattr(metadata_tag, "get_client", lambda: pytest.fail("Jev must not be called"))


@pytest.fixture()
def slow_key_fetch(monkeypatch):
    """Use the real cached metadata model setup, cold, with a slow Secret Manager key fetch.

    Yields the list of key fetches. Each fetch takes long enough for calls that start together
    on the cold cache to overlap, as the first concurrent model calls of a job run do.
    """
    key_fetches = []

    def configure_api_keys(**kwargs):
        key_fetches.append(kwargs)
        time.sleep(0.1)

    class _FakeModelRun:
        provider = PROVIDERS["OpenAI"]

        def get_response(self, prompt, max_output_tokens, **kwargs):
            return "Sports"

    monkeypatch.setattr(metadata_llm, "configure_api_keys", configure_api_keys)
    monkeypatch.setattr(metadata_llm, "validate_provider_keys", lambda providers: None)
    monkeypatch.setattr(
        metadata_llm,
        "model_runs",
        types.SimpleNamespace(get_model_run=lambda key: _FakeModelRun()),
    )
    monkeypatch.setattr(metadata_llm, "get_openai_safety_identifier", lambda: "test-safety-id")
    metadata_llm._get_metadata_model_run.cache_clear()
    yield key_fetches
    metadata_llm._get_metadata_model_run.cache_clear()


def _rows(dfmeta, source):
    rows = dfmeta[dfmeta["source"] == source]
    return rows.set_index("id")[["category", "valid_question", "english"]].to_dict("index")


@pytest.mark.parametrize("source", sorted(FIXED_CATEGORIES))
def test_sources_with_a_fixed_category_are_tagged_without_a_model_call(source, no_llm, no_jev):
    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1", "q2"]), source)

    assert _rows(dfmeta, source) == {
        "q1": {"category": FIXED_CATEGORIES[source], "valid_question": "", "english": True},
        "q2": {"category": FIXED_CATEGORIES[source], "valid_question": "", "english": True},
    }


def test_market_questions_get_the_category_the_model_answers(fake_llm, fake_jev):
    llm = fake_llm({"question q1": '"Sports."', "question q2": " Science & Tech "})
    fake_jev({"question q1": "english", "question q2": "english"})

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1", "q2"]), "metaculus")

    assert sorted(llm.asked) == ["question q1", "question q2"]
    assert _rows(dfmeta, "metaculus") == {
        "q1": {"category": "Sports", "valid_question": "", "english": True},
        "q2": {"category": "Science & Tech", "valid_question": "", "english": True},
    }


def test_the_category_call_leaves_the_reasoning_model_room_to_answer(fake_llm, fake_jev):
    """With too small an output budget the model never answers and every question is "Other"."""
    fake_llm(
        {"question q1": "Sports", "question q2": "Science & Tech"},
        min_output_tokens=CATEGORY_OUTPUT_TOKENS,
    )
    fake_jev({"question q1": "english", "question q2": "english"})

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1", "q2"]), "metaculus")

    assert dfmeta.set_index("id")["category"].to_dict() == {"q1": "Sports", "q2": "Science & Tech"}


def test_the_model_keys_are_fetched_once_for_a_batch_of_questions(slow_key_fetch, fake_jev):
    """The key cache has no lock: concurrent first calls on a cold cache each read every secret."""
    ids = [f"q{i}" for i in range(20)]
    fake_jev({f"question {i}": "english" for i in ids})

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(ids), "metaculus")

    assert dfmeta["category"].tolist() == ["Sports"] * len(ids)
    assert len(slow_key_fetch) == 1


@pytest.mark.parametrize(
    "response", ["Finance", "", "Sports, Other", RuntimeError("model unavailable")]
)
def test_an_answer_outside_the_category_list_or_a_failed_call_tags_other(
    response, fake_llm, fake_jev
):
    fake_llm({"question q1": response})
    fake_jev({"question q1": "english"})

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1"]), "polymarket")

    assert dfmeta["category"].tolist() == ["Other"]


def test_rows_already_tagged_keep_their_values_and_are_not_sent_again(fake_llm, fake_jev):
    llm = fake_llm({"question q2": "Sports"})
    jev = fake_jev({"question q2": "not_english"})
    dfmeta = _metadata([("metaculus", "q1", "Politics & Governance", False, True)])

    dfmeta = metadata_tag.tag(dfmeta, _questions(["q1", "q2"]), "metaculus")

    assert llm.asked == ["question q2"]
    assert jev.asked == ["question q2"]
    assert _rows(dfmeta, "metaculus") == {
        "q1": {"category": "Politics & Governance", "valid_question": False, "english": True},
        "q2": {"category": "Sports", "valid_question": "", "english": False},
    }


def test_sources_that_share_an_id_keep_their_own_category_and_english_flag(fake_llm, fake_jev):
    """Ids are unique within a source only: the dev metadata has metaculus and infer rows `1348`."""
    llm = fake_llm({"question 1348": "Sports"})
    jev = fake_jev({"question 1348": "not_english"})
    dfmeta = _metadata(
        [
            ("infer", "1348", "Politics & Governance", False, ""),
            ("metaculus", "1348", "Science & Tech", True, True),
        ]
    )

    dfmeta = metadata_tag.tag(dfmeta, _questions(["1348"]), "metaculus")

    assert llm.asked == []
    assert jev.asked == []

    dfmeta = metadata_tag.tag(dfmeta, _questions(["1348"]), "polymarket")

    assert llm.asked == ["question 1348"]
    assert jev.asked == ["question 1348"]
    assert dfmeta.to_dict("records") == [
        {
            "source": "infer",
            "id": "1348",
            "category": "Politics & Governance",
            "valid_question": False,
            "english": "",
        },
        {
            "source": "metaculus",
            "id": "1348",
            "category": "Science & Tech",
            "valid_question": True,
            "english": True,
        },
        {
            "source": "polymarket",
            "id": "1348",
            "category": "Sports",
            "valid_question": "",
            "english": False,
        },
    ]


def test_a_category_that_left_the_list_is_assigned_again(fake_llm, fake_jev):
    llm = fake_llm({"question q1": "Healthcare & Biology"})
    jev = fake_jev({})
    dfmeta = _metadata([("kalshi", "q1", "Finance", True, True)])

    dfmeta = metadata_tag.tag(dfmeta, _questions(["q1"]), "kalshi")

    assert llm.asked == ["question q1"]
    assert jev.asked == []
    assert _rows(dfmeta, "kalshi") == {
        "q1": {"category": "Healthcare & Biology", "valid_question": True, "english": True}
    }


def test_market_rows_get_the_english_flag_from_jev_and_a_failed_call_leaves_it_empty(
    fake_llm, fake_jev
):
    fake_llm({"question q1": "Sports", "question q2": "Sports", "question q3": "Sports"})
    fake_jev(
        {"question q1": "english", "question q2": "not_english", "question q3": RuntimeError("x")}
    )

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1", "q2", "q3"]), "polymarket")

    assert dfmeta.set_index("id")["english"].to_dict() == {"q1": True, "q2": False, "q3": ""}


def test_an_empty_english_flag_is_classified_again_on_the_next_run(fake_jev, no_llm):
    jev = fake_jev({"question q1": "english"})
    dfmeta = _metadata([("polymarket", "q1", "Sports", True, "")])

    dfmeta = metadata_tag.tag(dfmeta, _questions(["q1"]), "polymarket")

    assert jev.asked == ["question q1"]
    assert dfmeta["english"].tolist() == [True]


def test_questions_that_left_the_question_file_leave_the_metadata(no_llm, no_jev):
    dfmeta = _metadata(
        [
            ("fred", "gone", "Economics & Business", True, True),
            ("fred", "kept", "Economics & Business", True, True),
            ("kalshi", "k1", "Sports", True, True),
        ]
    )

    dfmeta = metadata_tag.tag(dfmeta, _questions(["kept", "new"]), "fred")

    assert list(zip(dfmeta["source"], dfmeta["id"])) == [
        ("fred", "kept"),
        ("fred", "new"),
        ("kalshi", "k1"),
    ]
    assert _rows(dfmeta, "kalshi") == {
        "k1": {"category": "Sports", "valid_question": True, "english": True}
    }


def test_output_has_exactly_the_metadata_columns_sorted_by_source_then_id(no_llm, no_jev):
    dfmeta = _metadata(
        [
            ("yfinance", "b", "Economics & Business", True, True),
            ("dbnomics", "z", "Economics & Business", True, True),
        ]
    )

    dfmeta = metadata_tag.tag(dfmeta, _questions(["c", "a"]), "fred")

    assert list(dfmeta.columns) == constants.META_DATA_FILE_COLUMNS
    MetadataFrame.validate(dfmeta)
    assert list(zip(dfmeta["source"], dfmeta["id"])) == [
        ("dbnomics", "z"),
        ("fred", "a"),
        ("fred", "c"),
        ("yfinance", "b"),
    ]


@pytest.mark.parametrize(
    "dfmeta",
    [
        pd.DataFrame(
            [("kalshi", "k1", "Sports", True)],
            columns=["source", "id", "category", "valid_question"],
        ),
        pd.DataFrame(
            [("kalshi", "k1", "Sports", True, "yes")], columns=constants.META_DATA_FILE_COLUMNS
        ),
    ],
    ids=["missing column", "english outside True/False/empty"],
)
def test_metadata_outside_the_contract_is_rejected_before_anything_is_tagged(
    dfmeta, no_llm, no_jev
):
    with pytest.raises(pa.errors.SchemaError):
        metadata_tag.tag(dfmeta, _questions(["q1"]), "acled")


def test_a_question_file_missing_a_column_is_rejected(no_llm, no_jev):
    dfq = _questions(["q1"]).drop(columns=["background"])

    with pytest.raises(pa.errors.SchemaError):
        metadata_tag.tag(_empty_metadata(), dfq, "acled")


def test_dbnomics_is_tagged_by_the_model_and_is_english_without_a_jev_call(fake_llm, no_jev):
    llm = fake_llm({"question q1": "Environment & Energy"})

    dfmeta = metadata_tag.tag(_empty_metadata(), _questions(["q1"]), "dbnomics")

    assert llm.asked == ["question q1"]
    assert _rows(dfmeta, "dbnomics") == {
        "q1": {"category": "Environment & Energy", "valid_question": "", "english": True}
    }


def test_an_empty_question_file_leaves_only_the_other_sources(no_llm, no_jev):
    dfmeta = _metadata(
        [("fred", "f1", "Economics & Business", True, True), ("kalshi", "k1", "Sports", True, True)]
    )

    dfmeta = metadata_tag.tag(dfmeta, _questions([]), "fred")

    assert list(zip(dfmeta["source"], dfmeta["id"])) == [("kalshi", "k1")]


def test_jev_client_does_not_use_truststore_for_tls(monkeypatch):
    """truststore 0.10.4 corrupts the OpenSSL heap under concurrent async handshakes on Linux.

    httpx2 picks truststore's SSLContext by default. The Jev client must hand httpx2 Python's
    own default context instead, or the classification fan-out aborts the job with
    "double free or corruption".
    """
    # setattr would read the old value first, which fetches the secret; patch the dict instead.
    monkeypatch.setitem(vars(keys), "API_KEY_TYPESAFE", "test-key")
    client = metadata_tag.get_client()
    ssl_context = client._http_client._transport._pool._ssl_context
    assert isinstance(ssl_context, ssl.SSLContext)
    assert not isinstance(ssl_context, truststore.SSLContext)


def _market_rows(english):
    return pd.DataFrame(
        {
            "source": ["metaculus"] * len(english),
            "id": [f"q{i}" for i in range(len(english))],
            "question": [f"question {i}" for i in range(len(english))],
            "background": [f"background {i}" for i in range(len(english))],
            "english": english,
        }
    )


def test_market_rows_get_true_or_false_from_the_choice(fake_jev):
    fake_jev({"question 0": "english", "question 1": "not_english"})
    dfq = metadata_tag.assign_english(_market_rows(["", ""]), "metaculus")
    assert dfq["english"].tolist() == [True, False]


def test_market_rows_with_a_value_are_not_sent(fake_jev):
    jev = fake_jev({"question 1": "not_english"})
    dfq = metadata_tag.assign_english(_market_rows([True, ""]), "metaculus")
    assert jev.asked == ["question 1"]
    assert dfq["english"].tolist() == [True, False]


def test_failed_call_leaves_the_row_empty(fake_jev):
    fake_jev({"question 0": RuntimeError("boom"), "question 1": "english"})
    dfq = metadata_tag.assign_english(_market_rows(["", ""]), "metaculus")
    assert dfq["english"].tolist() == ["", True]


@pytest.mark.parametrize("source", question_curation.DATA_SOURCES)
def test_dataset_source_is_english_without_a_call(source, no_jev):
    dfq = _market_rows(["", ""]).assign(source=source)
    dfq = metadata_tag.assign_english(dfq, source)
    assert dfq["english"].tolist() == [True, True]
