"""The english flag comes from Jev for market questions and is True for dataset questions."""

import types

import pandas as pd
import pytest

from helpers import question_curation
from metadata.tag_questions import main as tag_questions


class _FakeJev:
    """Stand-in for AsyncTypeSafeClient. `answer` maps question text to a choice or an exception."""

    def __init__(self, answer):
        self.answer = answer

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def system_one(self, state, questions):
        outcome = self.answer[state["question"]]
        if isinstance(outcome, Exception):
            raise outcome
        answer = types.SimpleNamespace(choice=outcome, probabilities={outcome: 0.9, "english": 0.1})
        return types.SimpleNamespace(choices={"language": answer})


def test_jev_client_does_not_use_truststore_for_tls(monkeypatch):
    """truststore 0.10.4 corrupts the OpenSSL heap under concurrent async handshakes on Linux.

    httpx2 picks truststore's SSLContext by default. The Jev client must hand httpx2 Python's
    own default context instead, or the classification fan-out aborts the job with
    "double free or corruption".
    """
    import ssl

    import truststore

    from helpers import keys

    # setattr would read the old value first, which fetches the secret; patch the dict instead.
    monkeypatch.setitem(vars(keys), "API_KEY_TYPESAFE", "test-key")
    client = tag_questions.get_client()
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


def test_market_rows_get_true_or_false_from_the_choice(monkeypatch):
    fake = _FakeJev({"question 0": "english", "question 1": "not_english"})
    monkeypatch.setattr(tag_questions, "get_client", lambda: fake)
    dfq = tag_questions.assign_english(_market_rows(["", ""]), "metaculus")
    assert dfq["english"].tolist() == [True, False]


def test_market_rows_with_a_value_are_not_sent(monkeypatch):
    fake = _FakeJev({"question 1": "not_english"})
    monkeypatch.setattr(tag_questions, "get_client", lambda: fake)
    dfq = tag_questions.assign_english(_market_rows([True, ""]), "metaculus")
    assert dfq["english"].tolist() == [True, False]


def test_failed_call_leaves_the_row_empty(monkeypatch):
    fake = _FakeJev({"question 0": RuntimeError("boom"), "question 1": "english"})
    monkeypatch.setattr(tag_questions, "get_client", lambda: fake)
    dfq = tag_questions.assign_english(_market_rows(["", ""]), "metaculus")
    assert dfq["english"].tolist() == ["", True]


@pytest.mark.parametrize("source", question_curation.DATA_SOURCES)
def test_dataset_source_is_english_without_a_call(monkeypatch, source):
    def no_client():
        raise AssertionError("dataset sources must not call Jev")

    monkeypatch.setattr(tag_questions, "get_client", no_client)
    dfq = _market_rows(["", ""]).assign(source=source)
    dfq = tag_questions.assign_english(dfq, source)
    assert dfq["english"].tolist() == [True, True]
