"""Integration: thin driver() wiring tests for orchestration entry points.

These run the real ``orchestration/func_*/main.py:driver()`` against a ``local_bucket`` mount
(no GCP), proving the IO wiring connects: read inputs -> call the source -> write outputs. The
*wiring* contract is uniform across the implemented sources, so it is **parametrized** over
``IMPLEMENTED_SOURCES`` (see ``tests/_sources.py``).

Source behaviour (parse/update logic) is NOT tested here — that lives in ``unit/sources/`` and
``contract/test_update_conformance.py``. ``fetch()``/``update()`` are mocked so these stay pure
wiring checks.

Divergences are handled with a narrower test *in this file* (see README "integration"):

- **Stateless fetch.** The ``dbnomics``/``kalshi``/``manifold``/``metaculus`` fetch drivers call
  ``fetch()`` with no args (they don't read the question bank), so the "passes the existing bank"
  assertion is scoped to ``BANK_READING_FETCH`` only.
- **Pure update.** ``polymarket`` front-loads its resolution data (``historical_prices``) into
  ``fetch()``, so its ``update()`` merely reshapes already-fetched data and makes no network calls.
  It is therefore run *unmocked* in ``TestPolymarketUpdateRealChain`` to prove the whole
  read -> update -> write chain for real. The market sources pull per-question resolution data via
  the network *inside* ``update()``, so they can't run offline unmocked; they are covered by the
  mocked, parametrized wiring test here, and their ``update()`` assembly is exercised offline in
  ``unit/sources/`` and ``contract/test_update_conformance.py``.
- **Self-inspecting fetch.** ``serpapi``'s fetch driver reads the measurements it just fetched
  (per-category alerts, an API-usage check), so it gets its own fetch test; its update driver
  additionally proves the strict history load (``TestSerpapiDrivers``).
- **Update-only source.** ``manifold`` is no longer fetched (no fetch driver, no fetch file) but
  its update driver still runs the real ``update()`` off the question bank every night, so the
  already-published markets keep resolving; its wiring gets its own test below.
- **Retired source.** ``infer`` has no fetch driver at all, and its update driver runs a no-op
  ``update()`` that uploads nothing; it gets its own test at the bottom of this file.
- **No resolution files.** ``acled`` questions resolve straight off the aggregated fetch file, so
  its update driver persists questions and the id hash mapping but never a resolution file, and it
  must hand ``update()`` the raw event rows with ``event_date`` still a string (the year-prefix fix
  inside ``update()`` needs the unparsed value). Its ``update()`` is network-free, so the chain is
  also run unmocked (``TestAcledDrivers``).
- **One fetch file per page.** ``wikipedia``'s fetch returns one frame per configured page, written
  to ``wikipedia/fetch/<id_root>.jsonl`` (with a Slack alert for pages that failed), and its update
  driver reads those files back into the same per-page mapping — so neither driver fits the single
  ``<source>_fetch.jsonl`` wiring (``TestWikipediaDrivers``).
"""

import inspect
import os
from contextlib import ExitStack
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from _fb_types import UpdateResult
from helpers import constants
from orchestration.func_acled_fetch.main import driver as acled_fetch
from orchestration.func_acled_update.main import driver as acled_update
from orchestration.func_dbnomics_fetch.main import driver as dbnomics_fetch
from orchestration.func_dbnomics_update.main import driver as dbnomics_update
from orchestration.func_fred_fetch.main import driver as fred_fetch
from orchestration.func_fred_update.main import driver as fred_update
from orchestration.func_infer_update.main import driver as infer_update
from orchestration.func_kalshi_fetch.main import driver as kalshi_fetch
from orchestration.func_kalshi_update.main import driver as kalshi_update
from orchestration.func_manifold_update.main import driver as manifold_update
from orchestration.func_metaculus_fetch.main import driver as metaculus_fetch
from orchestration.func_metaculus_update.main import driver as metaculus_update
from orchestration.func_polymarket_fetch.main import driver as polymarket_fetch
from orchestration.func_polymarket_update.main import driver as polymarket_update
from orchestration.func_serpapi_fetch import main as serpapi_fetch_main
from orchestration.func_serpapi_update.main import driver as serpapi_update
from orchestration.func_wikipedia_fetch import main as wikipedia_fetch_main
from orchestration.func_wikipedia_update.main import driver as wikipedia_update
from orchestration.func_yfinance_fetch.main import driver as yfinance_fetch
from orchestration.func_yfinance_update.main import driver as yfinance_update
from sources.registry import SOURCES
from tests._sources import IMPLEMENTED_SOURCES
from tests.factories import (
    make_acled_event,
    make_polymarket_fetch_df,
    make_question_df,
    make_resolution_df,
    make_serpapi_fetch_df,
)

FETCH_DRIVERS = {
    "acled": acled_fetch,
    "dbnomics": dbnomics_fetch,
    "fred": fred_fetch,
    "kalshi": kalshi_fetch,
    "metaculus": metaculus_fetch,
    "polymarket": polymarket_fetch,
    "yfinance": yfinance_fetch,
}
UPDATE_DRIVERS = {
    "dbnomics": dbnomics_update,
    "fred": fred_update,
    "kalshi": kalshi_update,
    "metaculus": metaculus_update,
    "polymarket": polymarket_update,
    "serpapi": serpapi_update,
    "yfinance": yfinance_update,
}
# serpapi's fetch driver reads the measurements it just fetched (per-category validity alerts,
# an API-usage check) and wikipedia's writes one file per page, so both diverge from the uniform
# fetch wiring: see TestSerpapiDrivers / TestWikipediaDrivers.
UNIFORM_FETCH_SOURCES = [n for n in IMPLEMENTED_SOURCES if n not in ("serpapi", "wikipedia")]
# acled's update driver persists no resolution files and wikipedia's reads per-page fetch files,
# so neither fits the uniform update wiring: see TestAcledDrivers / TestWikipediaDrivers.
UNIFORM_UPDATE_SOURCES = [n for n in IMPLEMENTED_SOURCES if n not in ("acled", "wikipedia")]
# Fetch drivers that read the existing bank and feed it to fetch(dfq=...). acled/dbnomics/kalshi/
# metaculus diverge: they fetch all IDs fresh via a stateless fetch() and are excluded from that
# assertion.
BANK_READING_FETCH = ["fred", "polymarket", "yfinance"]
# Attributes a driver reads off the source *after* fetch()/update() returns, to raise an operator
# Slack alert. The real method populates them as it runs; the mocked wiring tests below never do,
# so they seed a class-level default first.
POST_FETCH_ALERT_ATTRS = {
    "wikipedia": ["failed_pages"],
    "yfinance": ["uncurated_delisted_tickers"],
}
POST_UPDATE_ALERT_ATTRS = {"kalshi": ["invalid_resolution_window_actions"]}


def _call_arguments(mock, method) -> dict:
    """Return the arguments the driver passed to the mocked source method, by parameter name.

    Bound through the real method's signature, so a driver may pass ``dfq``/``dff`` positionally
    or by keyword: the test asserts what reached the source, not the call's shape.
    """
    bound = inspect.signature(method).bind(None, *mock.call_args.args, **mock.call_args.kwargs)
    bound.apply_defaults()
    return bound.arguments


def _seed_alert_attrs(stack, name, attrs):
    """Seed the operator-alert attributes that the mocked fetch()/update() never sets."""
    for attr in attrs.get(name, ()):
        stack.enter_context(patch.object(type(SOURCES[name]), attr, [], create=True))


class TestFetchDriverWiring:
    """Every fetch driver: read bank (if applicable) -> fetch() -> write <source>_fetch.jsonl."""

    def test_every_implemented_source_has_a_wiring_test(self):
        """Adding a source to IMPLEMENTED_SOURCES must route it through a fetch and an update test."""
        assert set(UNIFORM_FETCH_SOURCES) | {"serpapi", "wikipedia"} == set(IMPLEMENTED_SOURCES)
        assert set(FETCH_DRIVERS) == set(UNIFORM_FETCH_SOURCES)
        assert set(UNIFORM_UPDATE_SOURCES) | {"acled", "wikipedia"} == set(IMPLEMENTED_SOURCES)
        assert set(UPDATE_DRIVERS) == set(UNIFORM_UPDATE_SOURCES)

    @pytest.mark.parametrize("name", UNIFORM_FETCH_SOURCES)
    def test_writes_fetch_output(self, name, local_bucket):
        local_bucket.seed_questions(name, [])  # ignored by the stateless fetch drivers
        fetched = pd.DataFrame([{"id": "x1"}, {"id": "x2"}])

        with ExitStack() as stack:
            _seed_alert_attrs(stack, name, POST_FETCH_ALERT_ATTRS)
            mock_fetch = stack.enter_context(
                patch.object(type(SOURCES[name]), "fetch", return_value=fetched)
            )
            FETCH_DRIVERS[name](None)

        assert mock_fetch.called
        out = local_bucket.read_fetch(name)
        assert sorted(out["id"].astype(str)) == ["x1", "x2"]

    @pytest.mark.parametrize("name", BANK_READING_FETCH)
    def test_passes_existing_bank_to_fetch(self, name, local_bucket):
        """Bank-reading drivers hand the existing question bank to fetch() as ``dfq``."""
        banked = make_question_df([{"id": "banked"}])
        local_bucket.seed_questions(name, banked.to_dict("records"))
        source_cls = type(SOURCES[name])
        real_fetch = source_cls.fetch
        with ExitStack() as stack:
            _seed_alert_attrs(stack, name, POST_FETCH_ALERT_ATTRS)
            mock_fetch = stack.enter_context(
                patch.object(source_cls, "fetch", return_value=pd.DataFrame([{"id": "x1"}]))
            )
            FETCH_DRIVERS[name](None)
        dfq = _call_arguments(mock_fetch, real_fetch)["dfq"]
        assert dfq["id"].astype(str).tolist() == ["banked"]  # the seeded bank, not a stand-in


class TestUpdateDriverWiring:
    """Every update driver: read bank + fetch -> update() -> persist questions + resolution files.

    update() is mocked (most sources reach the network inside it); this asserts only that the
    driver persists what update() returns. update()'s assembly contract lives in
    contract/test_update_conformance.py.
    """

    @pytest.mark.parametrize("name", UNIFORM_UPDATE_SOURCES)
    def test_persists_questions_and_resolution(self, name, local_bucket):
        # The bank + fetch file the driver must read. The bank carries the full persisted column
        # set, since update drivers read question fields (e.g. `resolved`) before calling update().
        bank = make_question_df([{"id": "existing"}])
        local_bucket.seed_questions(name, bank.to_dict("records"))
        local_bucket.seed_fetch(name, [{"id": "m1"}])
        canned = UpdateResult(
            dfq=make_question_df([{"id": "m1"}]),
            resolution_files={
                "m1": make_resolution_df([{"id": "m1", "date": "2025-01-01", "value": 0.5}])
            },
        )

        source_cls = type(SOURCES[name])
        real_update = source_cls.update
        with ExitStack() as stack:
            _seed_alert_attrs(stack, name, POST_UPDATE_ALERT_ATTRS)
            mock_update = stack.enter_context(
                patch.object(source_cls, "update", return_value=canned)
            )
            UPDATE_DRIVERS[name](None)

        # The driver read the bucket inputs and fed them to update() as dfq and dff — a driver
        # that ignored the bank/fetch would still persist, so assert the wiring both ways.
        arguments = _call_arguments(mock_update, real_update)
        assert "existing" in arguments["dfq"]["id"].astype(str).tolist()
        assert "m1" in arguments["dff"]["id"].astype(str).tolist()
        # ...and it persisted exactly what update() returned: the bank is replaced, never appended
        # to (a driver that concatenated the old bank would keep "existing").
        questions = local_bucket.read_questions(name)
        assert questions["id"].astype(str).tolist() == ["m1"]
        res = local_bucket.read_resolution_file(name, "m1")
        assert list(res.columns) == ["id", "date", "value"]


class TestPolymarketUpdateRealChain:
    """Divergence: polymarket's update() is the only network-free update, so run it unmocked.

    Polymarket front-loads its resolution data (``historical_prices``) into ``fetch()``, so
    ``update()`` only reshapes already-fetched data. This proves the full read -> update -> write
    chain end to end (real update + real IO), which the mocked parametrized test above cannot. The
    other four sources pull resolution data via the network inside update(), so their update()
    behaviour is covered offline in contract/test_update_conformance.py (and unit/sources/) instead.
    """

    def test_writes_questions_and_resolution_files(self, local_bucket):
        local_bucket.seed_questions("polymarket", [])
        fetched = make_polymarket_fetch_df(
            [
                {
                    "id": "m1",
                    "historical_prices": [
                        {"date": "2024-06-01", "value": 0.4},
                        {"date": "2024-06-02", "value": 0.6},
                    ],
                }
            ]
        )
        local_bucket.seed_fetch("polymarket", fetched.to_dict("records"))

        polymarket_update(None)

        # Question upserted into the bank.
        questions = local_bucket.read_questions("polymarket")
        assert "m1" in questions["id"].astype(str).tolist()
        # Transient fetch-only columns are stripped before persisting.
        assert "historical_prices" not in questions.columns
        assert "probability" not in questions.columns

        # Resolution file written with canonical columns and both dates.
        res = local_bucket.read_resolution_file("polymarket", "m1")
        assert list(res.columns) == ["id", "date", "value"]
        assert len(res) == 2

    def test_existing_question_is_upserted_not_duplicated(self, local_bucket):
        existing = make_question_df([{"id": "m1", "source": "polymarket", "question": "old?"}])
        local_bucket.seed_questions("polymarket", existing.to_dict("records"))
        fetched = make_polymarket_fetch_df(
            [
                {
                    "id": "m1",
                    "question": "new?",
                    "historical_prices": [{"date": "2024-06-01", "value": 0.5}],
                }
            ]
        )
        local_bucket.seed_fetch("polymarket", fetched.to_dict("records"))

        polymarket_update(None)

        questions = local_bucket.read_questions("polymarket")
        ids = questions["id"].astype(str).tolist()
        assert ids.count("m1") == 1  # upserted, not duplicated...
        assert questions.loc[ids.index("m1"), "question"] == "new?"  # ...and the new row won


class TestSerpapiDrivers:
    """Divergence: serpapi's drivers do more than hand frames across the IO boundary.

    The fetch driver inspects what it fetched (alerting when a whole question category came back
    without a valid measurement) and checks the API's monthly usage, so the uniform "mock fetch,
    read the file back" test does not fit. The update driver loads each fetched question's saved
    history *strictly* (a missing history must be skipped, never read from a stale local copy)
    and hands it to ``update()``, which appends to it; the persisted file keeps the collection
    timestamps ``update()`` adds for flight series.
    """

    def test_fetch_driver_writes_the_fetched_measurements(self, local_bucket):
        fetched = make_serpapi_fetch_df([{"id": "cat__x1", "value": 1.0}, {"id": "cat__x2"}])
        with patch.object(
            type(SOURCES["serpapi"]), "fetch", return_value=fetched
        ) as mock_fetch, patch.object(serpapi_fetch_main, "warn_on_monthly_usage") as usage:
            serpapi_fetch_main.driver(None)

        assert mock_fetch.called
        assert usage.called  # the usage check runs even when fetch() raises; here it ran after
        out = local_bucket.read_fetch("serpapi")
        assert sorted(out["id"]) == ["cat__x1", "cat__x2"]

    def test_update_driver_loads_existing_histories_strictly(self, local_bucket):
        local_bucket.seed_questions(
            "serpapi", make_question_df([{"id": "a__1"}]).to_dict("records")
        )
        local_bucket.seed_fetch(
            "serpapi", make_serpapi_fetch_df([{"id": "a__1"}, {"id": "a__2"}]).to_dict("records")
        )
        # Only a__1 has a saved history; a__2 is brand new and must simply be absent.
        local_bucket.seed_resolution_file(
            "serpapi", "a__1", [{"id": "a__1", "date": "2026-03-17", "value": 9.0}]
        )
        canned = UpdateResult(
            dfq=make_question_df([{"id": "a__1"}, {"id": "a__2"}]),
            resolution_files={
                "a__1": pd.DataFrame(
                    [
                        {"id": "a__1", "date": "2026-03-17", "value": 9.0, "fetch_datetime": None},
                        {
                            "id": "a__1",
                            "date": "2026-03-18",
                            "value": 10.0,
                            "fetch_datetime": "2026-03-18T00:05:00+00:00",
                        },
                    ]
                ),
                "a__2": make_resolution_df([{"id": "a__2", "date": "2026-03-18", "value": 10.0}]),
            },
        )

        # A stale local copy of a__2's history from an earlier run: the strict load must not read
        # it back in place of the (absent) bucket object. Without it this test could not tell a
        # strict load from the tolerant one.
        stale = "/tmp/serpapi_a__2.jsonl"
        with open(stale, "w", encoding="utf-8") as fh:
            fh.write('{"id": "a__2", "date": "2026-03-01", "value": 99.0}\n')
        try:
            with patch.object(
                type(SOURCES["serpapi"]), "update", return_value=canned
            ) as mock_update:
                serpapi_update(None)
        finally:
            if os.path.exists(stale):
                os.remove(stale)

        existing = mock_update.call_args.kwargs["existing_resolution_files"]
        assert set(existing) == {"a__1"}
        assert existing["a__1"]["value"].tolist() == [9.0]
        assert set(local_bucket.read_questions("serpapi")["id"]) == {"a__1", "a__2"}
        # The timestamps update() attached are persisted; a file without them stays canonical.
        with_history = local_bucket.read_resolution_file("serpapi", "a__1")
        assert list(with_history.columns) == ["id", "date", "value", "fetch_datetime"]
        assert list(local_bucket.read_resolution_file("serpapi", "a__2").columns) == [
            "id",
            "date",
            "value",
        ]


class TestAcledDrivers:
    """Divergence: ACLED questions resolve off the fetch file, so update() writes no resolution files.

    The update driver's job is to hand ``update()`` the raw events exactly as fetched (``event_date``
    unparsed, so the year-prefix fix inside ``update()`` can see ``0025-``) together with the id hash
    mapping it loaded, then persist the questions and the mapping — and nothing under ``acled/`` but
    that mapping. ``update()`` itself is network-free, so the chain is also run unmocked.
    """

    def test_update_driver_hands_raw_events_to_update_and_persists_the_hash_mapping(
        self, local_bucket
    ):
        bank = make_question_df([{"id": "existing"}])
        local_bucket.seed_questions("acled", bank.to_dict("records"))
        # An event carrying ACLED's malformed century: update() repairs it, so it must arrive raw.
        local_bucket.seed_fetch(
            "acled", [make_acled_event(event_id_cnty="SOM1", event_date="0025-06-15")]
        )
        mapping = {"existing": {"key": "k", "country": "Somalia", "event_type": "Battles"}}
        local_bucket.seed_hash_mapping("acled", mapping)
        canned = UpdateResult(dfq=make_question_df([{"id": "q1"}]), hash_mapping=mapping)

        source_cls = type(SOURCES["acled"])
        real_update = source_cls.update
        with patch.object(source_cls, "update", return_value=canned) as mock_update:
            acled_update(None)

        arguments = _call_arguments(mock_update, real_update)
        assert "existing" in arguments["dfq"]["id"].astype(str).tolist()
        dff = arguments["dff"]
        assert dff["event_id_cnty"].tolist() == ["SOM1"]
        assert dff["event_date"].tolist() == ["0025-06-15"]  # a string, not a parsed (wrong) date
        assert dff["fatalities"].tolist() == [1] and dff["iso"].tolist() == [706]
        # Persisted: the questions update() returned and the hash mapping; no resolution files.
        assert local_bucket.read_questions("acled")["id"].astype(str).tolist() == ["q1"]
        assert local_bucket.read_hash_mapping("acled") == mapping
        assert local_bucket.list_resolution_ids("acled") == set()

    def test_update_chain_unmocked_generates_questions_and_their_hash_mapping(
        self, local_bucket, freeze_today
    ):
        freeze_today(date(2025, 6, 20))
        local_bucket.seed_questions("acled", [])
        local_bucket.seed_fetch(
            "acled", [make_acled_event(event_id_cnty="SOM1", event_date="2025-06-15")]
        )

        acled_update(None)

        # One country x (its event type + fatalities) x two question keys = four questions, every
        # one of them unhashable through the persisted mapping.
        questions = local_bucket.read_questions("acled")
        assert len(questions) == 4
        assert list(questions.columns) == constants.QUESTION_FILE_COLUMNS
        mapping = local_bucket.read_hash_mapping("acled")
        assert set(mapping) == set(questions["id"].astype(str))
        assert {entry["country"] for entry in mapping.values()} == {"Somalia"}
        assert local_bucket.list_resolution_ids("acled") == set()


class TestWikipediaDrivers:
    """Divergence: Wikipedia fetches one table per page and keeps one fetch file per page.

    The fetch driver writes each page's frame to ``wikipedia/fetch/<id_root>.jsonl`` and alerts on
    the pages the source reports as failed (their previous file stays in place); the update driver
    reads every per-page file back — an empty file as an empty frame, not a missing page — hands
    the mapping to ``update()`` and persists questions, resolution files and the hash mapping.
    """

    _PAGE = "FIDE_rankings_elo_rating"
    _OTHER_PAGE = "List_of_infectious_diseases"

    def test_fetch_driver_writes_one_file_per_page(self, local_bucket):
        fetched = {
            self._PAGE: pd.DataFrame(
                {"Player": ["Magnus Carlsen"], "Rating": [2850], "date": ["2024-07-01"]}
            ),
            self._OTHER_PAGE: pd.DataFrame(
                {"Common name": ["Measles"], "Vaccine(s)": ["Yes"], "date": ["2024-07-01"]}
            ),
        }
        source_cls = type(SOURCES["wikipedia"])
        with ExitStack() as stack:
            _seed_alert_attrs(stack, "wikipedia", POST_FETCH_ALERT_ATTRS)
            stack.enter_context(patch.object(source_cls, "fetch", return_value=fetched))
            alert = stack.enter_context(patch.object(wikipedia_fetch_main.slack, "send_message"))
            wikipedia_fetch_main.driver(None)

        assert local_bucket.list_wikipedia_fetch_pages() == {self._PAGE, self._OTHER_PAGE}
        assert local_bucket.read_wikipedia_fetch(self._PAGE)["Rating"].tolist() == [2850]
        assert local_bucket.read_fetch("wikipedia").empty  # no single-file layout
        assert not alert.called

    def test_fetch_driver_alerts_on_failed_pages_after_writing_the_rest(self, local_bucket):
        fetched = {
            self._PAGE: pd.DataFrame({"Player": ["A"], "Rating": [1], "date": ["2024-07-01"]})
        }
        source_cls = type(SOURCES["wikipedia"])
        failures = [f"{self._OTHER_PAGE}: no data was downloaded"]
        with patch.object(source_cls, "fetch", return_value=fetched), patch.object(
            source_cls, "failed_pages", failures, create=True
        ), patch.object(wikipedia_fetch_main.slack, "send_message") as alert:
            wikipedia_fetch_main.driver(None)

        assert local_bucket.list_wikipedia_fetch_pages() == {self._PAGE}
        assert alert.called
        assert self._OTHER_PAGE in alert.call_args.kwargs["message"]

    def test_update_driver_reads_every_page_file_and_persists_update_output(self, local_bucket):
        bank = make_question_df([{"id": "existing"}])
        local_bucket.seed_questions("wikipedia", bank.to_dict("records"))
        local_bucket.seed_wikipedia_fetch(
            self._PAGE, [{"Player": "Magnus Carlsen", "Rating": 2850, "date": "2024-07-01"}]
        )
        local_bucket.seed_wikipedia_fetch(self._OTHER_PAGE, [])  # fetched, but the table was empty
        mapping = {"w1": {"id_root": self._PAGE, "id_field_value": "Magnus Carlsen"}}
        local_bucket.seed_hash_mapping("wikipedia", mapping)
        canned = UpdateResult(
            dfq=make_question_df([{"id": "w1"}]),
            resolution_files={
                "w1": make_resolution_df([{"id": "w1", "date": "2024-07-01", "value": 2850.0}])
            },
            hash_mapping=mapping,
        )

        source_cls = type(SOURCES["wikipedia"])
        real_update = source_cls.update
        with patch.object(source_cls, "update", return_value=canned) as mock_update:
            wikipedia_update(None)

        arguments = _call_arguments(mock_update, real_update)
        assert "existing" in arguments["dfq"]["id"].astype(str).tolist()
        dff = arguments["dff"]
        assert set(dff) == {self._PAGE, self._OTHER_PAGE}
        assert dff[self._PAGE]["Rating"].tolist() == [2850]
        assert dff[
            self._OTHER_PAGE
        ].empty  # present as empty, so update() does not treat it as missing
        assert local_bucket.read_questions("wikipedia")["id"].astype(str).tolist() == ["w1"]
        assert list(local_bucket.read_resolution_file("wikipedia", "w1").columns) == [
            "id",
            "date",
            "value",
        ]
        assert local_bucket.read_hash_mapping("wikipedia") == mapping

    def test_update_chain_unmocked_creates_the_page_question_and_its_resolution_file(
        self, local_bucket
    ):
        from sources.wikipedia import _PAGES

        local_bucket.seed_questions("wikipedia", [])
        for page in _PAGES:
            local_bucket.seed_wikipedia_fetch(page["id_root"], [])
        local_bucket.seed_wikipedia_fetch(
            self._PAGE,
            [
                {"Player": "Magnus Carlsen", "Rating": 2839, "date": "2024-06-01"},
                {"Player": "Magnus Carlsen", "Rating": 2850, "date": "2024-07-01"},
            ],
        )

        wikipedia_update(None)

        questions = local_bucket.read_questions("wikipedia")
        assert len(questions) == 1
        wid = str(questions["id"].iloc[0])
        assert "Magnus Carlsen" in questions["question"].iloc[0]
        assert local_bucket.list_resolution_ids("wikipedia") == {wid}
        assert local_bucket.read_resolution_file("wikipedia", wid)["value"].tolist() == [
            2839.0,
            2850.0,
        ]
        assert local_bucket.read_hash_mapping("wikipedia")[wid]["id_root"] == self._PAGE


class TestManifoldUpdateDriver:
    """Divergence: Manifold is update-only — no new markets are fetched, published ones still update.

    There is no ``func_manifold_fetch`` job (and hence no fetch file) any more, so Manifold is
    absent from the parametrized wiring tests above. What its nightly job must still do is read
    the bank, run ``update()`` on it, and persist the refreshed questions plus any rebuilt
    resolution files — a stale Manifold bank would resolve questions on stale market values.
    """

    def test_reads_bank_and_persists_update_output(self, local_bucket):
        bank = make_question_df([{"id": "mkt1", "resolved": False}])
        local_bucket.seed_questions("manifold", bank.to_dict("records"))
        canned = UpdateResult(
            dfq=make_question_df([{"id": "mkt1", "resolved": True}]),
            resolution_files={
                "mkt1": make_resolution_df([{"id": "mkt1", "date": "2024-06-01", "value": 1.0}])
            },
        )

        real_update = type(SOURCES["manifold"]).update
        with patch.object(type(SOURCES["manifold"]), "update", return_value=canned) as mock_update:
            manifold_update(None)

        # The bank was read and handed to update(); there is no fetch frame to pass.
        arguments = _call_arguments(mock_update, real_update)
        assert arguments["dfq"]["id"].astype(str).tolist() == ["mkt1"]
        assert arguments["dff"] is None
        # ...and what update() returned is what got persisted.
        questions = local_bucket.read_questions("manifold")
        assert bool(questions.loc[questions["id"].astype(str) == "mkt1", "resolved"].iloc[0])
        assert local_bucket.list_resolution_ids("manifold") == {"mkt1"}


class TestInferUpdateDriver:
    """Divergence: INFER is retired — it is no longer fetched and its update() is a no-op.

    There is no ``func_infer_fetch`` job any more, so INFER is absent from the parametrized
    wiring tests above. Its update job still runs nightly, so the guarantee worth pinning is that
    it reads the bank, writes nothing back, and leaves the frozen resolution files alone. Run
    unmocked: the no-op ``update()`` touches no network.
    """

    def test_reads_bank_and_uploads_nothing(self, local_bucket):
        bank = make_question_df([{"id": "i1"}])
        local_bucket.seed_questions("infer", bank.to_dict("records"))
        local_bucket.seed_resolution_file("infer", "i1", [{"id": "i1", "date": "2024-06-01"}])

        infer_update(None)

        # Questions are not re-uploaded, and the frozen resolution files are untouched.
        assert local_bucket.read_questions("infer")["id"].astype(str).tolist() == ["i1"]
        assert local_bucket.list_resolution_ids("infer") == {"i1"}
        assert local_bucket.read_resolution_file("infer", "i1")["date"].tolist() == ["2024-06-01"]
