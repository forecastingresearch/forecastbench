"""Shared pytest fixtures for ForecastBench tests (builders live in factories.py)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from tests._harness import network

# Production modules are imported inside the fixtures, not here: this file is imported before
# ``pytest_configure`` runs, so a module-level import would execute production code before the
# network guard exists.


def pytest_configure(config):
    """Install the no-network guard before collection imports any production module.

    Test modules import the jobs they test at collection time; a production module that regressed
    to an import-time HTTP call would otherwise make that call once, unguarded, before the first
    test's fixtures run. The guard stays installed for the whole session; ``_guard_network`` lifts
    it only for ``@pytest.mark.live`` tests.
    """
    network.install()


def pytest_unconfigure(config):
    """Restore the real socket at session end."""
    network.uninstall()


# ---------------------------------------------------------------------------
# Time-freezing fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def freeze_today():
    """Patch dates.get_date_today() and get_date_yesterday() to return deterministic values.

    Usage:
        def test_something(freeze_today):
            freeze_today(date(2025, 1, 15))
            # Now dates.get_date_today() returns date(2025, 1, 15)
            # and dates.get_date_yesterday() returns date(2025, 1, 14)
    """
    patches = []

    def _freeze(target_date):
        target_datetime = datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
        )
        p1 = patch("helpers.dates.get_date_today", return_value=target_date)
        p2 = patch("helpers.dates.get_date_yesterday", return_value=target_date - timedelta(days=1))
        p3 = patch("helpers.dates.get_datetime_today", return_value=target_datetime)
        patches.extend([p1, p2, p3])
        for p in [p1, p2, p3]:
            p.start()

    yield _freeze

    # Unwind in reverse so nested freezes restore the original, not an intermediate mock.
    for p in reversed(patches):
        p.stop()


# ---------------------------------------------------------------------------
# Source instance fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def market_source():
    """Return a concrete MarketSource instance."""
    from sources.metaculus import MetaculusSource

    return MetaculusSource()


@pytest.fixture()
def dataset_source():
    """Return a concrete DatasetSource instance."""
    from sources.fred import FredSource

    return FredSource()


@pytest.fixture()
def acled_source():
    """Return an AcledSource instance."""
    from sources.acled import AcledSource

    return AcledSource()


@pytest.fixture()
def acled_source_with_creds():
    """Return an AcledSource instance with fake API credentials."""
    from sources.acled import AcledSource

    src = AcledSource()
    src.api_email = "test@example.com"
    src.api_password = "test-password"
    return src


@pytest.fixture()
def dbnomics_source():
    """Return a DbnomicsSource instance."""
    from sources.dbnomics import DbnomicsSource

    return DbnomicsSource()


@pytest.fixture()
def fred_source():
    """Return a FredSource instance with a fake API key."""
    from sources.fred import FredSource

    src = FredSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def kalshi_source():
    """Return a KalshiSource instance."""
    from sources.kalshi import KalshiSource

    return KalshiSource()


@pytest.fixture()
def manifold_source():
    """Return a ManifoldSource instance."""
    from sources.manifold import ManifoldSource

    return ManifoldSource()


@pytest.fixture()
def metaculus_source():
    """Return a MetaculusSource instance with a fake API key."""
    from sources.metaculus import MetaculusSource

    src = MetaculusSource()
    src.api_key = "test-key"
    return src


@pytest.fixture()
def polymarket_source():
    """Return a PolymarketSource instance."""
    from sources.polymarket import PolymarketSource

    return PolymarketSource()


@pytest.fixture()
def wikipedia_source():
    """Return a WikipediaSource instance."""
    from sources.wikipedia import WikipediaSource

    return WikipediaSource()


@pytest.fixture()
def yfinance_source():
    """Return a YfinanceSource instance with no pair questions and no rate-limit sleep.

    Tests of the pair pass set ``source.pairs`` themselves.
    """
    from sources.yfinance import YfinanceSource

    source = YfinanceSource()
    source.pairs = []
    with patch("sources.yfinance.time.sleep"):
        yield source


# ---------------------------------------------------------------------------
# Offline harness: no-network guard, fake secrets, local bucket mount
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _guard_network(request):
    """Block non-local network access for every test unless marked ``live``.

    An accidentally-unmocked HTTP call should fail loudly and deterministically rather than
    reach a live API: Python sockets and ``curl_cffi`` are guarded for the whole session (see
    ``pytest_configure``), and the Google Storage / Secret Manager / Cloud Run client constructors
    (native gRPC, invisible to the socket guard) raise. Tests that truly need the network use
    ``@pytest.mark.live``, which lifts the guard for their duration.
    """
    from google.cloud import run_v2, secretmanager, storage

    if request.node.get_closest_marker("live"):
        network.uninstall()
        try:
            yield
        finally:
            network.install()
        return

    def _blocked_client(name):
        def _construct(*args, **kwargs):
            raise network.BlockedNetworkError(
                f"{name} constructed in an offline test. Route the call through the local "
                "bucket / fake secrets, or mark the test @pytest.mark.live."
            )

        return _construct

    # Google's clients use native gRPC transports the socket guard cannot see, so fail closed at
    # construction: no offline test may build a Storage, Secret Manager or Cloud Run client at all.
    with patch.object(
        storage, "Client", _blocked_client("google.cloud.storage.Client")
    ), patch.object(
        secretmanager, "SecretManagerServiceClient", _blocked_client("SecretManagerServiceClient")
    ), patch.object(
        run_v2, "JobsClient", _blocked_client("google.cloud.run_v2.JobsClient")
    ):
        yield


@pytest.fixture(autouse=True)
def _fake_secrets(request):
    """Ensure secret access never hits Secret Manager during tests.

    ``helpers.keys`` resolves secrets lazily; here we patch the resolver to return a
    deterministic dummy and clear its cache, so any ``keys.API_KEY_*`` access is offline-safe
    even for code paths that read a key we didn't explicitly set.

    The metadata jobs also load the shared LLM registry's provider keys from Secret Manager
    (``metadata_llm._get_metadata_model_run`` calls ``configure_api_keys(from_gcp=True)`` and then
    refuses to run without them). That lookup is likewise faked here: the model run is still
    resolved from the real registry, but no key is fetched or required, and the cached run is
    dropped around each test so no test inherits another's. Skipped for ``@pytest.mark.live``
    tests, which need real secrets.
    """
    if request.node.get_closest_marker("live"):
        yield
        return

    from helpers import keys, metadata_llm

    keys._cache.clear()
    metadata_llm._get_metadata_model_run.cache_clear()
    with patch.object(
        keys, "get_secret", side_effect=lambda name, *a, **k: f"fake-{name}"
    ), patch.object(metadata_llm, "configure_api_keys"), patch.object(
        metadata_llm, "validate_provider_keys"
    ):
        yield
    keys._cache.clear()
    metadata_llm._get_metadata_model_run.cache_clear()


@pytest.fixture()
def local_bucket(tmp_path, monkeypatch):
    """Provide a temp-dir-backed stand-in for the project's GCS buckets.

    Sets ``GCP_STORAGE_LOCAL_ROOT`` / ``BUCKET_MOUNT_POINT`` and the ``*_BUCKET`` env vars so
    that all ``utils.gcp.storage`` operations read/write the temp tree instead of GCS. Returns a
    ``LocalBucket`` with helpers to seed inputs and read outputs using the real filename
    conventions.
    """
    import os

    from google.api_core.exceptions import NotFound
    from utils.gcp import storage

    from tests._harness.buckets import LocalBucket, apply_env

    bucket = LocalBucket(tmp_path / "buckets")
    apply_env(monkeypatch, bucket.env())

    # The jobs download into fixed ``/tmp/<name>`` paths and, after a tolerated 404, read whatever
    # is already there. In production that is a stale copy from an earlier run of the same
    # container; under pytest it would be another test's (or another process's) file. Make a
    # missing object leave no local file behind, so a test's outcome depends only on this bucket.
    real_tolerant_download = storage.download_no_error_message_on_404

    def _download_without_stale_fallback(bucket_name, filename, local_filename=None):
        try:
            return storage.download(
                bucket_name=bucket_name, filename=filename, local_filename=local_filename
            )
        except NotFound:
            if local_filename is None:
                local_filename = f"/tmp/{filename}"
            if os.path.exists(local_filename):
                os.remove(local_filename)
            print(f"GCP Storage: could not download {bucket_name}/{filename}.")
            return local_filename

    monkeypatch.setattr(
        storage, "download_no_error_message_on_404", _download_without_stale_fallback
    )
    assert real_tolerant_download is not _download_without_stale_fallback
    return bucket


# ---------------------------------------------------------------------------
# Source-contract helpers: fresh instances + offline update() adapter
# ---------------------------------------------------------------------------


def empty_question_bank():
    """Return an empty question bank with the canonical QuestionFrame columns (prod read shape)."""
    import pandas as pd

    from helpers import constants

    return pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)


@pytest.fixture()
def fresh_source():
    """Build a fresh source instance (never the registry singleton) for mutating tests.

    Sources carry mutable state (``api_key``, ``ticker_renames``); registry singletons must not
    be mutated across tests.
    """
    from sources import registry

    classes = {s.name: type(s) for s in registry.SOURCES.values()}

    def _make(name):
        src = classes[name]()
        if name in ("fred", "metaculus"):
            src.api_key = "test-key"
        if name == "yfinance":
            src.ticker_renames = []  # else update() fetches replacement tickers
        return src

    return _make


@pytest.fixture()
def offline_update_case(fresh_source):
    """Return a builder ``_case(name, stack) -> (source, dfq, dff)`` for an offline update() call.

    Most sources build their resolution data over the network inside ``update()``; patching each
    one's own network seam makes the call run offline:

    - ``kalshi``/``manifold``/``metaculus``/``yfinance``: ``_build_resolution_df`` (plus
      ``_get_market`` where the question fields come from a per-market request);
    - ``polymarket``: no seam either — its price history is embedded in the fetch frame, so the
      real resolution builder runs offline;
    - ``acled``/``dbnomics``/``fred``/``serpapi``/``wikipedia``: no seam needed — their
      ``update()`` already reads everything it needs out of the fetch data. ``dbnomics`` only has
      its series list pinned to a single entry so the case stays small and independent of the live
      question config; ``serpapi`` measures one configured snapshot series today; ``acled`` gets one
      event (its questions are generated per country x event type); ``wikipedia`` gets one FIDE
      rating observation and an empty frame for every other configured page (its fetch result is
      one frame per page, not a single frame).
    - ``manifold`` (update-only, no fetch frame): ``_build_resolution_df`` + ``_get_market``, with
      one unresolved question seeded in the bank and ``dff=None``, exactly as its driver calls it.

    This exercises update()'s *assembly* contract (dfq schema + exact columns, resolution_files
    packaging); resolution *content* is stubbed and asserted elsewhere. The caller supplies a
    ``contextlib.ExitStack`` so patches unwind cleanly.
    """
    import pandas as pd

    from tests import factories as f

    def _stub_resolution_df(src, stack):
        res = f.make_resolution_df([{"id": "x", "date": "2024-06-01", "value": 0.5}])
        stack.enter_context(patch.object(type(src), "_build_resolution_df", return_value=res))

    def _stub_market(src, stack, market):
        stack.enter_context(patch.object(type(src), "_get_market", return_value=market))

    def _acled(src, stack):
        # One event -> questions for one country x (its event type + fatalities) x each key.
        return f.make_acled_fetch_df(
            [{"event_id_cnty": "SOM1", "country": "Somalia", "event_date": "2025-06-15"}]
        )

    def _dbnomics(src, stack):
        from helpers import dates
        from sources import dbnomics

        question = dbnomics.DBNOMICS_QUESTIONS[0]
        stack.enter_context(patch.object(dbnomics, "DBNOMICS_QUESTIONS", [question]))
        # Recent enough that update() treats the series as fresh and curatable.
        period = dates.get_date_today() - timedelta(days=2)
        return f.make_dbnomics_fetch_df(
            [{"id": question["id"].replace("/", "_"), "period": period.isoformat(), "value": 6.0}]
        )

    def _fred(src, stack):
        return f.make_fred_fetch_df([{"id": "DGS10"}])

    def _kalshi(src, stack):
        _stub_resolution_df(src, stack)
        _stub_market(src, stack, f.make_kalshi_api_market(ticker="KXTEST-001"))
        return f.make_kalshi_fetch_df([{"id": "KXTEST-001"}])

    def _manifold(src, stack):
        # Update-only: no fetch frame exists any more, so update() works off the question bank.
        _stub_resolution_df(src, stack)
        _stub_market(src, stack, f.make_manifold_api_market(id="mkt1"))
        from helpers import constants

        # The bank as the driver reads it: canonical column order, one unresolved question.
        bank = f.make_question_df([{"id": "mkt1", "resolved": False}])
        return bank[constants.QUESTION_FILE_COLUMNS], None

    def _metaculus(src, stack):
        _stub_resolution_df(src, stack)
        _stub_market(src, stack, f.make_metaculus_market(id="mkt1"))
        return f.make_metaculus_fetch_df(["mkt1"])

    def _polymarket(src, stack):
        # No seam: polymarket's resolution data arrives embedded in the fetch frame, so its real
        # ``_build_resolution_df`` runs offline.
        return f.make_polymarket_fetch_df(
            [{"id": "m1", "historical_prices": [{"date": "2024-06-01", "value": 0.5}]}]
        )

    def _yfinance(src, stack):
        _stub_resolution_df(src, stack)
        return f.make_yfinance_fetch_df([{"id": "AAPL"}])

    def _serpapi(src, stack):
        # No seam needed: update() builds each question's history from the fetch frame alone. One
        # configured snapshot (price) series measured today, so the question is current and its
        # resolution file carries the canonical columns (flight series add ``fetch_datetime``).
        from helpers import dates
        from sources import serpapi
        from sources.serpapi_helpers import TODAY_ONLY_ENGINES

        name, spec = next(
            (n, s) for n, s in serpapi.QUESTION_SPECS.items() if s["engine"] in TODAY_ONLY_ENGINES
        )
        return f.make_serpapi_fetch_df(
            [
                {
                    "id": serpapi.question_id(name, spec["variables"][0]),
                    "date": dates.get_date_today().isoformat(),
                    "value": 12.5,
                }
            ]
        )

    def _wikipedia(src, stack):
        # Every configured page must be present in the fetch result; an empty frame means the
        # page had no data. One FIDE rating row is enough to assemble a question + resolution file.
        # The bank is seeded as the driver reads it (canonical column order, one question): on an
        # empty bank update() returns the new row's own column order, which only the first run ever
        # saw.
        from helpers import constants

        dff = f.make_wikipedia_fetch_result(
            FIDE_rankings_elo_rating=pd.DataFrame(
                {"Player": ["Magnus Carlsen"], "Rating": [2850], "date": ["2024-07-01"]}
            )
        )
        bank = f.make_question_df([{"id": "existing", "resolved": False}])
        return bank[constants.QUESTION_FILE_COLUMNS], dff

    builders = {
        "acled": _acled,
        "dbnomics": _dbnomics,
        "fred": _fred,
        "kalshi": _kalshi,
        "manifold": _manifold,
        "metaculus": _metaculus,
        "polymarket": _polymarket,
        "serpapi": _serpapi,
        "wikipedia": _wikipedia,
        "yfinance": _yfinance,
    }

    def _case(name, stack):
        src = fresh_source(name)
        built = builders[name](src, stack)
        if isinstance(built, tuple):  # (dfq, dff): the builder supplied the bank itself
            dfq, dff = built
            return src, dfq, dff
        return src, empty_question_bank(), built

    return _case
