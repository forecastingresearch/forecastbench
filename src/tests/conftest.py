"""Shared pytest fixtures for ForecastBench tests (builders live in factories.py)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from tests._harness import network

# Import production modules inside the fixtures: this file loads before ``pytest_configure``, so a
# module-level import would run production code before the network guard exists.


def pytest_configure(config):
    """Install the no-network guard before collection imports any production module.

    It stays installed for the session; ``_guard_network`` lifts it for ``@pytest.mark.live`` tests.
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
    """Block non-local network access for every test not marked ``live``."""
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

    # Google's clients use gRPC transports the socket guard misses, so block them at construction.
    with (
        patch.object(storage, "Client", _blocked_client("google.cloud.storage.Client")),
        patch.object(
            secretmanager,
            "SecretManagerServiceClient",
            _blocked_client("SecretManagerServiceClient"),
        ),
        patch.object(run_v2, "JobsClient", _blocked_client("google.cloud.run_v2.JobsClient")),
    ):
        yield


@pytest.fixture(autouse=True)
def _fake_secrets(request):
    """Serve every secret as a deterministic dummy instead of reading Secret Manager.

    The metadata jobs' shared-registry key setup (``configure_api_keys`` /
    ``validate_provider_keys``) is faked too; the model run still resolves from the real registry,
    and the cached run is cleared around each test. Skipped for ``@pytest.mark.live`` tests.
    """
    if request.node.get_closest_marker("live"):
        yield
        return

    from helpers import keys, metadata_llm

    keys._cache.clear()
    metadata_llm._get_metadata_model_run.cache_clear()
    with (
        patch.object(keys, "get_secret", side_effect=lambda name, *a, **k: f"fake-{name}"),
        patch.object(metadata_llm, "configure_api_keys"),
        patch.object(metadata_llm, "validate_provider_keys"),
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

    # The jobs read a fixed ``/tmp/<name>`` path after a tolerated 404, so delete any file there;
    # otherwise a test reads another test's (or another process's) download.
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
    """Return ``_case(name, stack)``, which builds ``(source, dfq, dff)`` for an offline update().

    Sources that build resolution data over the network inside ``update()`` get that seam patched
    (``_build_resolution_df``, plus ``_get_market`` where question fields come from a per-market
    request); the others read everything from the fetch data. The case pins ``update()``'s assembly
    (schema, exact columns, resolution-file packaging), not resolution content. The caller's
    ``contextlib.ExitStack`` unwinds the patches.
    """
    import pandas as pd

    from tests import factories as f

    def _stub_resolution_df(src, stack):
        res = f.make_resolution_df([{"id": "x", "date": "2024-06-01", "value": 0.5}])
        stack.enter_context(patch.object(type(src), "_build_resolution_df", return_value=res))

    def _stub_market(src, stack, market):
        stack.enter_context(patch.object(type(src), "_get_market", return_value=market))

    def _acled(src, stack):
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
        # Update-only: no fetch frame, so update() works off the question bank.
        _stub_resolution_df(src, stack)
        _stub_market(src, stack, f.make_manifold_api_market(id="mkt1"))
        from helpers import constants

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
        # A snapshot (price) series measured today: the question is current and its resolution
        # file carries the canonical columns (flight series add ``fetch_datetime``).
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
        # The bank is seeded as the driver reads it (canonical column order): on an empty bank
        # update() returns the new row's own column order.
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
        if isinstance(built, tuple):
            dfq, dff = built
            return src, dfq, dff
        return src, empty_question_bank(), built

    return _case
