"""Which nightly jobs each source is scheduled for, per `run_fetch` / `run_update`."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

from sources import ALL_SOURCE_NAMES, SOURCE_METADATA

# Sources whose upstream we no longer pull new questions from.
_NOT_FETCHED = {"infer", "manifold"}


def test_every_source_has_scheduling_flags():
    # Both flags default to True, so a source that never mentions them still reads as scheduled
    # and no consumer needs a fallback.
    for source in ALL_SOURCE_NAMES:
        assert SOURCE_METADATA[source]["run_fetch"] in (True, False)
        assert SOURCE_METADATA[source]["run_update"] in (True, False)


def test_every_source_declares_whether_its_dfq_is_frozen():
    # Defaults to False, so a source that never mentions it reads as actively written and the
    # staleness guard in `orchestration/_io.py` needs no fallback.
    for source in ALL_SOURCE_NAMES:
        assert SOURCE_METADATA[source]["dfq_is_frozen"] in (True, False)


def test_only_infer_has_a_frozen_dfq():
    # Every other source has an update that writes its question file every night, INFER's is a
    # no-op. Manifold is no longer fetched but is still updated, so its dfq is not frozen.
    frozen = {s for s in ALL_SOURCE_NAMES if SOURCE_METADATA[s]["dfq_is_frozen"]}
    assert frozen == {"infer"}


def test_infer_is_updated_but_never_fetched():
    # INFER's upstream shut down. Its questions still need updating so they can be resolved.
    assert SOURCE_METADATA["infer"]["run_fetch"] is False
    assert SOURCE_METADATA["infer"]["run_update"] is True


def test_manifold_is_updated_but_never_fetched():
    # We stopped sampling Manifold, so no new markets are pulled in. Its questions still need
    # updating so they can be resolved.
    assert SOURCE_METADATA["manifold"]["run_fetch"] is False
    assert SOURCE_METADATA["manifold"]["run_update"] is True


def test_every_other_source_is_fetched_and_updated():
    for source in ALL_SOURCE_NAMES:
        if source in _NOT_FETCHED:
            continue
        assert SOURCE_METADATA[source]["run_fetch"] is True, source
        assert SOURCE_METADATA[source]["run_update"] is True, source


def test_sources_import_stays_lightweight():
    # Importing the lightweight `sources` surface must NOT pull the heavy registry
    # or any concrete-source dependency (this is the worker deploy-safety property).
    code = textwrap.dedent("""
        import sys
        import sources
        assert sources.SOURCE_METADATA["infer"]["run_fetch"] is False
        heavy = [m for m in ("sources.registry", "yfinance", "backoff") if m in sys.modules]
        assert not heavy, f"lightweight import pulled heavy modules: {heavy}"
        """)
    src_dir = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(src_dir)},
    )
    assert result.returncode == 0, result.stderr
