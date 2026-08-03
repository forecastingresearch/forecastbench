"""Which nightly jobs each source is scheduled for, per `run_fetch` / `run_update`."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

from sources import ALL_SOURCE_NAMES, SOURCE_METADATA


def test_every_source_has_scheduling_flags():
    # Both flags default to True, so a source that never mentions them still reads as scheduled
    # and no consumer needs a fallback.
    for source in ALL_SOURCE_NAMES:
        assert SOURCE_METADATA[source]["run_fetch"] in (True, False)
        assert SOURCE_METADATA[source]["run_update"] in (True, False)


def test_infer_is_updated_but_never_fetched():
    # INFER's upstream shut down. Its questions still need updating so they can be resolved.
    assert SOURCE_METADATA["infer"]["run_fetch"] is False
    assert SOURCE_METADATA["infer"]["run_update"] is True


def test_every_other_source_is_fetched_and_updated():
    for source in ALL_SOURCE_NAMES:
        if source == "infer":
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
