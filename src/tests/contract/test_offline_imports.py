"""Offline-import contract: the job entry points and lazy helpers import with no GCP, network or
secrets, so a module that regresses to doing work at import time fails here.
"""

import importlib
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from google.api_core.exceptions import NotFound

_SRC = Path(__file__).resolve().parents[2]
_REPO_ROOT = _SRC.parent

_MISSING = object()

COLD_IMPORT_MODULES = [
    "orchestration.func_resolve.main",  # the registry, so every source, slack and keys
    "orchestration.func_kalshi_fetch.main",  # a market source's fetch job
    "leaderboard.main",
    "orchestration.func_question_set_create.main",  # question_sampling and every source
    "helpers.metadata_llm",  # metadata LLM model run built lazily; importing reads no secret
    "orchestration.func_fred_fetch.main",  # a keyed dataset fetch job: no secret read at import
    "orchestration.func_llm_forecaster_worker.main",  # runner + fb_model_runs + shared LLM registry
    "orchestration.func_llm_forecaster_manager.main",  # cloud_run glue + fb_model_runs
    # The metadata jobs: their shared-registry key loading must stay call-time. The in-process
    # test cannot see it (the offline fixtures fake ``configure_api_keys``), the cold import can.
    "metadata.tag_questions.main",
    "metadata.validate_questions.main",
]

JOB_MODULES = [
    "orchestration.func_acled_fetch.main",
    "orchestration.func_acled_update.main",
    "orchestration.func_dbnomics_fetch.main",
    "orchestration.func_dbnomics_update.main",
    "orchestration.func_fred_fetch.main",
    "orchestration.func_fred_update.main",
    "orchestration.func_kalshi_fetch.main",
    "orchestration.func_kalshi_update.main",
    "orchestration.func_polymarket_fetch.main",
    "orchestration.func_polymarket_update.main",
    "orchestration.func_manifold_update.main",
    "orchestration.func_metaculus_fetch.main",
    "orchestration.func_metaculus_update.main",
    "orchestration.func_serpapi_fetch.main",
    "orchestration.func_serpapi_update.main",
    "orchestration.func_wikipedia_fetch.main",
    "orchestration.func_wikipedia_update.main",
    # INFER is retired: no fetch job remains, but the no-op update job is still deployed.
    "orchestration.func_infer_update.main",
    "orchestration.func_yfinance_fetch.main",
    "orchestration.func_yfinance_update.main",
    "orchestration.func_resolve.main",
    "orchestration.func_push_datasets_to_git.main",
    "orchestration.func_llm_forecaster_worker.main",
    "orchestration.func_llm_forecaster_manager.main",
    "metadata.tag_questions.main",
    "metadata.validate_questions.main",
    "orchestration.func_question_set_create.main",
    "orchestration.func_question_set_publish.main",
    "base_eval.naive_and_dummy_forecasters.main",
    "nightly_update_workflow.manager.main",
    "nightly_update_workflow.worker.main",
]

IMPORT_TIME_SIDE_EFFECT_MODULES = [
    "helpers.keys",
    "helpers.env",
    "helpers.metadata_llm",
    "leaderboard.main",
]


@pytest.mark.parametrize("module_name", JOB_MODULES + IMPORT_TIME_SIDE_EFFECT_MODULES)
def test_module_imports_offline(module_name):
    """Importing the module touches neither the network nor Secret Manager.

    The module is evicted from ``sys.modules`` so its top-level body re-executes under the guards.
    Its ``sys.modules`` entry and parent-package attribute are restored afterwards; otherwise a
    re-imported ``helpers.keys`` would desync from the copy ``_fake_secrets`` patches.

    ``_fake_secrets`` makes secret reads succeed, so they are checked separately: a spy on
    ``keys.get_secret`` must see no call, and building a Secret Manager client fails (the only
    signal when ``helpers.keys`` itself is re-imported, with an unspied ``get_secret``).
    """
    from google.cloud import secretmanager

    from helpers import keys, metadata_llm

    saved = sys.modules.get(module_name)
    parent_name, _, leaf = module_name.rpartition(".")
    parent = sys.modules.get(parent_name) if parent_name else None
    saved_attr = getattr(parent, leaf, _MISSING) if parent is not None else _MISSING

    sys.modules.pop(module_name, None)
    keys._cache.clear()  # a cached key would bypass the spy
    secret_reads = []

    def _spy(name, *a, **k):
        secret_reads.append(name)
        return f"fake-{name}"

    def _no_secret_client(*a, **k):
        raise AssertionError(
            f"{module_name} constructed a Secret Manager client at import time (read a secret)."
        )

    def _no_registry_keys(**k):
        raise AssertionError(
            f"{module_name} loaded the shared LLM registry's provider keys at import time."
        )

    with (
        patch.object(keys, "get_secret", side_effect=_spy),
        patch.object(secretmanager, "SecretManagerServiceClient", _no_secret_client),
        patch.object(metadata_llm, "configure_api_keys", _no_registry_keys),
    ):
        try:
            mod = importlib.import_module(module_name)
            assert mod is not None
        finally:
            if saved is not None:
                sys.modules[module_name] = saved
            else:
                sys.modules.pop(module_name, None)
            if parent is not None:
                if saved_attr is not _MISSING:
                    setattr(parent, leaf, saved_attr)
                elif hasattr(parent, leaf):
                    delattr(parent, leaf)
    assert not secret_reads, (
        f"{module_name} read secret(s) {secret_reads} at import time — resolve them lazily "
        "(module __getattr__ / call-time) so importing the job touches no Secret Manager."
    )


@pytest.mark.parametrize("module_name", COLD_IMPORT_MODULES)
def test_module_imports_cold_in_subprocess(module_name):
    """Import the full chain in a fresh interpreter with the socket guard and no GCP credentials.

    Nothing is cached, so every transitive import re-executes; an import-time network, Secret
    Manager or GCS call makes the subprocess exit non-zero.
    """
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", "/tmp"),
        "PYTHONPATH": os.pathsep.join([str(_SRC), str(_REPO_ROOT)]),
        "CLOUD_PROJECT": "fake-offline-import-test",
        "BUCKET_MOUNT_POINT": "/tmp/fb_cold_import",
        "RUNNING_LOCALLY": "1",
        "GOOGLE_APPLICATION_CREDENTIALS": "/nonexistent",
        # Point library caches (e.g. matplotlib) at a writable temp dir so the import works in
        # constrained/read-only-HOME CI environments.
        "MPLCONFIGDIR": "/tmp/fb_mplconfig",
        "XDG_CACHE_HOME": "/tmp/fb_xdg_cache",
    }
    code = f"from tests._harness import network; network.install(); import {module_name}"
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"cold import of {module_name} failed:\n{result.stderr[-2000:]}"


def test_keys_are_lazy_and_memoized():
    """``keys.API_KEY_*`` resolves via get_secret on access, not at import, and is cached."""
    from helpers import keys

    keys._cache.clear()
    calls = []

    def fake_get_secret(name, *a, **k):
        calls.append(name)
        return f"secret-{name}"

    keys.get_secret = fake_get_secret
    try:
        assert not calls
        assert keys.API_KEY_FRED == "secret-API_KEY_FRED"
        assert keys.API_KEY_FRED == "secret-API_KEY_FRED"
        assert calls == ["API_KEY_FRED"]
        keys._cache.clear()
        assert keys.API_KEY_FRED == "secret-API_KEY_FRED"
        assert calls == ["API_KEY_FRED", "API_KEY_FRED"]
    finally:
        keys._cache.clear()


def test_unknown_key_raises_attribute_error():
    """An undefined key name raises AttributeError (not a silent None)."""
    from helpers import keys

    with pytest.raises(AttributeError):
        _ = keys.API_KEY_DOES_NOT_EXIST


def test_env_is_read_at_call_time(monkeypatch):
    """``env.X`` reflects environment changes made after import."""
    from helpers import env

    monkeypatch.setenv("QUESTION_BANK_BUCKET", "set-after-import")
    assert env.QUESTION_BANK_BUCKET == "set-after-import"
    monkeypatch.setenv("NUM_CPUS", "7")
    assert env.NUM_CPUS == 7


def test_storage_local_root_is_read_at_call_time(monkeypatch, tmp_path):
    """``storage`` reads GCP_STORAGE_LOCAL_ROOT at call time, not at import.

    While it is set, ``<root>/<bucket>`` is authoritative (a missing object raises ``NotFound``
    without a GCS lookup); once it is unset, ``download`` goes to GCS.
    """
    from utils.gcp import storage, storage_download_file

    bucket = "after-import-bucket"
    (tmp_path / bucket).mkdir()
    (tmp_path / bucket / "present.txt").write_text("local content")
    download_blob = Mock()
    monkeypatch.setattr(storage_download_file, "download_blob", download_blob)

    monkeypatch.setenv("GCP_STORAGE_LOCAL_ROOT", str(tmp_path))
    local_path = tmp_path / "present-copy.txt"
    storage.download(bucket, "present.txt", str(local_path))
    assert local_path.read_text() == "local content"
    with pytest.raises(NotFound):  # the GCS client's exception, so tolerant callers behave alike
        storage.download(bucket, "missing.txt", str(tmp_path / "missing-copy.txt"))
    download_blob.assert_not_called()

    monkeypatch.delenv("GCP_STORAGE_LOCAL_ROOT", raising=False)
    storage.download(bucket, "missing.txt", str(tmp_path / "gcs-copy.txt"))
    download_blob.assert_called_once()


def test_no_network_guard_blocks_real_connections():
    """The autouse guard blocks a non-local socket connection."""
    import socket

    from tests._harness.network import BlockedNetworkError

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with pytest.raises(BlockedNetworkError):
        s.connect(("93.184.216.34", 80))  # example.com
    s.close()


def test_smoke_test_tooling_imports_without_orchestration_io():
    """The LLM smoke-test tool imports with ``orchestration._io`` and ``termcolor`` blocked.

    It imports both at call time; a cold subprocess blocks them and imports the tool.
    """
    code = """
import importlib.abc
import sys

class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"orchestration._io", "termcolor"}:
            raise ModuleNotFoundError(f"No module named {fullname!r}")
        return None

sys.meta_path.insert(0, Blocker())
import llm_forecaster.smoke_test
print("ok")
"""

    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "ok"
