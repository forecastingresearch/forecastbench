"""Regression tests for the ACLED fetch job."""

import importlib.util
import sys
import types
import warnings
from collections import Counter
from pathlib import Path

import backoff._sync
import requests


def _load_acled_fetch_module(monkeypatch):
    """Load the ACLED fetch entrypoint with stubbed external dependencies."""
    import helpers

    fake_keys = types.ModuleType("helpers.keys")
    fake_keys.API_EMAIL_ACLED = "test@example.com"
    fake_keys.API_PASSWORD_ACLED = "secret"
    monkeypatch.setitem(sys.modules, "helpers.keys", fake_keys)
    monkeypatch.setattr(helpers, "keys", fake_keys, raising=False)

    fake_gcp = types.ModuleType("utils.gcp")
    fake_gcp.storage = types.SimpleNamespace(upload=lambda **kwargs: None)
    fake_archiving = types.ModuleType("utils.archiving")
    fake_utils = types.ModuleType("utils")
    fake_utils.archiving = fake_archiving
    fake_utils.gcp = fake_gcp
    monkeypatch.setitem(sys.modules, "utils.gcp", fake_gcp)
    monkeypatch.setitem(sys.modules, "utils.archiving", fake_archiving)
    monkeypatch.setitem(sys.modules, "utils", fake_utils)

    module_path = Path(__file__).resolve().parents[1] / "questions" / "acled" / "fetch" / "main.py"
    module_name = "tests._acled_fetch_main"
    sys.modules.pop(module_name, None)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*asyncio\.iscoroutinefunction.*deprecated.*",
            category=DeprecationWarning,
        )
        spec.loader.exec_module(module)
    return module


class _FakeResponse:
    """Minimal `requests.Response` test double."""

    def __init__(self, payload, *, error=None):
        self._payload = payload
        self._error = error
        self.status_code = 524 if error else 200
        self.text = ""

    @property
    def ok(self):
        return self._error is None

    def raise_for_status(self):
        if self._error is not None:
            raise self._error

    def json(self):
        return self._payload


def test_page_scoped_retry_does_not_restart_pagination(monkeypatch):
    module = _load_acled_fetch_module(monkeypatch)
    monkeypatch.setattr(backoff._sync.time, "sleep", lambda _: None)

    page_attempts = Counter()
    requested_pages = []

    def fake_get(_endpoint, headers=None, params=None, verify=None):
        del headers, verify
        page = params["page"]
        requested_pages.append(page)
        page_attempts[page] += 1

        if page == 1:
            return _FakeResponse(
                {
                    "count": 1,
                    "data": [
                        {
                            "event_id_cnty": "evt-1",
                            "event_date": "2024-01-01",
                            "iso": 1,
                            "region": "Region",
                            "country": "Country",
                            "admin1": "Admin",
                            "event_type": "Battles",
                            "fatalities": 1,
                            "timestamp": "1704067200",
                        }
                    ],
                }
            )

        if page == 2 and page_attempts[page] == 1:
            error = requests.exceptions.HTTPError("524 Server Error")
            return _FakeResponse({}, error=error)

        if page == 2:
            return _FakeResponse(
                {
                    "count": 1,
                    "data": [
                        {
                            "event_id_cnty": "evt-2",
                            "event_date": "2024-01-02",
                            "iso": 1,
                            "region": "Region",
                            "country": "Country",
                            "admin1": "Admin",
                            "event_type": "Riots",
                            "fatalities": 2,
                            "timestamp": "1704153600",
                        }
                    ],
                }
            )

        if page == 3:
            return _FakeResponse({"count": 0, "data": []})

        raise AssertionError(f"Unexpected page request: {page}")

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.certifi, "where", lambda: "/tmp/cacert.pem")

    df = module.get_acled_events(access_token="token")

    assert requested_pages == [1, 2, 2, 3]
    assert requested_pages.count(1) == 1
    assert list(df["event_id_cnty"]) == ["evt-1", "evt-2"]
