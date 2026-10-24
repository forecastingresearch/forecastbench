"""Socket-level no-network guard for the test suite.

Real network access is blocked by default so an accidentally-unmocked HTTP call fails loudly
and deterministically instead of hitting a live API. Connections to loopback are allowed (some
libraries open local sockets). Tests that genuinely need the network opt in with
``@pytest.mark.live``; the autouse fixture in ``conftest.py`` skips the block for them.
"""

import socket

# Loopback forms that are always allowed (some libraries open local sockets).
_ALLOWED_EXACT = {"::1", "localhost", "0.0.0.0", "", None}

_real_socket = socket.socket


def _is_local(host) -> bool:
    """Whether a connection target is loopback/local and may be allowed."""
    if host in _ALLOWED_EXACT:
        return True
    return isinstance(host, str) and host.startswith("127.")


class BlockedNetworkError(BaseException):
    """Raised when test code attempts a non-local network connection.

    Derives from ``BaseException``, not ``Exception``, on purpose: production code wraps its
    storage and API calls in tolerant ``except Exception`` blocks (a 404-tolerant download, a
    "could not find modified time" fallback, a Slack alert that must not abort a job). A policy
    violation must escape those blocks and fail the test, rather than be logged and replaced by a
    fallback value, or by a stale ``/tmp`` copy from an earlier test.

    This guards the Python ``socket`` layer, which covers ``requests``/``urllib``/``httpx`` and
    most clients. Native transports are handled separately: ``install()`` also disables
    ``curl_cffi`` (the HTTP client ``yfinance`` uses), and the autouse fixture makes the Google
    gRPC client constructors (Storage, Secret Manager, Cloud Run) raise this error.
    """


class _GuardedSocket(_real_socket):
    """A socket that refuses to connect to non-loopback addresses."""

    def connect(self, address):  # noqa: D102 - thin override
        self._guard(address)
        return super().connect(address)

    def connect_ex(self, address):  # noqa: D102 - thin override
        self._guard(address)
        return super().connect_ex(address)

    def _guard(self, address) -> None:
        if self.family == socket.AF_UNIX:
            return  # a filesystem socket is local by construction
        host = address[0] if isinstance(address, tuple) else address
        if not _is_local(host):
            raise BlockedNetworkError(
                f"Network access to {host!r} blocked in tests. Mock the HTTP call, or mark the "
                "test @pytest.mark.live if it must reach the network."
            )


def _blocked_curl_perform(self, *args, **kwargs):
    """Stand-in for ``curl_cffi.Curl.perform``: every libcurl transfer is a network access."""
    raise BlockedNetworkError(
        "curl_cffi transfer blocked in tests (yfinance uses it). Mock the call, or mark the test "
        "@pytest.mark.live if it must reach the network."
    )


def _curl_class():
    """Return ``curl_cffi.Curl`` if the package is installed, else None."""
    try:
        from curl_cffi import Curl
    except ImportError:  # pragma: no cover - depends on the installed job requirements
        return None
    return Curl


_real_curl_perform = getattr(_curl_class(), "perform", None)


def install() -> None:
    """Replace ``socket.socket`` with the guarded subclass and disable ``curl_cffi`` transfers."""
    socket.socket = _GuardedSocket
    curl = _curl_class()
    if curl is not None:
        curl.perform = _blocked_curl_perform


def uninstall() -> None:
    """Restore the real ``socket.socket`` and ``curl_cffi`` transfers."""
    socket.socket = _real_socket
    curl = _curl_class()
    if curl is not None and _real_curl_perform is not None:
        curl.perform = _real_curl_perform
