"""Credential-free import of `leaderboard.main` for leaderboard tests."""

import importlib
import types
from contextlib import chdir, contextmanager
from pathlib import Path

from tests._module_stubs import reset_modules, stubbed_modules

ROOT = Path(__file__).resolve().parents[3]

STUBS = {
    "pyfixest": types.SimpleNamespace(),
    "jinja2": types.SimpleNamespace(Template=object),
    "joblib": types.SimpleNamespace(Parallel=object, delayed=lambda fn: fn),
    "scipy": types.SimpleNamespace(),
    "scipy.stats": types.SimpleNamespace(norm=object()),
    "termcolor": types.SimpleNamespace(colored=lambda text, *args, **kwargs: text),
    "git": types.SimpleNamespace(
        Actor=object,
        Repo=object,
    ),
    "helpers.git": types.SimpleNamespace(),
    "helpers.slack": types.SimpleNamespace(),
}


@contextmanager
def patched_import_environment():
    """Stand in for the GCP credentials and heavy stats dependencies `leaderboard.main` needs.

    `leaderboard.main` reads files relative to its own directory at import time, hence the chdir.
    """
    with (
        stubbed_modules(STUBS),
        reset_modules("leaderboard.main", "llm_identities"),
        chdir(ROOT / "src" / "leaderboard"),
    ):
        yield


def import_leaderboard_main():
    """Import `leaderboard.main` without GCP credentials or the heavy stats dependencies."""
    with patched_import_environment():
        return importlib.import_module("leaderboard.main")
