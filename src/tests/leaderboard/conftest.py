"""Shared fixtures for leaderboard tests."""

import importlib

import pytest

from tests.leaderboard._leaderboard_import import patched_import_environment


@pytest.fixture
def leaderboard_main():
    """Yield `leaderboard.main`, imported credential-free."""
    with patched_import_environment():
        yield importlib.import_module("leaderboard.main")
