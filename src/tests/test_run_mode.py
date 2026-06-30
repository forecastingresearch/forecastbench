import pytest

from helpers.run_mode import RunMode


def test_run_mode_constructs_case_insensitively():
    assert RunMode("test") is RunMode.TEST
    assert RunMode("Prod") is RunMode.PROD


def test_run_mode_from_string_returns_mode_case_insensitively():
    assert RunMode.from_string("TEST") is RunMode.TEST
    assert RunMode.from_string("TeSt") is RunMode.TEST
    assert RunMode.from_string("prod") is RunMode.PROD
    assert RunMode.from_string("PrOd") is RunMode.PROD


def test_run_mode_from_string_defaults_to_test_for_missing_or_invalid_value():
    assert RunMode.from_string(None) is RunMode.TEST
    assert RunMode.from_string("DEV") is RunMode.TEST


def test_run_mode_constructor_raises_for_invalid_value():
    with pytest.raises(ValueError):
        RunMode("DEV")


def test_run_mode_predicates():
    assert RunMode.TEST.is_test is True
    assert RunMode.TEST.is_prod is False
    assert RunMode.PROD.is_test is False
    assert RunMode.PROD.is_prod is True
