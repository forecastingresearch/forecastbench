"""Tests for BaseSource: static utilities, nullification, resolve orchestration."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from _types import NullifiedQuestion, SourceType
from sources._base import BaseSource
from tests.conftest import make_forecast_df, make_question_df

# ---------------------------------------------------------------------------
# Test-only concrete subclass
# ---------------------------------------------------------------------------


class _StubSource(BaseSource):
    """Minimal concrete subclass for testing BaseSource."""

    name = "stub"
    display_name = "Stub"
    source_type = SourceType.DATASET

    def _resolve(self, df, dfq, dfr):
        df["resolved_to"] = 1.0
        df["resolved"] = True
        return df


class _StubSourceWithNullified(BaseSource):
    """Concrete subclass with nullified questions."""

    name = "stub_null"
    display_name = "StubNull"
    source_type = SourceType.DATASET
    nullified_questions = [
        NullifiedQuestion(id="null_q1", nullification_start_date=date(2024, 6, 1)),
        NullifiedQuestion(id="null_q2", nullification_start_date=date(2025, 1, 1)),
    ]

    def _resolve(self, df, dfq, dfr):
        df["resolved_to"] = 1.0
        df["resolved"] = True
        return df


# ---------------------------------------------------------------------------
# __init_subclass__
# ---------------------------------------------------------------------------


class TestInitSubclass:
    """Test that concrete sources must define required ClassVars."""

    def test_missing_name_raises(self):
        with pytest.raises(TypeError, match="must define ClassVar 'name'"):

            class _BadSource(BaseSource):
                display_name = "Bad"
                source_type = SourceType.DATASET

                def _resolve(self, df, dfq, dfr):
                    return df

    def test_missing_display_name_raises(self):
        with pytest.raises(TypeError, match="must define ClassVar 'display_name'"):

            class _BadSource(BaseSource):
                name = "bad"
                source_type = SourceType.DATASET

                def _resolve(self, df, dfq, dfr):
                    return df

    def test_valid_concrete_source_ok(self):
        # Should not raise
        class _GoodSource(BaseSource):
            name = "good"
            display_name = "Good"
            source_type = SourceType.MARKET

            def _resolve(self, df, dfq, dfr):
                return df


# ---------------------------------------------------------------------------
# _is_combo
# ---------------------------------------------------------------------------


class TestIsCombo:
    """Test combo question detection."""

    def test_series_with_tuple_id(self):
        row = pd.Series({"id": ("a", "b"), "source": "test"})
        assert BaseSource._is_combo(row) is True

    def test_series_with_string_id(self):
        row = pd.Series({"id": "single_id", "source": "test"})
        assert BaseSource._is_combo(row) is False

    def test_raw_tuple(self):
        assert BaseSource._is_combo(("a", "b")) is True

    def test_raw_string(self):
        assert BaseSource._is_combo("single_id") is False

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Problem in `_is_combo`"):
            BaseSource._is_combo(42)


# ---------------------------------------------------------------------------
# _combo_change_sign
# ---------------------------------------------------------------------------


class TestComboChangeSign:
    """Test binary value flipping based on direction sign."""

    def test_sign_positive_passthrough(self):
        assert BaseSource._combo_change_sign(1, 1) == 1
        assert BaseSource._combo_change_sign(0, 1) == 0
        assert BaseSource._combo_change_sign(0.7, 1) == 0.7

    def test_sign_negative_flips(self):
        assert BaseSource._combo_change_sign(1, -1) == 0
        assert BaseSource._combo_change_sign(0, -1) == 1
        assert BaseSource._combo_change_sign(0.7, -1) == pytest.approx(0.3)

    def test_invalid_sign_raises(self):
        with pytest.raises(ValueError, match="Wrong value for sign"):
            BaseSource._combo_change_sign(1, 2)


# ---------------------------------------------------------------------------
# _get_question
# ---------------------------------------------------------------------------


class TestGetQuestion:
    """Test question lookup by ID."""

    def test_found(self):
        dfq = make_question_df([{"id": "q1"}, {"id": "q2"}])
        result = BaseSource._get_question(dfq, "q1")
        assert result is not None
        assert result["id"] == "q1"

    def test_not_found(self):
        dfq = make_question_df([{"id": "q1"}])
        assert BaseSource._get_question(dfq, "missing") is None


# ---------------------------------------------------------------------------
# _make_columns_hashable
# ---------------------------------------------------------------------------


class TestMakeColumnsHashable:
    """Test list-to-tuple conversion for id/direction columns."""

    def test_converts_lists_to_tuples(self):
        df = pd.DataFrame({"id": [["a", "b"], "c"], "direction": [[1, -1], ()]})
        result = BaseSource._make_columns_hashable(df)
        assert result["id"].iloc[0] == ("a", "b")
        assert result["direction"].iloc[0] == (1, -1)

    def test_handles_nan(self):
        df = pd.DataFrame({"id": ["a", np.nan], "direction": [(), np.nan]})
        result = BaseSource._make_columns_hashable(df)
        assert result["id"].iloc[1] == ()
        assert result["direction"].iloc[1] == ()

    def test_missing_columns_no_error(self):
        df = pd.DataFrame({"other": [1, 2]})
        result = BaseSource._make_columns_hashable(df)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _id_is_nullified
# ---------------------------------------------------------------------------


class TestIdIsNullified:
    """Test nullification check for single and combo IDs."""

    def test_single_in_set(self):
        assert BaseSource._id_is_nullified("q1", {"q1", "q2"}) is True

    def test_single_not_in_set(self):
        assert BaseSource._id_is_nullified("q3", {"q1", "q2"}) is False

    def test_tuple_with_one_in_set(self):
        assert BaseSource._id_is_nullified(("q1", "q3"), {"q1", "q2"}) is True

    def test_tuple_with_none_in_set(self):
        assert BaseSource._id_is_nullified(("q3", "q4"), {"q1", "q2"}) is False


# ---------------------------------------------------------------------------
# get_nullified_ids
# ---------------------------------------------------------------------------


class TestGetNullifiedIds:
    """Test nullified ID filtering by date."""

    def test_no_nullified_questions(self):
        source = _StubSource()
        assert source.get_nullified_ids() == set()

    def test_as_of_none_returns_all(self):
        source = _StubSourceWithNullified()
        result = source.get_nullified_ids(as_of=None)
        assert result == {"null_q1", "null_q2"}

    def test_as_of_filters_by_date(self):
        source = _StubSourceWithNullified()
        # Before both → empty
        assert source.get_nullified_ids(as_of=date(2024, 1, 1)) == set()
        # After first, before second → just null_q1
        assert source.get_nullified_ids(as_of=date(2024, 7, 1)) == {"null_q1"}
        # After both → both
        assert source.get_nullified_ids(as_of=date(2025, 6, 1)) == {"null_q1", "null_q2"}

    def test_as_of_exact_date_inclusive(self):
        source = _StubSourceWithNullified()
        assert source.get_nullified_ids(as_of=date(2024, 6, 1)) == {"null_q1"}


# ---------------------------------------------------------------------------
# resolve() orchestration
# ---------------------------------------------------------------------------


class TestResolveOrchestration:
    """Test the resolve() method's nullification and delegation logic."""

    def test_no_nullified_questions_delegates_directly(self):
        source = _StubSource()
        df = make_forecast_df(
            [
                {"id": "q1", "source": "stub", "forecast_due_date": "2025-01-01"},
                {"id": "q2", "source": "stub", "forecast_due_date": "2025-01-01"},
            ]
        )
        dfq = make_question_df([{"id": "q1"}, {"id": "q2"}])
        dfr = pd.DataFrame()

        result = source.resolve(df, dfq, dfr)
        assert (result["resolved_to"] == 1.0).all()
        assert (result["resolved"]).all()

    def test_nullified_rows_get_nan(self):
        source = _StubSourceWithNullified()
        df = make_forecast_df(
            [
                {"id": "null_q1", "source": "stub_null", "forecast_due_date": "2025-01-01"},
                {"id": "normal_q", "source": "stub_null", "forecast_due_date": "2025-01-01"},
            ]
        )
        dfq = make_question_df([{"id": "null_q1"}, {"id": "normal_q"}])
        dfr = pd.DataFrame()

        result = source.resolve(df, dfq, dfr)
        null_row = result[result["id"] == "null_q1"].iloc[0]
        normal_row = result[result["id"] == "normal_q"].iloc[0]

        assert pd.isna(null_row["resolved_to"])
        assert bool(null_row["resolved"]) is True
        assert normal_row["resolved_to"] == 1.0

    def test_nullified_combo_question(self):
        source = _StubSourceWithNullified()
        df = make_forecast_df(
            [
                {
                    "id": ("null_q1", "normal_q"),
                    "source": "stub_null",
                    "direction": (1, 1),
                    "forecast_due_date": "2025-01-01",
                },
            ]
        )
        dfq = make_question_df([{"id": "null_q1"}, {"id": "normal_q"}])
        dfr = pd.DataFrame()

        result = source.resolve(df, dfq, dfr)
        assert pd.isna(result.iloc[0]["resolved_to"])
        assert bool(result.iloc[0]["resolved"]) is True

    def test_empty_source_with_nullified_rows(self):
        source = _StubSourceWithNullified()
        # All rows are nullified → _resolve() should not be called
        df = make_forecast_df(
            [
                {"id": "null_q1", "source": "stub_null", "forecast_due_date": "2025-01-01"},
            ]
        )
        dfq = make_question_df([{"id": "null_q1"}])
        dfr = pd.DataFrame()

        result = source.resolve(df, dfq, dfr)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["resolved_to"])
