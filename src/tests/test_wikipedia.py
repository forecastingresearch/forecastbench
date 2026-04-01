"""Tests for WikipediaSource: _compare_values, _ffill_dfr, _transform_id, hash mapping."""

from datetime import date

import pandas as pd
import pytest

from sources.wikipedia import QuestionType, WikipediaSource
from tests.conftest import make_resolution_df

# ---------------------------------------------------------------------------
# _compare_values
# ---------------------------------------------------------------------------


class TestCompareValues:
    """Parametrized tests for WikipediaSource._compare_values."""

    @pytest.mark.parametrize(
        "question_type,res_val,due_val,expected",
        [
            # SAME
            (QuestionType.SAME, 100, 100, True),
            (QuestionType.SAME, 101, 100, False),
            (QuestionType.SAME, 99, 100, False),
            # SAME_OR_MORE
            (QuestionType.SAME_OR_MORE, 100, 100, True),
            (QuestionType.SAME_OR_MORE, 101, 100, True),
            (QuestionType.SAME_OR_MORE, 99, 100, False),
            # SAME_OR_LESS
            (QuestionType.SAME_OR_LESS, 100, 100, True),
            (QuestionType.SAME_OR_LESS, 99, 100, True),
            (QuestionType.SAME_OR_LESS, 101, 100, False),
            # MORE
            (QuestionType.MORE, 101, 100, True),
            (QuestionType.MORE, 100, 100, False),
            (QuestionType.MORE, 99, 100, False),
            # ONE_PERCENT_MORE
            (QuestionType.ONE_PERCENT_MORE, 101, 100, True),
            (QuestionType.ONE_PERCENT_MORE, 100.99, 100, False),
            (QuestionType.ONE_PERCENT_MORE, 100, 100, False),
            (QuestionType.ONE_PERCENT_MORE, 1010, 1000, True),
            (QuestionType.ONE_PERCENT_MORE, 1009.99, 1000, False),
        ],
    )
    def test_compare_values(self, question_type, res_val, due_val, expected):
        result = WikipediaSource._compare_values(question_type, res_val, due_val)
        assert result == expected

    def test_invalid_question_type_raises(self):
        with pytest.raises(ValueError, match="Invalid QuestionType"):
            WikipediaSource._compare_values("not_a_type", 100, 100)


# ---------------------------------------------------------------------------
# _transform_id
# ---------------------------------------------------------------------------


class TestTransformId:
    """Test deprecated ID mapping."""

    def test_mapped_id_returns_new_id(self):
        # First entry from _TRANSFORM_ID_MAPPING
        old_id = "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92"
        new_id = "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f"
        assert WikipediaSource._transform_id(old_id) == new_id

    def test_unmapped_id_returns_original(self):
        original = "not_a_mapped_id"
        assert WikipediaSource._transform_id(original) == original


# ---------------------------------------------------------------------------
# _ffill_dfr
# ---------------------------------------------------------------------------


class TestFfillDfr:
    """Test forward-fill of resolution values."""

    def test_fills_gaps_between_observations(self, freeze_today):
        freeze_today(date(2025, 1, 10))

        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 10},
                {"id": "q1", "date": "2025-01-05", "value": 20},
            ]
        )

        result = WikipediaSource._ffill_dfr(dfr)
        q1 = result[result["id"] == "q1"].sort_values("date")

        # Should have daily values from Jan 1 to Jan 9 (yesterday)
        assert len(q1) == 9
        # Jan 2-4 should be forward-filled with 10
        jan3_val = q1[q1["date"] == pd.Timestamp("2025-01-03")]["value"].iloc[0]
        assert jan3_val == 10
        # Jan 5 onward should be 20
        jan7_val = q1[q1["date"] == pd.Timestamp("2025-01-07")]["value"].iloc[0]
        assert jan7_val == 20

    def test_extends_to_yesterday(self, freeze_today):
        freeze_today(date(2025, 1, 10))

        dfr = make_resolution_df([{"id": "q1", "date": "2025-01-05", "value": 42}])

        result = WikipediaSource._ffill_dfr(dfr)
        q1 = result[result["id"] == "q1"]
        max_date = q1["date"].max()
        assert max_date == pd.Timestamp("2025-01-09")  # yesterday
        # All values should be 42
        assert (q1["value"] == 42).all()

    def test_multiple_ids_independent(self, freeze_today):
        freeze_today(date(2025, 1, 10))

        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-05", "value": 10},
                {"id": "q2", "date": "2025-01-07", "value": 20},
            ]
        )

        result = WikipediaSource._ffill_dfr(dfr)
        assert set(result["id"].unique()) == {"q1", "q2"}
        q1 = result[result["id"] == "q1"]
        q2 = result[result["id"] == "q2"]
        assert len(q1) == 5  # Jan 5-9
        assert len(q2) == 3  # Jan 7-9

    def test_explicit_nan_not_forward_filled(self, freeze_today):
        """Explicit NaN (off-the-charts) must be preserved, not filled over."""
        freeze_today(date(2025, 1, 10))

        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 10},
                {"id": "q1", "date": "2025-01-05", "value": float("nan")},
            ]
        )

        result = WikipediaSource._ffill_dfr(dfr)
        q1 = result[result["id"] == "q1"].sort_values("date")

        # Should have daily values from Jan 1 to Jan 9 (yesterday)
        assert len(q1) == 9

        # Jan 2-4 should be forward-filled with 10 (gap filling)
        for day in [2, 3, 4]:
            val = q1[q1["date"] == pd.Timestamp(f"2025-01-0{day}")]["value"].iloc[0]
            assert val == 10, f"Jan {day} should be 10"

        # Jan 5 was explicit NaN -- must NOT be filled
        jan5_val = q1[q1["date"] == pd.Timestamp("2025-01-05")]["value"].iloc[0]
        assert pd.isna(jan5_val), "Jan 5 explicit NaN should be preserved"

        # Jan 6-9 (extended to yesterday) should also be NaN
        for day in [6, 7, 8, 9]:
            val = q1[q1["date"] == pd.Timestamp(f"2025-01-0{day}")]["value"].iloc[0]
            assert pd.isna(val), f"Jan {day} should be NaN (off the charts)"


# ---------------------------------------------------------------------------
# Hash mapping
# ---------------------------------------------------------------------------


class TestWikipediaHashMapping:
    """Test hash mapping load, dump, and unhash."""

    def test_populate_hash_mapping(self):
        source = WikipediaSource()
        source.populate_hash_mapping('{"abc": {"id_root": "page1"}}')
        assert source.hash_mapping == {"abc": {"id_root": "page1"}}

    def test_load_empty_string(self):
        source = WikipediaSource()
        source.populate_hash_mapping("")
        assert source.hash_mapping == {}

    def test_dump_removes_deprecated_keys(self):
        source = WikipediaSource()
        deprecated_key = "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92"
        source.hash_mapping = {
            deprecated_key: {"id_root": "old"},
            "keep_me": {"id_root": "new"},
        }
        result = source.dump_hash_mapping()
        import json

        parsed = json.loads(result)
        assert deprecated_key not in parsed
        assert "keep_me" in parsed

    def test_id_unhash_applies_transform(self):
        source = WikipediaSource()
        old_id = "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92"
        new_id = "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f"
        source.hash_mapping = {new_id: {"id_root": "page1"}}
        result = source._id_unhash(old_id)
        assert result == {"id_root": "page1"}

    def test_id_unhash_not_found_returns_none(self):
        source = WikipediaSource()
        source.hash_mapping = {}
        assert source._id_unhash("nonexistent") is None


# ---------------------------------------------------------------------------
# nullified_questions
# ---------------------------------------------------------------------------


class TestWikipediaNullifiedQuestions:
    """Verify nullified questions are correctly defined."""

    def test_nullified_questions_count(self):
        assert len(WikipediaSource.nullified_questions) == len(
            [entry for entry in WikipediaSource.nullified_questions]
        )
        assert len(WikipediaSource.nullified_questions) > 0

    def test_nullified_questions_are_nullified_question_instances(self):
        from _fb_types import NullifiedQuestion

        for nq in WikipediaSource.nullified_questions:
            assert isinstance(nq, NullifiedQuestion)
            assert isinstance(nq.id, str)
            assert isinstance(nq.nullification_start_date, date)
