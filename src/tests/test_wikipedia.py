"""Tests for WikipediaSource: resolution, fetch, and update."""

from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest

from sources.wikipedia import QuestionType, WikipediaSource
from tests.conftest import make_forecast_df, make_question_df, make_resolution_df

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


# ---------------------------------------------------------------------------
# Clean / value / resolved functions
# ---------------------------------------------------------------------------


class TestCleanFunctions:
    """Test the per-page cleaning helpers referenced by _PAGES."""

    def test_clean_fide_rankings_replaces_names_and_drops_change_rows(self):
        df = pd.DataFrame(
            {
                "Player": [
                    "Gukesh D.",
                    "Leinier Dominguez",
                    "Change from the previous month",
                    "Magnus Carlsen",
                ],
                "Rating": [2780, 2750, 0, 2839],
            }
        )
        result = WikipediaSource.clean_FIDE_rankings(df)
        players = list(result["Player"])
        assert "Change from the previous month" not in players
        assert "Gukesh Dommaraju" in players
        assert "Leinier Domínguez Pérez" in players
        assert "Magnus Carlsen" in players

    def test_clean_swimming_drops_parens_and_metadata_rows(self):
        df = pd.DataFrame(
            {
                "Name": ["Sarah Sjöström", "Someone (relay)", "eventsort", "recordinfo"],
                "Event": ["50m freestyle", "x", "y", "z"],
            }
        )
        result = WikipediaSource.clean_List_of_world_records_in_swimming(df)
        assert list(result["Name"]) == ["Sarah Sjöström"]

    def test_clean_infectious_diseases_maps_yes_no_to_binary(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-06-01", "2024-06-01", "2024-06-01", "2024-06-01"]),
                "Common name": ["Measles", "Smallpox", "DiseaseX", "DiseaseY"],
                "Vaccine(s)": ["Yes, since 1963", "No", "Under research[1]", "Under Development"],
            }
        )
        result = WikipediaSource.clean_List_of_infectious_diseases(df)
        mapping = dict(zip(result["Common name"], result["Vaccine(s)"]))
        assert mapping["Measles"] == 1
        assert mapping["Smallpox"] == 0
        assert mapping["DiseaseX"] == 0
        assert mapping["DiseaseY"] == 0

    def test_is_resolved_infectious_diseases(self):
        assert WikipediaSource.is_resolved_List_of_infectious_diseases(1) is True
        assert WikipediaSource.is_resolved_List_of_infectious_diseases("Yes") is True
        assert WikipediaSource.is_resolved_List_of_infectious_diseases(0) is False

    def test_get_value_infectious_diseases(self):
        assert WikipediaSource.get_value_List_of_infectious_diseases(1) == "Yes"
        assert WikipediaSource.get_value_List_of_infectious_diseases(0) == "No"


# ---------------------------------------------------------------------------
# _fill_template
# ---------------------------------------------------------------------------


class TestFillTemplate:
    """Test question/explanation template filling."""

    def test_fills_id_keeps_date_placeholders(self):
        page = {
            "question": (
                "Will {id} have an Elo rating on {resolution_date} higher than "
                "{forecast_due_date}?",
                ("id",),
            )
        }
        result = WikipediaSource._fill_template(
            page=page, page_key="question", values={"id": "Magnus Carlsen"}
        )
        assert "Magnus Carlsen" in result
        # date placeholders are preserved for later formatting
        assert "{resolution_date}" in result
        assert "{forecast_due_date}" in result


# ---------------------------------------------------------------------------
# _build_resolution_df
# ---------------------------------------------------------------------------


def _fide_elo_page():
    from sources.wikipedia import _PAGES

    return next(p for p in _PAGES if p["id_root"] == "FIDE_rankings_elo_rating")


class TestBuildResolutionDf:
    """Test resolution-DataFrame construction from fetched page data."""

    def test_returns_none_when_data_is_stale(self):
        page = _fide_elo_page()
        # All data well before QUESTION_BANK_DATA_STORAGE_START_DATE (2023-05-07).
        dff = pd.DataFrame(
            {
                "Player": ["Old Player", "Old Player"],
                "Rating": [2700, 2710],
                "date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            }
        )
        question_key = pd.Series({"Player": "Old Player"})
        result = WikipediaSource._build_resolution_df(
            dff=dff, page=page, wid="wid1", question_key=question_key
        )
        assert result is None

    def test_builds_id_date_value_frame(self):
        page = _fide_elo_page()
        dff = pd.DataFrame(
            {
                "Player": ["Magnus Carlsen", "Magnus Carlsen"],
                "Rating": [2839, 2850],
                "date": pd.to_datetime(["2024-06-01", "2024-07-01"]),
            }
        )
        question_key = pd.Series({"Player": "Magnus Carlsen"})
        result = WikipediaSource._build_resolution_df(
            dff=dff, page=page, wid="wid1", question_key=question_key
        )
        assert list(result.columns) == ["id", "date", "value"]
        assert (result["id"] == "wid1").all()
        # id/date are cast to str (RESOLUTION_FILE_COLUMN_DTYPE); value stays mixed (ANY).
        assert result["id"].dtype == object and isinstance(result["id"].iloc[0], str)
        assert isinstance(result["date"].iloc[0], str)
        assert list(result["value"]) == [2839, 2850]

    def test_fills_nan_when_item_drops_out_of_table(self):
        """A date present in the page but missing for this question gets a None value.

        Player A is absent on 2024-07-01 (a date that exists in the page because Player B was
        recorded then), so a None row must be inserted for A — the core fill_missing_with_nan
        behavior described in the method docstring.
        """
        page = _fide_elo_page()
        dff = pd.DataFrame(
            {
                "Player": ["A", "B", "A"],
                "Rating": [2800, 2700, 2820],
                "date": pd.to_datetime(["2024-06-01", "2024-07-01", "2024-08-01"]),
            }
        )
        question_key = pd.Series({"Player": "A"})
        result = WikipediaSource._build_resolution_df(
            dff=dff, page=page, wid="widA", question_key=question_key
        )
        by_date = dict(zip(result["date"], result["value"]))
        assert set(by_date) == {"2024-06-01", "2024-07-01", "2024-08-01"}
        assert by_date["2024-06-01"] == 2800
        assert by_date["2024-08-01"] == 2820
        assert pd.isna(by_date["2024-07-01"])  # dropped out -> None


# ---------------------------------------------------------------------------
# update() — end to end
# ---------------------------------------------------------------------------


class TestUpdate:
    """Behavioral tests for WikipediaSource.update()."""

    def test_creates_question_and_resolution_file(self, wikipedia_source):
        dff = {
            "FIDE_rankings_elo_rating": pd.DataFrame(
                {
                    "Player": ["Magnus Carlsen", "Magnus Carlsen"],
                    "Rating": [2839, 2850],
                    "date": ["2024-06-01", "2024-07-01"],
                }
            )
        }
        dfq = make_question_df([{"id": "seed"}]).iloc[0:0]
        result = wikipedia_source.update(dfq, dff)

        # One question created.
        added = result.dfq[result.dfq["question"].str.contains("Magnus Carlsen", na=False)]
        assert len(added) == 1
        row = added.iloc[0]
        assert "Elo rating" in row["question"]
        assert row["freeze_datetime_value_explanation"] == "Magnus Carlsen's ELO rating."
        assert row["url"] == "https://en.wikipedia.org/wiki/FIDE_rankings"
        assert not row["resolved"]
        # freeze_datetime_value is the latest fetched rating (coerced to str by QuestionFrame).
        assert row["freeze_datetime_value"] == "2850"

        # One resolution file, keyed by the hashed question id.
        wid = row["id"]
        assert wid in result.resolution_files
        assert list(result.resolution_files[wid].columns) == ["id", "date", "value"]

        # Hash mapping populated so the id can be unhashed back to its root.
        assert result.hash_mapping[wid]["id_root"] == "FIDE_rankings_elo_rating"

    def test_skips_pages_absent_from_fetch(self, wikipedia_source):
        dfq = make_question_df([{"id": "seed"}]).iloc[0:0]
        result = wikipedia_source.update(dfq, {})
        # No fetch data -> no questions added, no resolution files.
        assert result.resolution_files == {}

    def test_resolves_questions_for_dropped_pages(self, wikipedia_source):
        # A pre-existing question whose id is not in the hash mapping / not a current page
        # is marked resolved.
        dfq = make_question_df([{"id": "unknown_id", "resolved": False}])
        result = wikipedia_source.update(dfq, {})
        assert result.dfq[result.dfq["id"] == "unknown_id"]["resolved"].iloc[0]

    def test_resolves_questions_for_id_transformations(self):
        old_id = "d4fd9e41e71c3e5a2992b9c8b36ff655eb7265b7a46a434484f1267eabd59b92"
        new_id = "a1c131d5c2ad476fc579b30b72ea6762e3b6324b0252a57c10c890436604f44f"
        dfq = make_question_df(
            [
                {"id": old_id, "resolved": False},
                {"id": new_id, "resolved": True},
            ]
        )
        result = WikipediaSource._resolve_questions_for_id_transformations(dfq)
        # When the new id is resolved, the deprecated old id is resolved too.
        assert result[result["id"] == old_id]["resolved"].iloc[0]

    def test_infectious_disease_uses_is_resolved_and_value_funcs(self, wikipedia_source):
        """The is_resolved_func / value_func path: vaccine=Yes -> resolved, freeze value 'Yes'."""
        dff = {
            "List_of_infectious_diseases": pd.DataFrame(
                {
                    "Common name": ["Measles", "DiseaseX"],
                    "Vaccine(s)": ["Yes", "No"],
                    "date": ["2024-06-01", "2024-06-01"],
                }
            )
        }
        dfq = make_question_df([{"id": "seed"}]).iloc[0:0]
        result = wikipedia_source.update(dfq, dff)

        measles = result.dfq[result.dfq["question"].str.contains("Measles", na=False)].iloc[0]
        diseasex = result.dfq[result.dfq["question"].str.contains("DiseaseX", na=False)].iloc[0]
        # Vaccine present -> resolved, freeze value rendered as "Yes" by value_func.
        assert measles["resolved"]
        assert measles["freeze_datetime_value"] == "Yes"
        # No vaccine -> unresolved, "No".
        assert not diseasex["resolved"]
        assert diseasex["freeze_datetime_value"] == "No"

    def test_updates_existing_question_in_place(self, wikipedia_source):
        """A question already in dfq is updated, not duplicated (the _add_to_dfq update branch)."""
        wid = wikipedia_source._id_hash(
            id_root="FIDE_rankings_elo_rating", id_field_value="Magnus Carlsen"
        )
        dfq = make_question_df([{"id": wid, "question": "STALE QUESTION", "resolved": False}])
        dff = {
            "FIDE_rankings_elo_rating": pd.DataFrame(
                {
                    "Player": ["Magnus Carlsen"],
                    "Rating": [2850],
                    "date": ["2024-07-01"],
                }
            )
        }
        result = wikipedia_source.update(dfq, dff)
        matching = result.dfq[result.dfq["id"] == wid]
        assert len(matching) == 1  # updated in place, not appended
        assert "Magnus Carlsen" in matching.iloc[0]["question"]
        assert "STALE" not in matching.iloc[0]["question"]


# ---------------------------------------------------------------------------
# _download_tables (fetch data shaping, network calls mocked)
# ---------------------------------------------------------------------------


class TestDownloadTables:
    """_download_tables selects columns, stamps the date, coerces value dtype, drops NaN."""

    def test_coerces_value_dtype_and_drops_unparseable_rows(self):
        page = _fide_elo_page()  # fields Player/Rating, value dtype int
        raw = pd.DataFrame({"Player": ["A", "B"], "Rating": ["2800", "not-a-number"]})
        with patch.object(
            WikipediaSource,
            "_get_edit_history",
            return_value=[(datetime(2024, 6, 1, 12, 0), "rev1")],
        ), patch.object(WikipediaSource, "_download_wikipedia_table", return_value=raw.copy()):
            result = WikipediaSource._download_tables(page, session=object())

        # "not-a-number" -> NaN -> row dropped; A kept as an int; edit date stamped.
        assert list(result["Player"]) == ["A"]
        assert result["Rating"].iloc[0] == 2800
        assert result["Rating"].dtype.kind in "iu"
        assert result["date"].iloc[0] == "2024-06-01"


# ---------------------------------------------------------------------------
# fetch()
# ---------------------------------------------------------------------------


class TestFetch:
    """fetch() orchestrates per-page downloads and returns a dict keyed by id_root."""

    def test_returns_dict_keyed_by_id_root(self, wikipedia_source):
        fake_df = pd.DataFrame({"Player": ["A"], "Rating": [2800], "date": ["2024-06-01"]})

        def fake_download_tables(page, session):
            return fake_df.copy()

        with patch.object(WikipediaSource, "_make_session", return_value=object()), patch.object(
            WikipediaSource, "_download_tables", side_effect=fake_download_tables
        ):
            result = wikipedia_source.fetch()

        from sources.wikipedia import _PAGES

        assert set(result.keys()) == {p["id_root"] for p in _PAGES}
        for df in result.values():
            assert not df.empty

    def test_raises_when_page_returns_no_rows(self, wikipedia_source):
        def fake_download_tables(page, session):
            if page["id_root"] == "List_of_infectious_diseases":
                return pd.DataFrame({"Common name": ["X"], "Vaccine(s)": ["No"], "date": ["x"]})
            return None

        with patch.object(WikipediaSource, "_make_session", return_value=object()), patch.object(
            WikipediaSource, "_download_tables", side_effect=fake_download_tables
        ):
            with pytest.raises(ValueError, match="No Wikipedia data was downloaded"):
                wikipedia_source.fetch()

    def test_raises_when_page_returns_empty_dataframe(self, wikipedia_source):
        def fake_download_tables(page, session):
            if page["id_root"] == "List_of_infectious_diseases":
                return pd.DataFrame()
            return pd.DataFrame({"Common name": ["X"], "Vaccine(s)": ["No"], "date": ["x"]})

        with patch.object(WikipediaSource, "_make_session", return_value=object()), patch.object(
            WikipediaSource, "_download_tables", side_effect=fake_download_tables
        ):
            with pytest.raises(ValueError, match="No Wikipedia data was downloaded"):
                wikipedia_source.fetch()


# ---------------------------------------------------------------------------
# resolve() — custom row-by-row resolution
# ---------------------------------------------------------------------------


class TestResolve:
    """End-to-end tests for the custom resolution path (resolve -> _resolve)."""

    def test_resolves_single_question_to_binary(self, wikipedia_source, freeze_today):
        freeze_today(date(2025, 6, 2))  # yesterday = 2025-06-01
        wid = wikipedia_source._id_hash(
            id_root="FIDE_rankings_ranking", id_field_value="Test Player"
        )
        df = make_forecast_df(
            [
                {
                    "id": wid,
                    "source": "wikipedia",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-06-01",
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": wid, "date": "2025-01-01", "value": 5},
                {"id": wid, "date": "2025-06-01", "value": 3},
            ]
        )
        resolved, warnings = wikipedia_source.resolve(
            df, pd.DataFrame(), dfr, forecast_due_date=date(2025, 1, 1)
        )
        row = resolved.iloc[0]
        # SAME_OR_LESS: rank improved from 5 to 3 (3 <= 5) -> resolves True (1.0).
        assert row["resolved_to"] == 1.0
        assert row["resolved"]
        assert warnings == []

    def test_nullified_question_resolves_to_nan(self, wikipedia_source, freeze_today):
        freeze_today(date(2025, 6, 2))
        # Monkeypox -> Mpox id, nullified from 2022-08-21.
        nid = "f9323386a651ce67fc0da31285bee22a4ec53b8a2ea5220431ecb4560fb44c77"
        df = make_forecast_df(
            [
                {
                    "id": nid,
                    "source": "wikipedia",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-06-01",
                }
            ]
        )
        dfr = make_resolution_df([{"id": "unused", "date": "2025-01-01", "value": 1}])
        resolved, _ = wikipedia_source.resolve(
            df, pd.DataFrame(), dfr, forecast_due_date=date(2025, 1, 1)
        )
        row = resolved.iloc[0]
        assert pd.isna(row["resolved_to"])
        assert row["resolved"]

    def test_future_resolution_date_not_resolved(self, wikipedia_source, freeze_today):
        freeze_today(date(2025, 6, 2))
        wid = wikipedia_source._id_hash(
            id_root="FIDE_rankings_ranking", id_field_value="Future Player"
        )
        df = make_forecast_df(
            [
                {
                    "id": wid,
                    "source": "wikipedia",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-12-31",  # after yesterday -> not yet resolvable
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": wid, "date": "2025-01-01", "value": 5},
                {"id": wid, "date": "2025-06-01", "value": 3},
            ]
        )
        resolved, _ = wikipedia_source.resolve(
            df, pd.DataFrame(), dfr, forecast_due_date=date(2025, 1, 1)
        )
        row = resolved.iloc[0]
        assert pd.isna(row["resolved_to"])
        assert not row["resolved"]

    def test_resolves_combo_question(self, wikipedia_source, freeze_today):
        freeze_today(date(2025, 6, 2))
        w1 = wikipedia_source._id_hash(id_root="FIDE_rankings_ranking", id_field_value="P1")
        w2 = wikipedia_source._id_hash(id_root="FIDE_rankings_ranking", id_field_value="P2")
        df = make_forecast_df(
            [
                {
                    "id": (w1, w2),
                    "source": "wikipedia",
                    "direction": (1, 1),
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-06-01",
                }
            ]
        )
        dfr = make_resolution_df(
            [
                {"id": w1, "date": "2025-01-01", "value": 5},
                {"id": w1, "date": "2025-06-01", "value": 3},
                {"id": w2, "date": "2025-01-01", "value": 8},
                {"id": w2, "date": "2025-06-01", "value": 4},
            ]
        )
        resolved, _ = wikipedia_source.resolve(
            df, pd.DataFrame(), dfr, forecast_due_date=date(2025, 1, 1)
        )
        # Both sub-questions resolve True; direction (1, 1) -> 1.0 * 1.0 = 1.0.
        assert resolved.iloc[0]["resolved_to"] == 1.0
