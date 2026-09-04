"""Tests for DbnomicsSource: fetch and update logic."""

from collections import Counter
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from helpers import constants
from helpers import dbnomics as dbnomics_helper
from sources.dbnomics import DBNOMICS_QUESTIONS, DbnomicsSource

from .conftest import (
    make_dbnomics_api_response,
    make_dbnomics_fetch_df,
    make_question_df,
)

# A small stand-in for DBNOMICS_QUESTIONS so update tests don't need every series present.
_TEST_QUESTIONS = [
    {
        "id": "meteofrance/TEMPERATURE/celsius.07005.D",
        "question_text": (
            "What is the probability that the daily average temperature at the French weather "
            "station at Abbeville will be higher on {resolution_date} than on {forecast_due_date}?"
        ),
        "freeze_datetime_value_explanation": (
            "The daily average temperature at the French weather station at Abbeville."
        ),
        "fill_missing_dates": False,
    }
]
_RAW_ID = "meteofrance/TEMPERATURE/celsius.07005.D"
_SAFE_ID = "meteofrance_TEMPERATURE_celsius.07005.D"


def _empty_dfq():
    """Return an empty question bank with the canonical columns (so dfq['id'] exists)."""
    return pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)


# ---------------------------------------------------------------------------
# Backwards-compat shim
# ---------------------------------------------------------------------------


class TestHelperShim:
    """The helpers/dbnomics.py shim still exposes identity for question_curation."""

    def test_exposes_intro_and_criteria(self):
        assert isinstance(dbnomics_helper.SOURCE_INTRO, str) and dbnomics_helper.SOURCE_INTRO
        assert (
            isinstance(dbnomics_helper.RESOLUTION_CRITERIA, str)
            and dbnomics_helper.RESOLUTION_CRITERIA
        )


# ---------------------------------------------------------------------------
# Curated series configuration
# ---------------------------------------------------------------------------


class TestSeriesConfiguration:
    """The production configuration includes every curated ECB series and MétéoFrance."""

    def test_all_curated_series_are_present_once(self):
        ecb_questions = [row for row in DBNOMICS_QUESTIONS if row["id"].startswith("ECB/")]

        assert len(ecb_questions) == 458
        assert len({row["id"] for row in ecb_questions}) == 458
        assert Counter(row["id"].split("/")[1] for row in ecb_questions) == {
            "CISS": 32,
            "EST": 13,
            "EXR": 200,
            "FM": 6,
            "ILM": 7,
            "YC": 200,
        }
        assert all(row["fill_missing_dates"] for row in ecb_questions)
        assert len(DBNOMICS_QUESTIONS) == 458 + 53

    def test_ecb_question_states_the_latest_available_value_policy(self):
        row = next(row for row in DBNOMICS_QUESTIONS if row["id"] == "ECB/EXR/D.USD.EUR.SP00.A")

        assert row["question_text"] == (
            "What is the probability that the value of the latest observation dated on or before "
            "{resolution_date} for the European Central Bank time series “US dollar per euro "
            "spot exchange rate” will exceed the value of its latest observation dated on or "
            "before {forecast_due_date}?"
        )
        assert row["freeze_datetime_value_explanation"] == (
            "The latest value published on or before the question-set freeze date for “US dollar "
            "per euro spot exchange rate” in the European Central Bank’s “Exchange Rates” dataset."
        )


# ---------------------------------------------------------------------------
# _call_endpoint
# ---------------------------------------------------------------------------


class TestCallEndpoint:
    """Single-series API call: id safe-ification + date-window filtering."""

    def test_safe_ids_and_window_filter(self, dbnomics_source):
        today = date(2026, 1, 15)
        resp = Mock()
        resp.ok = True
        resp.json.return_value = make_dbnomics_api_response(
            [
                ("2025-01-01", 5.0),  # in window -> keep
                ("2026-01-14", 7.0),  # yesterday -> keep
                ("2026-01-15", 9.0),  # today -> dropped (period < today)
                ("2026-01-20", 9.0),  # future -> dropped
            ]
        )
        with patch("sources.dbnomics.requests.get", return_value=resp):
            df = dbnomics_source._call_endpoint(id=_RAW_ID, today=today)

        assert df["id"].unique().tolist() == [_SAFE_ID]
        assert set(df["period"]) == {date(2025, 1, 1), date(2026, 1, 14)}

    def test_omits_repeated_metadata_and_keeps_provider_names(self, dbnomics_source):
        provider_names = {
            "ECB": "European Central Bank",
            "meteofrance": "Météo-France",
        }
        for provider_code, provider_name in provider_names.items():
            resp = Mock()
            resp.ok = True
            resp.json.return_value = make_dbnomics_api_response(
                [("2026-01-14", 7.0)], provider_code=provider_code
            )

            with patch("sources.dbnomics.requests.get", return_value=resp) as mocked:
                df = dbnomics_source._call_endpoint(id=_RAW_ID, today=date(2026, 1, 15))

            assert mocked.call_args.kwargs["params"] == {
                "observations": 1,
                "metadata": 0,
            }
            assert mocked.call_args.kwargs["headers"] == {
                "Authorization": "Bearer test-dbnomics-api-key"
            }
            assert mocked.call_args.kwargs["timeout"] == (10, 60)
            assert df["provider_name"].unique().tolist() == [provider_name]

    def test_returns_none_when_window_empty(self, dbnomics_source):
        today = date(2026, 1, 15)
        resp = Mock()
        resp.ok = True
        resp.json.return_value = make_dbnomics_api_response([("2026-01-20", 9.0)])
        with patch("sources.dbnomics.requests.get", return_value=resp):
            df = dbnomics_source._call_endpoint(id=_RAW_ID, today=today)

        assert df is None


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


class TestFetch:
    """fetch() threads a single 'today' and concatenates per-series frames."""

    @patch("sources.dbnomics.DBNOMICS_QUESTIONS", _TEST_QUESTIONS)
    def test_concatenates_and_casts_period_to_str(self, dbnomics_source, freeze_today):
        freeze_today(date(2026, 1, 15))
        fake = pd.DataFrame(
            {
                "id": _SAFE_ID,
                "period": [date(2026, 1, 13), date(2026, 1, 14)],
                "value": [5.0, 6.0],
                "provider_name": "MeteoFrance",
                "dataset_name": "Temperature",
                "series_name": "Abbeville",
            }
        )
        with patch.object(DbnomicsSource, "_call_endpoint", return_value=fake) as mocked:
            dff = dbnomics_source.fetch()

        # today computed once on the surface and threaded down.
        assert mocked.call_args.kwargs["today"] == date(2026, 1, 15)
        assert dff["period"].tolist() == ["2026-01-13", "2026-01-14"]
        assert dff["id"].tolist() == [_SAFE_ID, _SAFE_ID]

    @patch(
        "sources.dbnomics.DBNOMICS_QUESTIONS",
        [{"id": "series/one"}, {"id": "series/empty"}, {"id": "series/two"}],
    )
    def test_returns_rows_from_nonempty_series_in_source_order(self, dbnomics_source, freeze_today):
        """Skip empty endpoint results without changing source order."""
        freeze_today(date(2026, 1, 15))
        first = make_dbnomics_fetch_df(
            [{"id": "series_one", "period": date(2026, 1, 13), "value": 5.0}]
        )
        second = make_dbnomics_fetch_df(
            [{"id": "series_two", "period": date(2026, 1, 14), "value": 6.0}]
        )

        with patch.object(DbnomicsSource, "_call_endpoint", side_effect=[first, None, second]):
            dff = dbnomics_source.fetch()

        assert dff["id"].tolist() == ["series_one", "series_two"]
        assert dff["period"].tolist() == ["2026-01-13", "2026-01-14"]

    @patch(
        "sources.dbnomics.DBNOMICS_QUESTIONS",
        [{"id": "series/one"}, {"id": "series/two"}],
    )
    def test_raises_clear_error_when_all_series_are_empty(self, dbnomics_source, freeze_today):
        """Fail the fetch rather than producing an empty output file."""
        freeze_today(date(2026, 1, 15))

        with patch.object(DbnomicsSource, "_call_endpoint", return_value=None):
            with pytest.raises(
                RuntimeError, match="No DBnomics series returned usable observations"
            ):
                dbnomics_source.fetch()


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestUpdate:
    """update() builds resolution files for every series and upserts questions."""

    @patch("sources.dbnomics.DBNOMICS_QUESTIONS", _TEST_QUESTIONS)
    def test_new_question_inserted_with_resolution_file(self, dbnomics_source, freeze_today):
        freeze_today(date(2026, 1, 15))
        dff = make_dbnomics_fetch_df(
            [
                {"id": _SAFE_ID, "period": "2026-01-13", "value": 5.0},
                {"id": _SAFE_ID, "period": "2026-01-14", "value": 6.0},
            ]
        )
        result = dbnomics_source.update(_empty_dfq(), dff)

        assert _SAFE_ID in result.dfq["id"].tolist()
        row = result.dfq[result.dfq["id"] == _SAFE_ID].iloc[0]
        assert not row["resolved"]
        assert row["url"] == f"https://db.nomics.world/{_RAW_ID}"
        assert row["freeze_datetime_value"] == "6.0"

        assert _SAFE_ID in result.resolution_files
        rf = result.resolution_files[_SAFE_ID]
        assert list(rf.columns) == ["id", "date", "value"]

    @patch("sources.dbnomics.DBNOMICS_QUESTIONS", _TEST_QUESTIONS)
    def test_existing_question_updated_in_place(self, dbnomics_source, freeze_today):
        freeze_today(date(2026, 1, 15))
        dff = make_dbnomics_fetch_df(
            [
                {"id": _SAFE_ID, "period": "2026-01-13", "value": 5.0},
                {"id": _SAFE_ID, "period": "2026-01-14", "value": 6.0},
            ]
        )
        dfq = make_question_df([{"id": _SAFE_ID, "freeze_datetime_value": "stale"}])

        result = dbnomics_source.update(dfq, dff)

        # No duplicate row; freeze value refreshed to the last non-NA value. The QuestionFrame
        # contract coerces freeze_datetime_value to a string on update() output (downstream always
        # reads it back as str), so the float 6.0 surfaces as "6.0".
        assert (result.dfq["id"] == _SAFE_ID).sum() == 1
        row = result.dfq[result.dfq["id"] == _SAFE_ID].iloc[0]
        assert row["freeze_datetime_value"] == "6.0"

    @patch(
        "sources.dbnomics.DBNOMICS_QUESTIONS",
        [
            {
                **_TEST_QUESTIONS[0],
                "id": "meteofrance/TEMPERATURE/celsius.07015.D",
            },
            _TEST_QUESTIONS[0],
        ],
    )
    def test_missing_series_is_preserved_while_available_series_is_processed(
        self, dbnomics_source, freeze_today
    ):
        """Keep prior question state when one configured series has no fetched rows."""
        freeze_today(date(2026, 1, 15))
        missing_id = "meteofrance_TEMPERATURE_celsius.07015.D"
        dfq = make_question_df([{"id": missing_id, "freeze_datetime_value": "stale"}])
        dff = make_dbnomics_fetch_df([{"id": _SAFE_ID, "period": "2026-01-14", "value": 6.0}])

        result = dbnomics_source.update(dfq, dff)

        missing_row = result.dfq[result.dfq["id"] == missing_id].iloc[0]
        assert missing_row["freeze_datetime_value"] == "stale"
        assert missing_id not in result.resolution_files
        assert _SAFE_ID in result.dfq["id"].tolist()
        assert _SAFE_ID in result.resolution_files

    @patch("sources.dbnomics.DBNOMICS_QUESTIONS", _TEST_QUESTIONS)
    def test_all_na_window_skipped_from_questions_but_resolution_built(
        self, dbnomics_source, freeze_today
    ):
        freeze_today(date(2026, 1, 15))
        # 11 consecutive "NA"s: the last 10 are all NA -> series is not minted into a question.
        rows = [{"id": _SAFE_ID, "period": f"2026-01-{d:02d}", "value": "NA"} for d in range(1, 12)]
        dff = make_dbnomics_fetch_df(rows)

        result = dbnomics_source.update(_empty_dfq(), dff)

        assert _SAFE_ID not in result.dfq["id"].tolist()
        assert _SAFE_ID in result.resolution_files  # resolution file still written

    @patch("sources.dbnomics.DBNOMICS_QUESTIONS", _TEST_QUESTIONS)
    def test_resolution_file_na_to_not_available_and_values_are_strings(
        self, dbnomics_source, freeze_today
    ):
        freeze_today(date(2026, 1, 15))
        dff = make_dbnomics_fetch_df(
            [
                {"id": _SAFE_ID, "period": "2026-01-13", "value": "NA"},
                {"id": _SAFE_ID, "period": "2026-01-14", "value": 6.0},
            ]
        )

        result = dbnomics_source.update(_empty_dfq(), dff)
        values = result.resolution_files[_SAFE_ID]["value"].tolist()

        assert "N/A" in values  # "NA" rewritten
        assert "NA" not in values
        assert "6.0" in values  # numeric value preserved as a string (legacy parity)

    @patch(
        "sources.dbnomics.DBNOMICS_QUESTIONS",
        [{**_TEST_QUESTIONS[0], "fill_missing_dates": True}],
    )
    def test_values_are_carried_forward_over_omitted_and_unavailable_dates(
        self, dbnomics_source, freeze_today
    ):
        freeze_today(date(2026, 1, 13))
        dff = make_dbnomics_fetch_df(
            [
                {"id": _SAFE_ID, "period": "2026-01-09", "value": "5.0"},
                {"id": _SAFE_ID, "period": "2026-01-11", "value": "NA"},
                {"id": _SAFE_ID, "period": "2026-01-12", "value": "6.0"},
            ]
        )

        result = dbnomics_source.update(_empty_dfq(), dff)
        resolution = result.resolution_files[_SAFE_ID].set_index("date")

        assert resolution.loc["2026-01-10", "value"] == "5.0"
        assert resolution.loc["2026-01-11", "value"] == "5.0"
        assert resolution.loc["2026-01-12", "value"] == "6.0"
