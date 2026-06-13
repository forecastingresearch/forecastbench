"""Tests for AcledSource: aggregation, resolution, hash mapping, fetch, update."""

from datetime import date, timedelta
from unittest.mock import patch

import backoff._sync
import pandas as pd
import pytest
import requests

from _schemas import AcledFetchFrame
from helpers import constants
from sources.acled import FETCH_COLUMN_DTYPE, FETCH_COLUMNS, AcledSource
from tests.conftest import (
    make_acled_api_auth_response,
    make_acled_api_data_response,
    make_acled_event,
    make_acled_fetch_df,
    make_acled_resolution_df,
    make_question_df,
)

# ---------------------------------------------------------------------------
# Shared test data factory
# ---------------------------------------------------------------------------


def _make_acled_dfr():
    """Build a small ACLED resolution DataFrame for testing aggregation functions.

    Creates 60 days of data (2024-11-01 to 2024-12-30) for two countries.
    """
    rows = []
    base_date = date(2024, 11, 1)
    for day_offset in range(60):
        d = base_date + timedelta(days=day_offset)
        rows.append(
            {
                "country": "CountryA",
                "event_date": d,
                "Battles": 2,
                "Riots": 1,
            }
        )
        rows.append(
            {
                "country": "CountryB",
                "event_date": d,
                "Battles": 5,
                "Riots": 3,
            }
        )
    return make_acled_resolution_df(rows)


# ---------------------------------------------------------------------------
# _sum_over_past_30_days
# ---------------------------------------------------------------------------


class TestSumOverPast30Days:
    """Test 30-day sum aggregation."""

    def test_sums_correct_window(self):
        dfr = _make_acled_dfr()
        ref_date = date(2024, 12, 15)
        # 30 days before Dec 15 = Nov 15 to Dec 14 = 30 days
        # CountryA has Battles=2 per day → 30 * 2 = 60
        result = AcledSource._sum_over_past_30_days(dfr, "CountryA", "Battles", ref_date)
        assert result == 60

    def test_different_country(self):
        dfr = _make_acled_dfr()
        ref_date = date(2024, 12, 15)
        # CountryB has Battles=5 per day → 30 * 5 = 150
        result = AcledSource._sum_over_past_30_days(dfr, "CountryB", "Battles", ref_date)
        assert result == 150

    def test_different_event_type(self):
        dfr = _make_acled_dfr()
        ref_date = date(2024, 12, 15)
        # CountryA Riots=1 per day → 30
        result = AcledSource._sum_over_past_30_days(dfr, "CountryA", "Riots", ref_date)
        assert result == 30

    def test_empty_country_returns_zero(self):
        dfr = _make_acled_dfr()
        result = AcledSource._sum_over_past_30_days(
            dfr, "NonExistent", "Battles", date(2024, 12, 15)
        )
        assert result == 0

    def test_no_events_in_window_returns_zero(self):
        dfr = _make_acled_dfr()
        # Data starts Nov 1, so a ref_date of Oct 1 has no data in its 30-day window
        result = AcledSource._sum_over_past_30_days(dfr, "CountryA", "Battles", date(2024, 10, 1))
        assert result == 0


# ---------------------------------------------------------------------------
# _thirty_day_avg_over_past_360_days
# ---------------------------------------------------------------------------


class TestThirtyDayAvgOverPast360Days:
    """Test 360-day average (total/12) aggregation."""

    def test_with_60_days_of_data(self):
        dfr = _make_acled_dfr()
        ref_date = date(2024, 12, 31)
        # CountryA Battles: 60 days * 2 = 120 total in 360 window (only 60 days have data)
        # Average = 120 / 12 = 10
        result = AcledSource._thirty_day_avg_over_past_360_days(
            dfr, "CountryA", "Battles", ref_date
        )
        assert result == 10

    def test_empty_country_returns_zero(self):
        dfr = _make_acled_dfr()
        result = AcledSource._thirty_day_avg_over_past_360_days(
            dfr, "NonExistent", "Battles", date(2024, 12, 15)
        )
        assert result == 0


# ---------------------------------------------------------------------------
# _thirty_day_avg_over_past_360_days_plus_1
# ---------------------------------------------------------------------------


class TestThirtyDayAvgPlus1:
    """Test 1 + 30-day average."""

    def test_adds_one(self):
        dfr = _make_acled_dfr()
        ref_date = date(2024, 12, 31)
        avg = AcledSource._thirty_day_avg_over_past_360_days(dfr, "CountryA", "Battles", ref_date)
        result = AcledSource._thirty_day_avg_over_past_360_days_plus_1(
            dfr, "CountryA", "Battles", ref_date
        )
        assert result == 1 + avg


# ---------------------------------------------------------------------------
# _get_base_comparison_value
# ---------------------------------------------------------------------------


class TestGetBaseComparisonValue:
    """Test dispatch on key string."""

    def test_key_last30_days(self):
        dfr = _make_acled_dfr()
        result = AcledSource._get_base_comparison_value(
            key="last30Days.gt.30DayAvgOverPast360Days",
            dfr=dfr,
            country="CountryA",
            col="Battles",
            ref_date=date(2024, 12, 31),
        )
        expected = AcledSource._thirty_day_avg_over_past_360_days(
            dfr, "CountryA", "Battles", date(2024, 12, 31)
        )
        assert result == expected

    def test_key_last30_days_times_10(self):
        dfr = _make_acled_dfr()
        result = AcledSource._get_base_comparison_value(
            key="last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1",
            dfr=dfr,
            country="CountryA",
            col="Battles",
            ref_date=date(2024, 12, 31),
        )
        expected = 10 * AcledSource._thirty_day_avg_over_past_360_days_plus_1(
            dfr, "CountryA", "Battles", date(2024, 12, 31)
        )
        assert result == expected

    def test_invalid_key_raises(self):
        dfr = _make_acled_dfr()
        with pytest.raises(ValueError, match="Invalid key"):
            AcledSource._get_base_comparison_value(
                key="invalid_key",
                dfr=dfr,
                country="CountryA",
                col="Battles",
                ref_date=date(2024, 12, 31),
            )


# ---------------------------------------------------------------------------
# _acled_resolve
# ---------------------------------------------------------------------------


class TestAcledResolve:
    """Test the core comparison: int(30_day_sum > baseline)."""

    def test_lhs_greater_returns_1(self):
        dfr = _make_acled_dfr()
        # ref for lhs: Dec 15 → sum = 30 * 2 = 60
        # ref for rhs: Nov 5 → avg over 360 days from Nov 5 = 5 days * 2 / 12 = 0.83
        # 60 > 0.83 → 1
        result = AcledSource._acled_resolve(
            key="last30Days.gt.30DayAvgOverPast360Days",
            dfr=dfr,
            country="CountryA",
            event_type="Battles",
            forecast_due_date=date(2024, 11, 5),
            resolution_date=date(2024, 12, 15),
        )
        assert result == 1

    def test_lhs_not_greater_returns_0(self):
        # Create data where the baseline is very high but 30-day sum is 0
        rows = []
        for day_offset in range(360):
            d = date(2024, 1, 1) + timedelta(days=day_offset)
            rows.append(
                {
                    "country": "CountryX",
                    "event_date": d,
                    "Battles": 100,
                }
            )
        dfr = make_acled_resolution_df(rows)
        # Zero out the last 30 days
        mask = dfr["event_date"] >= pd.Timestamp(date(2024, 12, 1))
        dfr.loc[mask, "Battles"] = 0

        # resolution_date = Dec 31 → sum of last 30 days = 0
        # forecast_due_date = Jan 1 → baseline avg over 360 days is high
        result = AcledSource._acled_resolve(
            key="last30Days.gt.30DayAvgOverPast360Days",
            dfr=dfr,
            country="CountryX",
            event_type="Battles",
            forecast_due_date=date(2024, 1, 1),
            resolution_date=date(2024, 12, 31),
        )
        assert result == 0


# ---------------------------------------------------------------------------
# Hash mapping
# ---------------------------------------------------------------------------


class TestAcledHashMapping:
    """Test hash mapping load, dump, and unhash."""

    def test_populate_hash_mapping(self):
        source = AcledSource()
        source.populate_hash_mapping(
            '{"hash1": {"key": "last30Days.gt.30DayAvgOverPast360Days", '
            '"country": "Somalia", "event_type": "Battles"}}'
        )
        assert "hash1" in source.hash_mapping
        assert source.hash_mapping["hash1"]["country"] == "Somalia"

    def test_load_empty_string(self):
        source = AcledSource()
        source.populate_hash_mapping("")
        assert source.hash_mapping == {}

    def test_dump_hash_mapping(self):
        source = AcledSource()
        source.hash_mapping = {"h1": {"key": "test"}}
        result = source.dump_hash_mapping()
        assert '"h1"' in result
        assert '"test"' in result

    def test_id_unhash_found(self):
        source = AcledSource()
        source.hash_mapping = {"h1": {"key": "k1", "country": "X", "event_type": "Y"}}
        assert source._id_unhash("h1") == {"key": "k1", "country": "X", "event_type": "Y"}

    def test_id_unhash_not_found(self):
        source = AcledSource()
        source.hash_mapping = {}
        assert source._id_unhash("missing") is None


# ---------------------------------------------------------------------------
# _id_hash
# ---------------------------------------------------------------------------


class TestIdHash:
    """Test hash encoding of question IDs."""

    def test_deterministic(self):
        d = {"key": "k1", "event_type": "Battles", "country": "Somalia"}
        assert AcledSource()._id_hash(d) == AcledSource()._id_hash(d)

    def test_stored_in_hash_mapping(self):
        source = AcledSource()
        d = {"key": "k1", "event_type": "Battles", "country": "Somalia"}
        aid = source._id_hash(d)
        assert source.hash_mapping[aid] == d

    def test_different_inputs_different_hashes(self):
        source = AcledSource()
        h1 = source._id_hash({"key": "k1", "event_type": "Battles", "country": "Somalia"})
        h2 = source._id_hash({"key": "k1", "event_type": "Riots", "country": "Somalia"})
        assert h1 != h2


# ---------------------------------------------------------------------------
# _fill_template
# ---------------------------------------------------------------------------


class TestFillTemplate:
    """Test question template filling."""

    def test_fills_event_type_and_country(self):
        result = AcledSource._fill_template(
            template="More {event_type} in {country}?",
            fields=("event_type", "country"),
            values={"event_type": "'Battles'", "country": "Somalia"},
        )
        assert result == "More 'Battles' in Somalia?"

    def test_preserves_date_placeholders(self):
        result = AcledSource._fill_template(
            template="{event_type} before {resolution_date} vs {forecast_due_date} in {country}?",
            fields=("event_type", "country"),
            values={"event_type": "'Battles'", "country": "Somalia"},
        )
        assert "{resolution_date}" in result
        assert "{forecast_due_date}" in result


# ---------------------------------------------------------------------------
# _get_freeze_value
# ---------------------------------------------------------------------------


class TestGetFreezeValue:
    """Test freeze value dispatch on key string."""

    def test_key_avg(self):
        dfr = _make_acled_dfr()
        result = AcledSource._get_freeze_value(
            key="last30Days.gt.30DayAvgOverPast360Days",
            dfr=dfr,
            country="CountryA",
            event_type="Battles",
            today=date(2024, 12, 31),
        )
        expected = AcledSource._thirty_day_avg_over_past_360_days(
            dfr, "CountryA", "Battles", date(2024, 12, 31)
        )
        assert result == expected

    def test_key_plus_1(self):
        dfr = _make_acled_dfr()
        result = AcledSource._get_freeze_value(
            key="last30DaysTimes10.gt.30DayAvgOverPast360DaysPlus1",
            dfr=dfr,
            country="CountryA",
            event_type="Battles",
            today=date(2024, 12, 31),
        )
        expected = AcledSource._thirty_day_avg_over_past_360_days_plus_1(
            dfr, "CountryA", "Battles", date(2024, 12, 31)
        )
        assert result == expected

    def test_invalid_key_raises(self):
        with pytest.raises(Exception, match="Invalid key"):
            AcledSource._get_freeze_value(
                key="invalid_key",
                dfr=_make_acled_dfr(),
                country="CountryA",
                event_type="Battles",
                today=date(2024, 12, 31),
            )


# ---------------------------------------------------------------------------
# _prepare_resolution_data
# ---------------------------------------------------------------------------


class TestPrepareResolutionData:
    """Test the fetch-data-to-resolution-frame transformation."""

    def test_basic_transformation(self):
        dff = make_acled_fetch_df(
            [
                {"event_id_cnty": "A1", "country": "X", "event_type": "Battles", "fatalities": 2},
                {"event_id_cnty": "A2", "country": "Y", "event_type": "Riots", "fatalities": 0},
            ]
        )
        dfr, countries, event_types = AcledSource._prepare_resolution_data(dff)

        assert set(countries) == {"X", "Y"}
        assert event_types == ["Battles", "Riots", "fatalities"]
        assert {"country", "event_date", "Battles", "Riots", "fatalities"} <= set(dfr.columns)

    def test_year_prefix_bug_fix(self):
        dff = make_acled_fetch_df(
            [
                {"event_id_cnty": "A1", "event_date": "0025-01-03"},
                {"event_id_cnty": "A2", "event_date": "0024-12-31"},
                {"event_id_cnty": "A3", "event_date": "2025-02-01"},
            ]
        )
        dfr, _, _ = AcledSource._prepare_resolution_data(dff)

        assert pd.Timestamp("2025-01-03") in set(dfr["event_date"])
        assert pd.Timestamp("2024-12-31") in set(dfr["event_date"])
        assert dfr["event_date"].min() >= pd.Timestamp("2024-01-01")

    def test_groupby_sums_events_per_country_and_date(self):
        dff = make_acled_fetch_df(
            [
                {
                    "event_id_cnty": "A1",
                    "country": "X",
                    "event_date": "2025-06-15",
                    "fatalities": 1,
                },
                {
                    "event_id_cnty": "A2",
                    "country": "X",
                    "event_date": "2025-06-15",
                    "fatalities": 2,
                },
            ]
        )
        dfr, _, _ = AcledSource._prepare_resolution_data(dff)

        assert len(dfr) == 1
        row = dfr.iloc[0]
        assert row["Battles"] == 2
        assert row["fatalities"] == 3


# ---------------------------------------------------------------------------
# _create_question
# ---------------------------------------------------------------------------


class TestCreateQuestion:
    """Test single question dict creation."""

    def test_basic_question_structure(self):
        source = AcledSource()
        question = source._create_question(
            question_key="last30Days.gt.30DayAvgOverPast360Days",
            country="CountryA",
            event_type="Battles",
            dfr=_make_acled_dfr(),
            today=date(2024, 12, 31),
        )

        assert set(question.keys()) == set(constants.QUESTION_FILE_COLUMNS)
        assert question["id"] in source.hash_mapping
        assert question["resolved"] is False
        assert question["url"] == "https://acleddata.com/"
        assert question["forecast_horizons"] == constants.FORECAST_HORIZONS_IN_DAYS

    def test_event_type_quoted_in_question_text(self):
        source = AcledSource()
        question = source._create_question(
            question_key="last30Days.gt.30DayAvgOverPast360Days",
            country="CountryA",
            event_type="Battles",
            dfr=_make_acled_dfr(),
            today=date(2024, 12, 31),
        )
        assert "'Battles'" in question["question"]

    def test_fatalities_not_quoted_in_question_text(self):
        source = AcledSource()
        dfr = _make_acled_dfr()
        dfr["fatalities"] = 1
        question = source._create_question(
            question_key="last30Days.gt.30DayAvgOverPast360Days",
            country="CountryA",
            event_type="fatalities",
            dfr=dfr,
            today=date(2024, 12, 31),
        )
        assert "more fatalities" in question["question"]
        assert "'fatalities'" not in question["question"]

    def test_freeze_value_is_string(self):
        source = AcledSource()
        question = source._create_question(
            question_key="last30Days.gt.30DayAvgOverPast360Days",
            country="CountryA",
            event_type="Battles",
            dfr=_make_acled_dfr(),
            today=date(2024, 12, 31),
        )
        assert isinstance(question["freeze_datetime_value"], str)


# ---------------------------------------------------------------------------
# _generate_questions
# ---------------------------------------------------------------------------


class TestGenerateQuestions:
    """Test question generation and upsert into dfq."""

    def test_generates_two_questions_per_country_event_type(self):
        source = AcledSource()
        dfq = pd.DataFrame()
        df = source._generate_questions(
            dfq=dfq,
            dfr=_make_acled_dfr(),
            countries=["CountryA", "CountryB"],
            event_types=["Battles", "Riots"],
            today=date(2024, 12, 31),
        )
        assert len(df) == 2 * 2 * 2

    def test_upserts_existing_question(self):
        source = AcledSource()
        aid = source._id_hash(
            {
                "key": "last30Days.gt.30DayAvgOverPast360Days",
                "event_type": "Battles",
                "country": "CountryA",
            }
        )
        dfq = make_question_df([{"id": aid, "question": "stale text"}])

        df = source._generate_questions(
            dfq=dfq,
            dfr=_make_acled_dfr(),
            countries=["CountryA"],
            event_types=["Battles"],
            today=date(2024, 12, 31),
        )

        assert len(df) == 2
        assert df.loc[df["id"] == aid, "question"].iloc[0] != "stale text"
        assert sorted(df["id"]) == list(df["id"])


# ---------------------------------------------------------------------------
# _get_access_token (mock requests.post)
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal `requests.Response` test double."""

    def __init__(self, payload, *, error=None):
        self._payload = payload
        self._error = error
        self.status_code = 524 if error else 200
        self.headers = {}
        self.text = ""

    @property
    def ok(self):
        return self._error is None

    def raise_for_status(self):
        if self._error is not None:
            raise self._error

    def json(self):
        return self._payload


class TestGetAccessToken:
    """Test OAuth authentication."""

    @patch("sources.acled.requests.post")
    def test_successful_auth(self, mock_post, acled_source_with_creds):
        mock_post.return_value = _FakeResponse(make_acled_api_auth_response())

        token = acled_source_with_creds._get_access_token()

        assert token == "test-token-abc123"

    @patch("sources.acled.requests.post")
    def test_requests_authenticated_scope(self, mock_post, acled_source_with_creds):
        """Regression: the oauth request must carry scope=authenticated (prod fix ebcadc8)."""
        mock_post.return_value = _FakeResponse(make_acled_api_auth_response())

        acled_source_with_creds._get_access_token()

        params = mock_post.call_args.kwargs["data"]
        assert params["scope"] == "authenticated"
        assert params["grant_type"] == "password"
        assert params["client_id"] == "acled"

    @patch("sources.acled.requests.post")
    def test_missing_token_in_response(self, mock_post, acled_source_with_creds):
        mock_post.return_value = _FakeResponse({"token_type": "Bearer"})

        with pytest.raises(ValueError, match="Access token not found"):
            acled_source_with_creds._get_access_token()


# ---------------------------------------------------------------------------
# _get_events (mock requests.get)
# ---------------------------------------------------------------------------


class TestGetEvents:
    """Test event pagination, dedup, and retry semantics."""

    def test_page_scoped_retry_does_not_restart_pagination(
        self, monkeypatch, acled_source_with_creds
    ):
        """Regression: a failing page is retried alone, not from page 1 (prod fix 2da4643)."""
        monkeypatch.setattr(backoff._sync.time, "sleep", lambda _: None)
        requested_pages = []
        page_attempts = {}

        def fake_get(_endpoint, headers=None, params=None, timeout=None):
            del headers
            assert timeout == 100
            page = params["page"]
            requested_pages.append(page)
            page_attempts[page] = page_attempts.get(page, 0) + 1

            if page == 1:
                return _FakeResponse(
                    make_acled_api_data_response([make_acled_event(event_id_cnty="evt-1")])
                )
            if page == 2 and page_attempts[page] == 1:
                error = requests.exceptions.HTTPError("524 Server Error")
                return _FakeResponse({}, error=error)
            if page == 2:
                return _FakeResponse(
                    make_acled_api_data_response([make_acled_event(event_id_cnty="evt-2")])
                )
            if page == 3:
                return _FakeResponse(make_acled_api_data_response([]))
            raise AssertionError(f"Unexpected page request: {page}")

        monkeypatch.setattr("sources.acled.requests.get", fake_get)

        df = acled_source_with_creds._get_events(access_token="token")

        assert requested_pages == [1, 2, 2, 3]
        assert list(df["event_id_cnty"]) == ["evt-1", "evt-2"]

    def test_empty_data_page_stops_pagination_when_count_is_null(
        self, monkeypatch, acled_source_with_creds
    ):
        """Regression: pagination stops on an empty data list even when count is not 0
        (prod fix b833376)."""
        requested_pages = []

        def fake_get(_endpoint, headers=None, params=None, timeout=None):
            del headers
            assert timeout == 100
            page = params["page"]
            requested_pages.append(page)

            if page == 1:
                return _FakeResponse(
                    make_acled_api_data_response(
                        [make_acled_event(event_id_cnty="evt-1")], count=None
                    )
                )
            if page == 2:
                return _FakeResponse(make_acled_api_data_response([], count=None))
            raise AssertionError(f"Unexpected page request: {page}")

        monkeypatch.setattr("sources.acled.requests.get", fake_get)

        df = acled_source_with_creds._get_events(access_token="token")

        assert requested_pages == [1, 2]
        assert list(df["event_id_cnty"]) == ["evt-1"]

    def test_deduplicates_by_event_id_across_pages(self, monkeypatch, acled_source_with_creds):
        responses = iter(
            [
                _FakeResponse(
                    make_acled_api_data_response(
                        [
                            make_acled_event(event_id_cnty="DUP1"),
                            make_acled_event(event_id_cnty="UNIQUE"),
                        ]
                    )
                ),
                _FakeResponse(
                    make_acled_api_data_response([make_acled_event(event_id_cnty="DUP1")])
                ),
                _FakeResponse(make_acled_api_data_response([])),
            ]
        )
        monkeypatch.setattr("sources.acled.requests.get", lambda *args, **kwargs: next(responses))

        df = acled_source_with_creds._get_events(access_token="token")

        assert sorted(df["event_id_cnty"]) == ["DUP1", "UNIQUE"]

    def test_sorts_by_event_id(self, monkeypatch, acled_source_with_creds):
        responses = iter(
            [
                _FakeResponse(
                    make_acled_api_data_response(
                        [
                            make_acled_event(event_id_cnty="ZZZ"),
                            make_acled_event(event_id_cnty="AAA"),
                        ]
                    )
                ),
                _FakeResponse(make_acled_api_data_response([])),
            ]
        )
        monkeypatch.setattr("sources.acled.requests.get", lambda *args, **kwargs: next(responses))

        df = acled_source_with_creds._get_events(access_token="token")

        assert list(df["event_id_cnty"]) == ["AAA", "ZZZ"]

    def test_empty_first_page_returns_empty_frame(self, monkeypatch, acled_source_with_creds):
        """Regression: empty data on the first page returns an empty frame instead of raising
        ValueError from pd.concat([]), so the job's `if dff.empty` guard is reachable."""
        monkeypatch.setattr(
            "sources.acled.requests.get",
            lambda *args, **kwargs: _FakeResponse(make_acled_api_data_response([], count=0)),
        )

        df = acled_source_with_creds._get_events(access_token="token")

        assert df.empty
        assert list(df.columns) == FETCH_COLUMNS


# ---------------------------------------------------------------------------
# fetch()
# ---------------------------------------------------------------------------


class TestFetch:
    """Test the public fetch entry point."""

    @patch.object(AcledSource, "_get_events")
    @patch.object(AcledSource, "_get_access_token")
    def test_basic_fetch(self, mock_token, mock_events, acled_source_with_creds):
        mock_token.return_value = "fake-token"
        mock_events.return_value = make_acled_fetch_df(
            [
                {"event_id_cnty": "A1", "event_date": "2025-06-15"},
                {"event_id_cnty": "A2", "event_date": "2025-06-16"},
            ]
        )

        dff = acled_source_with_creds.fetch()

        assert len(dff) == 2
        AcledFetchFrame.validate(dff)

    def test_credentials_required(self):
        with pytest.raises(RuntimeError, match="api_email"):
            AcledSource().fetch()


# ---------------------------------------------------------------------------
# update()
# ---------------------------------------------------------------------------


class TestUpdate:
    """Test the public update entry point."""

    def test_basic_update(self, freeze_today):
        freeze_today(date(2025, 6, 20))
        source = AcledSource()
        dfq = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)
        dff = make_acled_fetch_df(
            [
                {
                    "event_id_cnty": "A1",
                    "country": "Somalia",
                    "event_type": "Battles",
                    "event_date": "2025-06-15",
                    "fatalities": 3,
                },
            ]
        )

        result = source.update(dfq, dff)

        # 1 country x 2 event_types (Battles + fatalities) x 2 question keys
        assert len(result.dfq) == 4
        assert list(result.dfq.columns) == constants.QUESTION_FILE_COLUMNS
        assert result.resolution_files is None

    def test_hash_mapping_returned_for_all_questions(self, freeze_today):
        freeze_today(date(2025, 6, 20))
        source = AcledSource()
        dfq = pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)
        dff = make_acled_fetch_df(
            [
                {
                    "event_id_cnty": "A1",
                    "country": "X",
                    "event_type": "Battles",
                    "event_date": "2025-06-15",
                    "fatalities": 0,
                },
            ]
        )

        result = source.update(dfq, dff)

        assert result.hash_mapping is not None
        assert set(result.dfq["id"]) == set(result.hash_mapping.keys())
        for entry in result.hash_mapping.values():
            assert {"key", "country", "event_type"} == set(entry.keys())

    def test_updates_existing_question_fields(self, freeze_today):
        freeze_today(date(2025, 6, 20))
        source = AcledSource()
        aid = source._id_hash(
            {
                "key": "last30Days.gt.30DayAvgOverPast360Days",
                "event_type": "Battles",
                "country": "X",
            }
        )
        dfq = make_question_df(
            [{"id": aid, "question": "stale text", "freeze_datetime_value": "999"}]
        )
        dff = make_acled_fetch_df(
            [
                {
                    "event_id_cnty": "A1",
                    "country": "X",
                    "event_type": "Battles",
                    "event_date": "2025-06-15",
                    "fatalities": 0,
                },
            ]
        )

        result = source.update(dfq, dff)

        row = result.dfq[result.dfq["id"] == aid].iloc[0]
        assert row["question"] != "stale text"
        assert row["freeze_datetime_value"] != "999"
        assert len(result.dfq) == 4


# ---------------------------------------------------------------------------
# FETCH_COLUMN_DTYPE <-> AcledFetchFrame consistency
# ---------------------------------------------------------------------------


class TestFetchColumnDtypeMatchesSchema:
    """FETCH_COLUMN_DTYPE is the operational twin of AcledFetchFrame.

    The dict drives the API `fields` param and the fetch-file read dtypes; the
    pandera model validates the resulting frame. These pin the two in sync.
    """

    def test_same_columns_in_same_order(self):
        assert list(AcledFetchFrame.to_schema().columns.keys()) == FETCH_COLUMNS

    def test_dict_typed_data_satisfies_schema_unchanged(self):
        """Data typed per FETCH_COLUMN_DTYPE passes validation with no coercion drift."""
        df = make_acled_fetch_df([make_acled_event()]).astype(FETCH_COLUMN_DTYPE)
        validated = AcledFetchFrame.validate(df)
        pd.testing.assert_frame_equal(validated, df)
