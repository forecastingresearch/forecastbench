"""Tests for MetaculusSource fetch/update logic."""

from datetime import date
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from _schemas import MetaculusFetchFrame, QuestionFrame, ResolutionFrame
from sources.metaculus import MetaculusSource

from .conftest import (
    make_metaculus_fetch_df,
    make_metaculus_market,
    make_metaculus_search_result,
    make_question_df,
)

# ---------------------------------------------------------------------------
# _get_resolved_market_value (pure, no mocking)
# ---------------------------------------------------------------------------


class TestGetResolvedMarketValue:
    """Tests for MetaculusSource._get_resolved_market_value static method."""

    def _market(self, resolution):
        return {"id": 42472, "question": {"resolution": resolution}}

    def test_yes(self):
        """'yes' resolves to 1."""
        assert MetaculusSource._get_resolved_market_value(self._market("yes")) == 1

    def test_no(self):
        """'no' resolves to 0."""
        assert MetaculusSource._get_resolved_market_value(self._market("no")) == 0

    def test_ambiguous(self):
        """'ambiguous' resolves to NaN."""
        result = MetaculusSource._get_resolved_market_value(self._market("ambiguous"))
        assert np.isnan(result)

    def test_annulled(self):
        """'annulled' resolves to NaN."""
        result = MetaculusSource._get_resolved_market_value(self._market("annulled"))
        assert np.isnan(result)

    def test_case_insensitive(self):
        """Resolution string is lowered before matching."""
        assert MetaculusSource._get_resolved_market_value(self._market("YES")) == 1
        assert MetaculusSource._get_resolved_market_value(self._market("No")) == 0

    def test_invalid_raises(self):
        """Unrecognized resolution raises AssertionError."""
        with pytest.raises(AssertionError, match="Problem getting resolution"):
            MetaculusSource._get_resolved_market_value(self._market("invalid"))


# ---------------------------------------------------------------------------
# _parse_retry_after (pure, no mocking)
# ---------------------------------------------------------------------------


class TestParseRetryAfter:
    """Tests for MetaculusSource._parse_retry_after static method."""

    def test_valid_integer(self):
        """Parses integer Retry-After header."""
        resp = Mock()
        resp.headers = {"Retry-After": "30"}
        assert MetaculusSource._parse_retry_after(resp) == 30

    def test_missing_header(self):
        """Returns None when Retry-After header is absent."""
        resp = Mock()
        resp.headers = {}
        assert MetaculusSource._parse_retry_after(resp) is None

    def test_non_integer(self):
        """Returns None when Retry-After is not an integer."""
        resp = Mock()
        resp.headers = {"Retry-After": "invalid"}
        assert MetaculusSource._parse_retry_after(resp) is None


# ---------------------------------------------------------------------------
# _finalize_resolution_df (pure, no mocking)
# ---------------------------------------------------------------------------


class TestFinalizeResolutionDf:
    """Tests for MetaculusSource._finalize_resolution_df static method."""

    def test_validates_schema(self):
        """Output is a valid ResolutionFrame."""
        df = pd.DataFrame(
            {
                "id": ["42472"],
                "date": [date(2025, 6, 1)],
                "value": [0.5],
            }
        )
        result = MetaculusSource._finalize_resolution_df(df)
        ResolutionFrame.validate(result)

    def test_keeps_all_historical_data(self):
        """Unlike Infer, rows before BENCHMARK_START_DATE are NOT filtered."""
        df = pd.DataFrame(
            {
                "id": ["A", "A", "A"],
                "date": [date(2018, 1, 1), date(2020, 6, 1), date(2025, 7, 1)],
                "value": [0.1, 0.2, 0.3],
            }
        )
        result = MetaculusSource._finalize_resolution_df(df)
        assert len(result) == 3

    def test_date_coerced_to_string(self):
        """datetime.date objects become 'YYYY-MM-DD' strings."""
        df = pd.DataFrame(
            {
                "id": ["A"],
                "date": [date(2025, 6, 1)],
                "value": [0.5],
            }
        )
        result = MetaculusSource._finalize_resolution_df(df)
        assert result["date"].iloc[0] == "2025-06-01"

    def test_id_coerced_to_string(self):
        """Integer IDs are coerced to string."""
        df = pd.DataFrame(
            {
                "id": [42472],
                "date": [date(2025, 6, 1)],
                "value": [0.5],
            }
        )
        result = MetaculusSource._finalize_resolution_df(df)
        assert result["id"].iloc[0] == "42472"

    def test_only_keeps_id_date_value(self):
        """Extra columns are stripped."""
        df = pd.DataFrame(
            {
                "id": ["A"],
                "date": [date(2025, 6, 1)],
                "value": [0.5],
                "extra": ["junk"],
            }
        )
        result = MetaculusSource._finalize_resolution_df(df)
        assert list(result.columns) == ["id", "date", "value"]


# ---------------------------------------------------------------------------
# _create_resolution_file
# ---------------------------------------------------------------------------


class TestCreateResolutionFile:
    """Tests for MetaculusSource._create_resolution_file."""

    def _dfq_row(self, resolved=False, resolution_datetime="N/A"):
        return make_question_df(
            [
                {
                    "id": "42472",
                    "resolved": resolved,
                    "market_info_resolution_datetime": resolution_datetime,
                }
            ]
        )

    def test_empty_history_returns_none(self, metaculus_source):
        """Market with empty history returns None."""
        market = make_metaculus_market(
            question={"aggregations": {"recency_weighted": {"history": []}}}
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is None

    def test_single_day_entries_filtered(self, metaculus_source):
        """Entries where start_date == end_date (not last-ms) are filtered out."""
        # Both start and end on the same day, not last millisecond -> filtered
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00 UTC
                                "end_time": 1735700400.0,  # 2025-01-01 03:00 UTC
                                "centers": [0.5],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is None

    def test_single_day_last_millisecond_kept(self, metaculus_source):
        """Entry with end_datetime at 23:59:59.999999 is kept even if same day."""
        # 2025-01-01 23:59:59.999999 is the last microsecond of the day
        epoch_last_ms = 1735689600.0 + 86399.999999  # 2025-01-01T23:59:59.999999 UTC
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00 UTC
                                "end_time": epoch_last_ms,
                                "centers": [0.5],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        assert len(result) >= 1

    def test_backfill_gaps(self, metaculus_source):
        """Missing dates between history entries are backfilled."""
        # History: Jan 1-2, then Jan 4-5 (gap on Jan 3)
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00
                                "end_time": 1735776000.0,  # 2025-01-02 00:00
                                "centers": [0.3],
                            },
                            {
                                "start_time": 1735948800.0,  # 2025-01-04 00:00
                                "end_time": 1736035200.0,  # 2025-01-05 00:00
                                "centers": [0.7],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # Should have continuous dates from start through last date
        dates_in_result = result["date"].tolist()
        assert len(dates_in_result) > 2  # more than just the two history entries

    def test_deduplication_keeps_last(self, metaculus_source):
        """Multiple entries for same date keep the last value."""
        # Two entries both mapping to the same date
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00
                                "end_time": 1735776000.0,  # 2025-01-02 00:00
                                "centers": [0.3],
                            },
                            {
                                "start_time": 1735700400.0,  # 2025-01-01 03:00
                                "end_time": 1735790400.0,  # 2025-01-02 04:00
                                "centers": [0.9],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # Both entries map to date 2025-01-01 (end_date - 1 day)
        # The second (0.9) should be kept
        row = result[result["date"] == "2025-01-01"]
        assert len(row) == 1
        assert row["value"].iloc[0] == 0.9

    def test_resolved_market_truncates_and_appends(self, metaculus_source):
        """Resolved market removes rows after resolution date and adds final row."""
        market = make_metaculus_market(
            resolved=True,
            question={
                "resolution": "yes",
                "actual_close_time": "2025-01-03T00:00:00Z",
                "actual_resolve_time": "2025-01-03T00:00:00Z",
            },
        )
        dfq = self._dfq_row(
            resolved=True,
            resolution_datetime="2025-01-03T00:00:00+00:00",
        )
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # Last row should be the resolution value (yes -> 1)
        assert float(result.iloc[-1]["value"]) == 1.0
        # Resolution date should be in the data
        assert "2025-01-03" in result["date"].tolist()

    def test_null_end_time_uses_today(self, metaculus_source, freeze_today):
        """When end_time is None, uses dates.get_datetime_today()."""
        freeze_today(date(2025, 1, 5))
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00
                                "end_time": 1735776000.0,  # 2025-01-02 00:00
                                "centers": [0.4],
                            },
                            {
                                "start_time": 1735776000.0,  # 2025-01-02 00:00
                                "end_time": None,  # ongoing
                                "centers": [0.6],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # Should have dates from Dec 31 through Jan 4 (today - 1 day)
        assert len(result) >= 2

    def test_future_dates_dropped(self, metaculus_source, freeze_today):
        """Forecast periods with future end_times are capped at yesterday."""
        freeze_today(date(2025, 1, 5))  # yesterday = 2025-01-04
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735689600.0,  # 2025-01-01 00:00
                                "end_time": 1735776000.0,  # 2025-01-02 00:00 -> date 2025-01-01
                                "centers": [0.3],
                            },
                            {
                                "start_time": 1736294400.0,  # 2025-01-08 00:00
                                "end_time": 1736467200.0,  # 2025-01-10 00:00 -> date 2025-01-09
                                "centers": [0.9],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # No date may exceed yesterday (2025-01-04); the future 2025-01-09 entry is dropped.
        assert max(result["date"].tolist()) <= "2025-01-04"
        assert "2025-01-09" not in result["date"].tolist()

    def test_future_only_unresolved_market_returns_none(self, metaculus_source, freeze_today):
        """Unresolved markets with no usable historical date do not produce empty files."""
        freeze_today(date(2025, 1, 5))  # yesterday = 2025-01-04
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1736294400.0,  # 2025-01-08 00:00
                                "end_time": 1736467200.0,  # 2025-01-10 00:00 -> date 2025-01-09
                                "centers": [0.9],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is None

    def test_date_assignment_subtracts_day(self, metaculus_source):
        """Regular end_datetime maps to date = end_date - 1 day."""
        # Single entry: start 2025-01-01, end 2025-01-02 00:00 UTC
        # end_date is 2025-01-02, not last millisecond -> date = 2025-01-01
        market = make_metaculus_market(
            question={
                "aggregations": {
                    "recency_weighted": {
                        "history": [
                            {
                                "start_time": 1735603200.0,  # 2024-12-31 00:00
                                "end_time": 1735776000.0,  # 2025-01-02 00:00
                                "centers": [0.5],
                            },
                        ]
                    }
                }
            }
        )
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        # end_date 2025-01-02 minus 1 day = 2025-01-01
        assert "2025-01-01" in result["date"].tolist()

    def test_id_is_string(self, metaculus_source):
        """Output id column contains string values."""
        market = make_metaculus_market()
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        assert all(isinstance(v, str) for v in result["id"].tolist())

    def test_output_validated_as_resolution_frame(self, metaculus_source):
        """Output passes ResolutionFrame.validate()."""
        market = make_metaculus_market()
        dfq = self._dfq_row()
        result = metaculus_source._create_resolution_file(dfq, 0, market)
        assert result is not None
        ResolutionFrame.validate(result)


# ---------------------------------------------------------------------------
# _call_search_endpoint (mock requests.get)
# ---------------------------------------------------------------------------


class TestCallSearchEndpoint:
    """Tests for MetaculusSource._call_search_endpoint."""

    def _mock_response(self, results):
        resp = Mock()
        resp.ok = True
        resp.json.return_value = {"results": results}
        resp.raise_for_status = Mock()
        return resp

    @patch("sources.metaculus.requests.get")
    def test_basic_search(self, mock_get, metaculus_source, freeze_today):
        """Returns set of IDs from eligible markets."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response(
            [
                make_metaculus_search_result(id=100),
                make_metaculus_search_result(id=200),
            ]
        )
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == {"100", "200"}

    @patch("sources.metaculus.requests.get")
    def test_filters_low_forecaster_count(self, mock_get, metaculus_source, freeze_today):
        """Markets with nr_forecasters <= 5 are excluded."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response(
            [
                make_metaculus_search_result(id=100, nr_forecasters=5),  # excluded (not >5)
                make_metaculus_search_result(id=200, nr_forecasters=6),  # included
            ]
        )
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == {"200"}

    @patch("sources.metaculus.requests.get")
    def test_filters_future_cp_reveal_time(self, mock_get, metaculus_source, freeze_today):
        """Markets with cp_reveal_time >= today are excluded."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response(
            [
                make_metaculus_search_result(
                    id=100, question={"cp_reveal_time": "2026-03-01T00:00:00Z"}
                ),  # today = excluded (must be < today)
                make_metaculus_search_result(
                    id=200, question={"cp_reveal_time": "2026-02-28T00:00:00Z"}
                ),  # past = included
            ]
        )
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == {"200"}

    @patch("sources.metaculus.requests.get")
    def test_filters_missing_cp_reveal_time(self, mock_get, metaculus_source, freeze_today):
        """Markets without cp_reveal_time field are excluded."""
        freeze_today(date(2026, 3, 1))
        result = make_metaculus_search_result(id=100)
        del result["question"]["cp_reveal_time"]
        mock_get.return_value = self._mock_response([result])
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == set()

    @patch("sources.metaculus.requests.get")
    def test_additional_params_merged(self, mock_get, metaculus_source, freeze_today):
        """Additional params are merged into the request."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response([])
        metaculus_source._call_search_endpoint(
            today=date(2026, 3, 1), additional_params={"categories": "artificial-intelligence"}
        )
        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs["params"]["categories"] == "artificial-intelligence"

    @patch("sources.metaculus.requests.get")
    def test_ids_are_strings(self, mock_get, metaculus_source, freeze_today):
        """Integer API IDs are returned as strings."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response([make_metaculus_search_result(id=42472)])
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert all(isinstance(i, str) for i in ids)

    @patch("sources.metaculus.requests.get")
    def test_empty_results(self, mock_get, metaculus_source, freeze_today):
        """No matching questions returns empty set."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response([])
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == set()

    @patch("sources.metaculus.requests.get")
    def test_deduplicates(self, mock_get, metaculus_source, freeze_today):
        """Duplicate IDs in response are deduplicated via set."""
        freeze_today(date(2026, 3, 1))
        mock_get.return_value = self._mock_response(
            [
                make_metaculus_search_result(id=100),
                make_metaculus_search_result(id=100),
            ]
        )
        ids = metaculus_source._call_search_endpoint(today=date(2026, 3, 1))
        assert ids == {"100"}


# ---------------------------------------------------------------------------
# _get_market (mock requests.get and time.sleep)
# ---------------------------------------------------------------------------


class TestGetMarket:
    """Tests for MetaculusSource._get_market."""

    @patch("sources.metaculus.requests.get")
    def test_success(self, mock_get, metaculus_source):
        """200 response returns JSON dict."""
        resp = Mock()
        resp.status_code = 200
        resp.ok = True
        resp.json.return_value = {"id": 42472, "title": "Test"}
        mock_get.return_value = resp

        result = metaculus_source._get_market("42472")
        assert result == {"id": 42472, "title": "Test"}

    @patch("sources.metaculus.time.sleep")
    @patch("sources.metaculus.requests.get")
    def test_429_retries_with_retry_after(self, mock_get, mock_sleep, metaculus_source):
        """429 with Retry-After header sleeps that duration then retries."""
        rate_resp = Mock()
        rate_resp.status_code = 429
        rate_resp.ok = False
        rate_resp.headers = {"Retry-After": "5"}

        ok_resp = Mock()
        ok_resp.status_code = 200
        ok_resp.ok = True
        ok_resp.json.return_value = {"id": 42472}

        mock_get.side_effect = [rate_resp, ok_resp]
        result = metaculus_source._get_market("42472")

        assert result == {"id": 42472}
        mock_sleep.assert_called_once_with(5)

    @patch("sources.metaculus.time.sleep")
    @patch("sources.metaculus.requests.get")
    def test_429_default_sleep(self, mock_get, mock_sleep, metaculus_source):
        """429 without Retry-After header sleeps 10 seconds."""
        rate_resp = Mock()
        rate_resp.status_code = 429
        rate_resp.ok = False
        rate_resp.headers = {}

        ok_resp = Mock()
        ok_resp.status_code = 200
        ok_resp.ok = True
        ok_resp.json.return_value = {"id": 42472}

        mock_get.side_effect = [rate_resp, ok_resp]
        metaculus_source._get_market("42472")

        mock_sleep.assert_called_once_with(10)

    @patch("sources.metaculus.time.sleep")
    @patch("sources.metaculus.requests.get")
    def test_429_all_retries_exhausted(self, mock_get, mock_sleep, metaculus_source):
        """Five consecutive 429s raises."""
        rate_resp = Mock()
        rate_resp.status_code = 429
        rate_resp.ok = False
        rate_resp.headers = {"Retry-After": "1"}
        rate_resp.raise_for_status.side_effect = Exception("429 Too Many Requests")

        mock_get.return_value = rate_resp
        with pytest.raises(Exception, match="429"):
            metaculus_source._get_market("42472")

        assert mock_sleep.call_count == 5

    @patch("sources.metaculus.requests.get")
    def test_non_429_error_raises(self, mock_get, metaculus_source):
        """Non-429 error raises immediately."""
        resp = Mock()
        resp.status_code = 500
        resp.ok = False
        resp.text = "Internal Server Error"
        resp.raise_for_status.side_effect = Exception("500 Server Error")

        mock_get.return_value = resp
        with pytest.raises(Exception, match="500"):
            metaculus_source._get_market("42472")


# ---------------------------------------------------------------------------
# fetch() (mock _call_search_endpoint)
# ---------------------------------------------------------------------------


class TestFetch:
    """Tests for MetaculusSource.fetch."""

    @patch.object(MetaculusSource, "_call_search_endpoint")
    def test_basic_fetch(self, mock_search, metaculus_source):
        """Returns MetaculusFetchFrame with discovered IDs."""
        mock_search.return_value = {"100", "200"}

        dff = metaculus_source.fetch()

        assert len(dff) == 2
        MetaculusFetchFrame.validate(dff)

    @patch.object(MetaculusSource, "_call_search_endpoint")
    def test_deduplicates_across_categories(self, mock_search, metaculus_source):
        """Same ID returned by multiple category searches appears once."""
        # First call (no category) returns {100, 200}
        # Category calls return overlapping sets
        mock_search.side_effect = [
            {"100", "200"},  # base search
        ] + [
            {"100", "300"} if i == 0 else set() for i in range(14)  # 14 categories
        ]

        dff = metaculus_source.fetch()

        assert set(dff["id"].tolist()) == {"100", "200", "300"}

    @patch.object(MetaculusSource, "_call_search_endpoint")
    def test_ids_sorted(self, mock_search, metaculus_source):
        """Output IDs are sorted."""
        mock_search.return_value = {"300", "100", "200"}

        dff = metaculus_source.fetch()

        assert dff["id"].tolist() == ["100", "200", "300"]

    def test_api_key_required(self):
        """Raises RuntimeError if api_key not set."""
        src = MetaculusSource()
        with pytest.raises(RuntimeError, match="api_key must be set"):
            src.fetch()


# ---------------------------------------------------------------------------
# update() (mock _get_market and _create_resolution_file)
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for MetaculusSource.update."""

    def _resolution_df(self, question_id="42472", value=0.6):
        """Build a minimal resolution DataFrame."""
        return pd.DataFrame(
            {
                "id": [str(question_id)],
                "date": ["2025-01-01"],
                "value": [value],
            }
        )

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_new_question_appended(self, mock_market, mock_res, metaculus_source):
        """ID in dff not in dfq gets appended with defaults."""
        mock_market.return_value = make_metaculus_market(id=200)
        mock_res.return_value = self._resolution_df("200")

        dfq = make_question_df([{"id": "100"}])
        dff = make_metaculus_fetch_df([200])

        result = metaculus_source.update(dfq, dff)

        assert "200" in result.dfq["id"].values
        row = result.dfq[result.dfq["id"] == "200"].iloc[0]
        assert row["freeze_datetime_value_explanation"] == "The community prediction."

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_existing_question_not_duplicated(self, mock_market, mock_res, metaculus_source):
        """ID already in dfq does not add a new row."""
        mock_market.return_value = make_metaculus_market(id=42472)
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert len(result.dfq) == 1

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_question_fields_updated(self, mock_market, mock_res, metaculus_source):
        """Unresolved question fields are updated from market data."""
        market = make_metaculus_market(
            id=42472,
            title="Updated title",
            question={
                "description": "New background",
                "resolution_criteria": "New criteria",
            },
        )
        mock_market.return_value = market
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472", "question": "Old title"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        row = result.dfq.iloc[0]
        assert row["question"] == "Updated title"
        assert row["background"] == "New background"
        assert row["market_info_resolution_criteria"] == "New criteria"
        assert "metaculus.com/questions/42472" in row["url"]

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_background_empty_string_kept(self, mock_market, mock_res, metaculus_source):
        """Empty description string is stored as-is, not converted to 'N/A'."""
        market = make_metaculus_market(question={"description": ""})
        mock_market.return_value = market
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert result.dfq.iloc[0]["background"] == ""

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_background_missing_key_becomes_na(self, mock_market, mock_res, metaculus_source):
        """Missing description key becomes 'N/A'."""
        market = make_metaculus_market()
        del market["question"]["description"]
        mock_market.return_value = market
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert result.dfq.iloc[0]["background"] == "N/A"

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_resolved_market_sets_resolution_datetime(
        self, mock_market, mock_res, metaculus_source
    ):
        """Resolved market sets market_info_resolution_datetime to min(close, resolve)."""
        market = make_metaculus_market(
            resolved=True,
            question={
                "resolution": "yes",
                "actual_close_time": "2026-03-01T00:00:00Z",
                "actual_resolve_time": "2026-02-15T00:00:00Z",
            },
        )
        mock_market.return_value = market
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        row = result.dfq.iloc[0]
        assert bool(row["resolved"]) is True
        # min(close=March 1, resolve=Feb 15) = Feb 15
        assert "2026-02-15" in str(row["market_info_resolution_datetime"])

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_resolution_file_stored(self, mock_market, mock_res, metaculus_source):
        """Resolution file from _create_resolution_file is stored in result."""
        mock_market.return_value = make_metaculus_market()
        res_df = self._resolution_df()
        mock_res.return_value = res_df

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert "42472" in result.resolution_files

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_freeze_value_from_last_resolution(self, mock_market, mock_res, metaculus_source):
        """freeze_datetime_value is the last value in the resolution file."""
        mock_market.return_value = make_metaculus_market()
        mock_res.return_value = self._resolution_df(value=0.75)

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        # QuestionFrame coerces freeze_datetime_value to str
        assert str(result.dfq.iloc[0]["freeze_datetime_value"]) == "0.75"

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_freeze_value_na_when_no_resolution(self, mock_market, mock_res, metaculus_source):
        """freeze_datetime_value is 'N/A' when _create_resolution_file returns None."""
        mock_market.return_value = make_metaculus_market()
        mock_res.return_value = None

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert result.dfq.iloc[0]["freeze_datetime_value"] == "N/A"

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_market_api_failure_propagates(self, mock_market, mock_res, metaculus_source):
        """A persistent _get_market failure propagates (fail loudly), not silently skipped."""
        mock_market.side_effect = Exception("API down")

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        with pytest.raises(Exception, match="API down"):
            metaculus_source.update(dfq, dff)
        mock_res.assert_not_called()

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_resolved_missing_resolution_regenerated(self, mock_market, mock_res, metaculus_source):
        """Resolved question without existing resolution file triggers regeneration."""
        market = make_metaculus_market(
            id=42472,
            resolved=True,
            question={
                "resolution": "no",
                "actual_close_time": "2025-06-01T00:00:00Z",
                "actual_resolve_time": "2025-06-01T00:00:00Z",
            },
        )
        mock_market.return_value = market
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df(
            [
                {
                    "id": "42472",
                    "resolved": True,
                    "market_info_resolution_datetime": "2025-06-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_metaculus_fetch_df([])

        result = metaculus_source.update(dfq, dff)

        # Should have called _get_market for the resolved question's missing file
        mock_market.assert_called_once_with("42472")
        assert "42472" in result.resolution_files

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_resolved_with_existing_file_not_regenerated(
        self, mock_market, mock_res, metaculus_source
    ):
        """Resolved question whose file is already in storage is not regenerated."""
        dfq = make_question_df(
            [
                {
                    "id": "42472",
                    "resolved": True,
                    "market_info_resolution_datetime": "2025-06-01T00:00:00+00:00",
                }
            ]
        )
        dff = make_metaculus_fetch_df([])

        metaculus_source.update(dfq, dff, files_in_storage=["metaculus/42472.jsonl"])

        mock_market.assert_not_called()
        mock_res.assert_not_called()

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_cap_new_questions(self, mock_market, mock_res, metaculus_source):
        """New IDs exceeding _QUESTION_LIMIT - unresolved are capped."""
        mock_market.return_value = make_metaculus_market()
        mock_res.return_value = self._resolution_df()

        # Create dfq with 1999 unresolved questions
        rows = [{"id": str(i)} for i in range(1999)]
        dfq = make_question_df(rows)
        # Try to add 5 new
        dff = make_metaculus_fetch_df([2000, 2001, 2002, 2003, 2004])

        result = metaculus_source.update(dfq, dff)
        # Only 1 should be added (2000 - 1999 = 1)
        assert len(result.dfq) == 2000

    def test_api_key_required(self):
        """Raises RuntimeError if api_key not set."""
        src = MetaculusSource()
        dfq = make_question_df([{"id": "100"}])
        dff = make_metaculus_fetch_df([200])
        with pytest.raises(RuntimeError, match="api_key must be set"):
            src.update(dfq, dff)

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_forecast_horizons_always_na(self, mock_market, mock_res, metaculus_source):
        """Every updated row has forecast_horizons = 'N/A'."""
        mock_market.return_value = make_metaculus_market()
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        assert result.dfq.iloc[0]["forecast_horizons"] == "N/A"

    @patch.object(MetaculusSource, "_create_resolution_file")
    @patch.object(MetaculusSource, "_get_market")
    def test_valid_question_frame_output(self, mock_market, mock_res, metaculus_source):
        """result.dfq passes QuestionFrame.validate()."""
        mock_market.return_value = make_metaculus_market()
        mock_res.return_value = self._resolution_df()

        dfq = make_question_df([{"id": "42472"}])
        dff = make_metaculus_fetch_df([42472])

        result = metaculus_source.update(dfq, dff)
        QuestionFrame.validate(result.dfq)
