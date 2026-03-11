"""Tests for AcledSource: aggregation functions, _acled_resolve, hash mapping."""

from datetime import date, timedelta

import pandas as pd
import pytest

from sources.acled import AcledSource
from tests.conftest import make_acled_resolution_df

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

    def test_load_hash_mapping(self):
        source = AcledSource()
        source._load_hash_mapping(
            '{"hash1": {"key": "last30Days.gt.30DayAvgOverPast360Days", '
            '"country": "Somalia", "event_type": "Battles"}}'
        )
        assert "hash1" in source.hash_mapping
        assert source.hash_mapping["hash1"]["country"] == "Somalia"

    def test_load_empty_string(self):
        source = AcledSource()
        source._load_hash_mapping("")
        assert source.hash_mapping == {}

    def test_dump_hash_mapping(self):
        source = AcledSource()
        source.hash_mapping = {"h1": {"key": "test"}}
        result = source._dump_hash_mapping()
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
