"""Tests for helpers/dates.py: date conversion utilities."""

from datetime import date, datetime, timezone

import pytest

from helpers import dates


class TestConvertIsoStrToDate:
    """Test ISO string to date conversion."""

    def test_valid_date(self):
        assert dates.convert_iso_str_to_date("2025-01-15") == date(2025, 1, 15)

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            dates.convert_iso_str_to_date("01-15-2025")


class TestGetDateFunctions:
    """Test get_date_today, get_date_yesterday, etc."""

    def test_get_date_today_returns_date(self):
        result = dates.get_date_today()
        assert isinstance(result, date)

    def test_get_date_yesterday_is_one_day_before_today(self):
        today = dates.get_date_today()
        yesterday = dates.get_date_yesterday()
        assert (today - yesterday).days == 1

    def test_get_date_today_as_iso(self):
        result = dates.get_date_today_as_iso()
        # Should be YYYY-MM-DD format
        assert len(result) == 10
        assert result[4] == "-"

    def test_get_datetime_today_is_utc(self):
        result = dates.get_datetime_today()
        assert result.tzinfo is not None

    def test_get_datetime_today_midnight(self):
        result = dates.get_datetime_today_midnight()
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

    def test_get_datetime_now_format(self):
        result = dates.get_datetime_now()
        # Should be ISO format with seconds precision
        assert "T" in result
        assert "." not in result  # no microseconds


class TestEpochConversions:
    """Test epoch time conversions."""

    def test_epoch_sec_to_iso(self):
        result = dates.convert_epoch_time_in_sec_to_iso(1705524187)
        assert result == "2024-01-17T20:43:07+00:00"

    def test_epoch_sec_to_datetime(self):
        result = dates.convert_epoch_time_in_sec_to_datetime(1705524187)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 17

    def test_epoch_ms_to_iso(self):
        result = dates.convert_epoch_time_in_ms_to_iso(1705524187192)
        assert result == "2024-01-17T20:43:07+00:00"

    def test_epoch_ms_to_datetime(self):
        result = dates.convert_epoch_in_ms_to_datetime(1705524187192)
        assert result.year == 2024


class TestZuluConversions:
    """Test Zulu time conversions."""

    def test_zulu_to_datetime(self):
        result = dates.convert_zulu_to_datetime("2023-05-06T14:00:00Z")
        assert result.year == 2023
        assert result.month == 5
        assert result.day == 6
        assert result.hour == 14

    def test_zulu_to_iso(self):
        result = dates.convert_zulu_to_iso("2023-05-06T14:00:00Z")
        assert result == "2023-05-06T14:00:00+00:00"


class TestDatetimeConversions:
    """Test datetime format conversions."""

    def test_datetime_to_iso(self):
        dt = datetime(2023, 5, 6, 14, 0, tzinfo=timezone.utc)
        result = dates.convert_datetime_to_iso(dt)
        assert result == "2023-05-06T14:00:00+00:00"

    def test_iso_date_to_epoch_time(self):
        d = date(2024, 1, 1)
        result = dates.convert_iso_date_to_epoch_time(d)
        assert isinstance(result, int)
        assert result > 0

    def test_change_timezone_to_utc(self):
        result = dates.change_timezone_to_utc("2023-06-22T15:00:00.000-04:00")
        assert result == "2023-06-22T19:00:00+00:00"

    def test_convert_datetime_str_to_iso_utc_zulu(self):
        result = dates.convert_datetime_str_to_iso_utc("2023-06-22T19:00:00Z")
        assert result == "2023-06-22T19:00:00+00:00"

    def test_convert_datetime_str_to_iso_utc_with_offset(self):
        result = dates.convert_datetime_str_to_iso_utc("2023-06-22T15:00:00.000-04:00")
        assert result == "2023-06-22T19:00:00+00:00"

    def test_convert_datetime_str_invalid_raises(self):
        with pytest.raises(ValueError):
            dates.convert_datetime_str_to_iso_utc("not-a-date")
