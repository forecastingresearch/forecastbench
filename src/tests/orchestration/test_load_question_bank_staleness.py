"""The staleness guard must skip sources we no longer fetch but still catch the ones we do."""

from datetime import datetime
from unittest.mock import patch

import pytest

from orchestration import _io
from sources import MARKET_SOURCE_NAMES

TODAY = datetime(2026, 8, 3)
STALE = datetime(2026, 7, 1)


def _modified_times(active_time, infer_time):
    def _fn(source):
        return infer_time if source == "infer" else active_time

    return _fn


@patch.object(_io, "_build_question_bank", return_value={})
@patch.object(_io.gcp.storage, "get_last_modified_time", return_value=TODAY)
@patch.object(_io.dates, "get_date_today", return_value=TODAY.date())
@patch.object(_io.data_utils, "get_last_modified_time_of_dfq_from_cloud_storage")
def test_stale_unfetched_source_does_not_raise(mock_mtime, *_):
    assert "infer" in MARKET_SOURCE_NAMES
    mock_mtime.side_effect = _modified_times(active_time=TODAY, infer_time=STALE)
    _io.load_question_bank(sources_to_get=[])  # should not raise


@patch.object(_io, "_build_question_bank", return_value={})
@patch.object(_io.gcp.storage, "get_last_modified_time", return_value=TODAY)
@patch.object(_io.dates, "get_date_today", return_value=TODAY.date())
@patch.object(_io.data_utils, "get_last_modified_time_of_dfq_from_cloud_storage")
def test_stale_active_source_still_raises(mock_mtime, *_):
    mock_mtime.side_effect = _modified_times(active_time=STALE, infer_time=TODAY)
    with pytest.raises(ValueError, match="Market-based dfq files need updating"):
        _io.load_question_bank(sources_to_get=[])
