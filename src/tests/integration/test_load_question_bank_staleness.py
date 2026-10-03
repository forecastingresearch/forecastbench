"""The staleness guard must skip sources with a frozen dfq but still catch the ones we write."""

from datetime import datetime
from unittest.mock import patch

import pytest

from orchestration import _io
from sources import MARKET_SOURCE_NAMES

TODAY = datetime(2026, 8, 3)
STALE = datetime(2026, 7, 1)


def _modified_times(**by_source):
    """Stub `get_last_modified_time_of_dfq_from_cloud_storage`, per source or by `default`."""
    default = by_source.pop("default")

    def _fn(source):
        return by_source.get(source, default)

    return _fn


@patch.object(_io, "_build_question_bank", return_value={})
@patch.object(_io.gcp.storage, "get_last_modified_time", return_value=TODAY)
@patch.object(_io.dates, "get_date_today", return_value=TODAY.date())
@patch.object(_io.data_utils, "get_last_modified_time_of_dfq_from_cloud_storage")
def test_stale_frozen_source_does_not_raise(mock_mtime, *_):
    # INFER's update is a no-op, so nothing writes its dfq and it is always out of date.
    assert "infer" in MARKET_SOURCE_NAMES
    mock_mtime.side_effect = _modified_times(default=TODAY, infer=STALE)
    _io.load_question_bank(sources_to_get=[])  # should not raise


@patch.object(_io, "_build_question_bank", return_value={})
@patch.object(_io.gcp.storage, "get_last_modified_time", return_value=TODAY)
@patch.object(_io.dates, "get_date_today", return_value=TODAY.date())
@patch.object(_io.data_utils, "get_last_modified_time_of_dfq_from_cloud_storage")
def test_stale_unfetched_but_updated_source_still_raises(mock_mtime, *_):
    # Manifold is no longer fetched, but its update still writes dfq every night. A stale dfq
    # means the update job stopped running, which would resolve questions on stale market values.
    assert "manifold" in MARKET_SOURCE_NAMES
    mock_mtime.side_effect = _modified_times(default=TODAY, manifold=STALE)
    with pytest.raises(ValueError, match="Market-based dfq files need updating"):
        _io.load_question_bank(sources_to_get=[])


@patch.object(_io, "_build_question_bank", return_value={})
@patch.object(_io.gcp.storage, "get_last_modified_time", return_value=TODAY)
@patch.object(_io.dates, "get_date_today", return_value=TODAY.date())
@patch.object(_io.data_utils, "get_last_modified_time_of_dfq_from_cloud_storage")
def test_stale_active_source_still_raises(mock_mtime, *_):
    mock_mtime.side_effect = _modified_times(default=STALE, infer=TODAY)
    with pytest.raises(ValueError, match="Market-based dfq files need updating"):
        _io.load_question_bank(sources_to_get=[])
