"""Tests for InferSource, whose upstream shut down so it can no longer be fetched."""

import pandas as pd
import pytest

from sources import MARKET_SOURCE_NAMES
from sources.infer import InferSource

from .conftest import make_question_df


def test_infer_still_a_market_source():
    assert "infer" in MARKET_SOURCE_NAMES


def test_infer_fetch_raises():
    with pytest.raises(RuntimeError, match="can no longer be fetched"):
        InferSource().fetch()


def test_infer_update_is_noop():
    dfq = make_question_df([{"id": "100"}])
    before = dfq.copy()
    result = InferSource().update(dfq)
    # No-op writes no resolution files and leaves the caller's questions untouched, so the
    # update job has nothing to upload.
    assert not result.resolution_files
    pd.testing.assert_frame_equal(result.dfq, before, check_dtype=False)
    pd.testing.assert_frame_equal(dfq, before, check_dtype=False)


def test_infer_nullified_questions_are_registered():
    # The questions INFER left unresolved must be nullified so they drop out of scoring
    # instead of silently resolving to NaN against a frozen resolution file.
    assert InferSource().nullified_questions
