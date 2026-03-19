"""Tests for resolve/resolve_all.py: orchestration of per-source resolution."""

import numpy as np
import pandas as pd
import pytest

from _types import SourceQuestionBank
from resolve.resolve_all import resolve_all
from tests.conftest import make_forecast_df, make_question_df, make_resolution_df


class _MockSource:
    """Minimal mock source that resolves all its rows to a fixed value."""

    def __init__(self, name, resolved_value=1.0):
        self.name = name
        self._resolved_value = resolved_value

    def resolve(self, df, dfq, dfr, *, as_of=None):
        mask = df["source"] == self.name
        df.loc[mask, "resolved_to"] = self._resolved_value
        df.loc[mask, "resolved"] = True
        return df


class TestResolveAll:
    """Test resolve_all orchestration."""

    def test_missing_source_raises(self):
        df = make_forecast_df(
            [{"id": "q1", "source": "unknown", "forecast_due_date": "2025-01-01"}]
        )
        with pytest.raises(ValueError, match="not able to resolve"):
            resolve_all(df, question_bank={}, sources={})

    def test_missing_question_bank_raises(self):
        df = make_forecast_df([{"id": "q1", "source": "test", "forecast_due_date": "2025-01-01"}])
        sources = {"test": _MockSource("test")}
        with pytest.raises(ValueError, match="question bank not found"):
            resolve_all(df, question_bank={}, sources=sources)

    def test_empty_dfq_raises(self):
        df = make_forecast_df([{"id": "q1", "source": "test", "forecast_due_date": "2025-01-01"}])
        sources = {"test": _MockSource("test")}
        qb = {
            "test": SourceQuestionBank(
                dfq=pd.DataFrame(),
                dfr=make_resolution_df([{"id": "q1", "date": "2025-01-01", "value": 1}]),
            )
        }
        with pytest.raises(ValueError, match="dfq empty"):
            resolve_all(df, question_bank=qb, sources=sources)

    def test_empty_dfr_raises(self):
        df = make_forecast_df([{"id": "q1", "source": "test", "forecast_due_date": "2025-01-01"}])
        sources = {"test": _MockSource("test")}
        qb = {"test": SourceQuestionBank(dfq=make_question_df([{"id": "q1"}]), dfr=pd.DataFrame())}
        with pytest.raises(ValueError, match="dfr empty"):
            resolve_all(df, question_bank=qb, sources=sources)

    def test_successful_resolution(self):
        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "test",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                }
            ]
        )
        sources = {"test": _MockSource("test", resolved_value=0.8)}
        qb = {
            "test": SourceQuestionBank(
                dfq=make_question_df([{"id": "q1"}]),
                dfr=make_resolution_df([{"id": "q1", "date": "2025-01-01", "value": 100}]),
            )
        }

        result = resolve_all(df, question_bank=qb, sources=sources)
        assert len(result) == 1
        assert result.iloc[0]["resolved_to"] == 0.8

    def test_drops_nan_resolved_to(self):
        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "test",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                }
            ]
        )
        sources = {"test": _MockSource("test", resolved_value=np.nan)}
        qb = {
            "test": SourceQuestionBank(
                dfq=make_question_df([{"id": "q1"}]),
                dfr=make_resolution_df([{"id": "q1", "date": "2025-01-01", "value": 100}]),
            )
        }

        result = resolve_all(df, question_bank=qb, sources=sources)
        assert len(result) == 0  # NaN resolved_to rows are dropped

    def test_drops_unresolved_dataset_rows(self):
        """Unresolved dataset rows are dropped."""

        class _DatasetMockSource:
            name = "fred"

            def resolve(self, df, dfq, dfr, *, as_of=None):
                # Leave rows unresolved (resolved=False)
                return df

        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                }
            ]
        )
        sources = {"fred": _DatasetMockSource()}
        qb = {
            "fred": SourceQuestionBank(
                dfq=make_question_df([{"id": "q1"}]),
                dfr=make_resolution_df([{"id": "q1", "date": "2025-01-01", "value": 100}]),
            )
        }

        result = resolve_all(df, question_bank=qb, sources=sources)
        assert len(result) == 0
