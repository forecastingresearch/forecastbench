"""Tests for DatasetSource._resolve: binary comparison resolution."""

import pandas as pd
import pytest

from tests.conftest import make_forecast_df, make_question_df, make_resolution_df

# ---------------------------------------------------------------------------
# DatasetSource._resolve
# ---------------------------------------------------------------------------


class TestDatasetResolve:
    """Integration-style tests for dataset resolution. Tests input→output only."""

    def test_resolution_value_greater_than_due_date_resolves_to_1(self, dataset_source):
        """res_value > due_value → resolved_to = 1.0."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                {"id": "q1", "date": "2025-01-31", "value": 110},
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "fred"]
        assert result_source.iloc[0]["resolved_to"] == 1.0
        assert bool(result_source.iloc[0]["resolved"]) is True

    def test_resolution_value_equal_to_due_date_resolves_to_0(self, dataset_source):
        """res_value == due_value → resolved_to = 0.0 (not strictly greater)."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                {"id": "q1", "date": "2025-01-31", "value": 100},
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "fred"]
        assert result_source.iloc[0]["resolved_to"] == 0.0

    def test_resolution_value_less_than_due_date_resolves_to_0(self, dataset_source):
        """res_value < due_value → resolved_to = 0.0."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                {"id": "q1", "date": "2025-01-31", "value": 90},
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "fred"]
        assert result_source.iloc[0]["resolved_to"] == 0.0

    def test_missing_resolution_value_is_nan(self, dataset_source):
        """Missing value at resolution_date → resolved_to = NaN."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                # No value on 2025-01-31
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "fred"]
        assert pd.isna(result_source.iloc[0]["resolved_to"])

    def test_missing_due_date_value_is_nan(self, dataset_source):
        """Missing value at forecast_due_date → resolved_to = NaN."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df(
            [
                # No value on 2025-01-01
                {"id": "q1", "date": "2025-01-31", "value": 110},
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        result_source = result[result["source"] == "fred"]
        assert pd.isna(result_source.iloc[0]["resolved_to"])

    def test_combo_question_resolves(self, dataset_source):
        """Combo question: product of direction-adjusted sub-question resolutions."""
        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                },
                {
                    "id": "q2",
                    "source": "fred",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                },
                {
                    "id": ("q1", "q2"),
                    "source": "fred",
                    "direction": (1, -1),
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                },
            ]
        )
        dfq = make_question_df([{"id": "q1"}, {"id": "q2"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                {"id": "q1", "date": "2025-01-31", "value": 110},  # resolves to 1.0
                {"id": "q2", "date": "2025-01-01", "value": 100},
                {"id": "q2", "date": "2025-01-31", "value": 110},  # resolves to 1.0
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        combo_row = result[result["id"].apply(lambda x: isinstance(x, tuple))]
        assert len(combo_row) == 1
        # q1 dir=1 → 1.0, q2 dir=-1 → 1-1.0=0.0. Product = 0.0
        assert combo_row.iloc[0]["resolved_to"] == 0.0

    def test_combo_with_missing_sub_question_is_nan(self, dataset_source):
        """Combo with missing sub-question resolution → NaN."""
        df = make_forecast_df(
            [
                {
                    "id": "q1",
                    "source": "fred",
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                },
                {
                    "id": ("q1", "q_missing"),
                    "source": "fred",
                    "direction": (1, 1),
                    "forecast_due_date": "2025-01-01",
                    "resolution_date": "2025-01-31",
                },
            ]
        )
        dfq = make_question_df([{"id": "q1"}, {"id": "q_missing"}])
        dfr = make_resolution_df(
            [
                {"id": "q1", "date": "2025-01-01", "value": 100},
                {"id": "q1", "date": "2025-01-31", "value": 110},
                {"id": "q_missing", "date": "2025-01-01", "value": 50},
                # No q_missing at resolution date
            ]
        )

        result = dataset_source._resolve(df, dfq, dfr)
        combo_row = result[result["id"].apply(lambda x: isinstance(x, tuple))]
        # q_missing has no resolution value → combo can't compute → NaN
        assert pd.isna(combo_row.iloc[0]["resolved_to"])

    def test_missing_id_in_dfr_raises(self, dataset_source):
        """Missing resolution data for a question raises ValueError."""
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
        dfq = make_question_df([{"id": "q1"}])
        dfr = make_resolution_df([{"id": "q_other", "date": "2025-01-31", "value": 100}])

        with pytest.raises(ValueError, match="Missing resolution values"):
            dataset_source._resolve(df, dfq, dfr)
