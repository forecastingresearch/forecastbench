"""Unit: cross-cutting invariants: ``update()`` idempotency and order-independent nullification."""

from datetime import date

import pandas as pd

from _fb_types import SourceQuestionBank
from helpers import constants
from resolve.explode_question_set import explode_question_set
from resolve.resolve_all import resolve_all
from sources.polymarket import PolymarketSource
from sources.registry import SOURCES
from tests.factories import make_polymarket_fetch_df, make_question_set_df

_PRICE_HISTORY = [{"date": f"2025-01-{day:02d}", "value": 0.4 + day * 0.01} for day in range(1, 32)]
_DATE_DRIVER = {"id": "date_driver", "source": "fred", "resolution_dates": ["2025-01-31"]}


def _empty_bank_frame():
    """The bank as the first nightly run reads it: no rows, the persisted question columns."""
    return pd.DataFrame(columns=constants.QUESTION_FILE_COLUMNS)


class TestUpdateIdempotency:
    """Running update() twice over the same fetch must not duplicate or drift questions."""

    def test_second_update_is_a_no_op_on_questions(self):
        dff = make_polymarket_fetch_df(
            [{"id": "m1", "historical_prices": [{"date": "2024-06-01", "value": 0.5}]}]
        )
        source = PolymarketSource()

        first = source.update(_empty_bank_frame(), dff.copy())
        second = source.update(first.dfq.copy(), dff.copy())

        pd.testing.assert_frame_equal(first.dfq, second.dfq)
        assert second.dfq["id"].tolist().count("m1") == 1
        pd.testing.assert_frame_equal(
            first.resolution_files["m1"].reset_index(drop=True),
            second.resolution_files["m1"].reset_index(drop=True),
        )


class TestNullificationOrderIndependence:
    """A nullified question is dropped from resolution regardless of its row position."""

    def _resolve_with_order(self, ids_in_order, nullified_id):
        dff = make_polymarket_fetch_df(
            [
                {"id": qid, "resolved": False, "historical_prices": _PRICE_HISTORY}
                for qid in ids_in_order
            ]
        )
        result = PolymarketSource().update(_empty_bank_frame(), dff)
        dfr = pd.concat(result.resolution_files.values(), ignore_index=True)
        dfr["date"] = pd.to_datetime(dfr["date"])
        bank = {"polymarket": SourceQuestionBank(dfq=result.dfq, dfr=dfr)}

        rows = [
            {"id": qid, "source": "polymarket", "resolution_dates": "N/A"} for qid in ids_in_order
        ]
        rows.append(_DATE_DRIVER)
        exploded = explode_question_set(make_question_set_df(rows), "2025-01-01")
        exploded = exploded[exploded["source"] == "polymarket"].copy()

        resolved, _ = resolve_all(
            exploded,
            question_bank=bank,
            sources={"polymarket": SOURCES["polymarket"]},
            forecast_due_date=date(2025, 1, 1),
        )
        return resolved

    def test_nullified_dropped_regardless_of_position(self, freeze_today):
        freeze_today(date(2025, 2, 1))
        nullified_id = sorted(PolymarketSource().get_nullified_ids())[0]

        first = self._resolve_with_order(["happy1", nullified_id], nullified_id)
        second = self._resolve_with_order([nullified_id, "happy1"], nullified_id)

        for resolved in (first, second):
            # Exactly the happy sibling survives, with a value: never vacuous over an empty frame.
            assert resolved["id"].tolist() == ["happy1"]
            assert resolved["resolved_to"].notna().all()
