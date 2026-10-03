"""Contract: resolution is fail-fast and nullification-safe for every market source.

Registry-parametrized over ``MARKET_SOURCES``, so every market source in the registry is covered.
These assert the *real* guarantees of the resolve path rather than inventing tolerant behavior:

- a question id absent from ``dfr`` raises (``_validate_ids``) — resolution never fabricates a
  value for a question it has no data for;
- an unknown source raises in ``resolve_all`` rather than silently skipping;
- a nullified question is dropped from the resolved output regardless of siblings.

Dataset sources also call ``_validate_ids`` but have different value semantics, so they get their
own cases rather than being forced through this market-shaped harness.
"""

from datetime import date

import pandas as pd
import pytest

from _fb_types import SourceQuestionBank
from resolve.explode_question_set import explode_question_set
from resolve.resolve_all import resolve_all
from sources.registry import MARKET_SOURCES
from tests.factories import make_question_df, make_question_set_df, make_resolution_df

# A dataset "date driver" supplies the global resolution dates that market questions inherit during
# explode (a market-only set would have none); it is filtered out before resolving.
_DATE_DRIVER = {"id": "dd", "source": "fred", "resolution_dates": ["2025-01-31"]}
_FORECAST_DUE = date(2025, 1, 1)


def _exploded(name, ids):
    """Explode a question set for ``name`` (plus the date driver), keeping only this source."""
    rows = [{"id": qid, "source": name, "resolution_dates": "N/A"} for qid in ids]
    rows.append(_DATE_DRIVER)
    exploded = explode_question_set(make_question_set_df(rows), "2025-01-01")
    return exploded[exploded["source"] == name].copy()


@pytest.mark.parametrize("name", sorted(MARKET_SOURCES))
class TestResolveFailFast:
    """The resolve path fails loudly on missing data instead of fabricating resolutions."""

    def test_missing_resolution_for_id_raises(self, name, freeze_today):
        freeze_today(date(2025, 2, 1))
        dfq = make_question_df([{"id": "q1", "source": name}])
        # Non-empty dfr (so we pass the empty-bank guard) but lacking q1 -> _validate_ids raises.
        dfr = make_resolution_df([{"id": "other", "date": "2025-01-31", "value": 0.5}])
        bank = {name: SourceQuestionBank(dfq=dfq, dfr=dfr)}
        with pytest.raises(ValueError, match="Missing resolution"):
            resolve_all(
                _exploded(name, ["q1"]),
                question_bank=bank,
                sources={name: MARKET_SOURCES[name]},
                forecast_due_date=_FORECAST_DUE,
            )

    def test_unknown_source_raises(self, name, freeze_today):
        freeze_today(date(2025, 2, 1))
        with pytest.raises(ValueError, match="not able to resolve"):
            resolve_all(
                _exploded(name, ["q1"]),
                question_bank={},
                sources={},
                forecast_due_date=_FORECAST_DUE,
            )


@pytest.mark.parametrize("name", sorted(MARKET_SOURCES))
def test_nullified_id_is_dropped(name, freeze_today):
    """A nullified question resolves to NaN and is dropped; a normal sibling still resolves."""
    freeze_today(date(2025, 2, 1))  # yesterday == 2025-01-31, matching the resolution date
    nullified = sorted(MARKET_SOURCES[name].get_nullified_ids(as_of=_FORECAST_DUE))
    if not nullified:
        pytest.skip(f"{name} has no nullified ids as of {_FORECAST_DUE}")
    nid = nullified[0]

    dfq = make_question_df([{"id": "norm1", "source": name}, {"id": nid, "source": name}])
    # Only the non-nullified id needs a resolution row (the nullified id is removed pre-_resolve).
    dfr = make_resolution_df([{"id": "norm1", "date": "2025-01-31", "value": 0.6}])
    bank = {name: SourceQuestionBank(dfq=dfq, dfr=dfr)}

    resolved, _ = resolve_all(
        _exploded(name, ["norm1", nid]),
        question_bank=bank,
        sources={name: MARKET_SOURCES[name]},
        forecast_due_date=_FORECAST_DUE,
    )

    # Exactly the sibling survives: an empty frame would pass the two membership checks alone.
    assert resolved["id"].tolist() == ["norm1"]
    assert resolved["resolved_to"].notna().all()


@pytest.mark.parametrize("name", sorted(MARKET_SOURCES))
def test_resolved_output_value_and_date_invariants(name, freeze_today):
    """Resolved values are probabilities in [0, 1] and never dated before the forecast due date."""
    freeze_today(date(2025, 2, 1))
    dfq = make_question_df([{"id": "q1", "source": name}])
    dfr = make_resolution_df([{"id": "q1", "date": "2025-01-31", "value": 0.6}])
    bank = {name: SourceQuestionBank(dfq=dfq, dfr=dfr)}

    resolved, _ = resolve_all(
        _exploded(name, ["q1"]),
        question_bank=bank,
        sources={name: MARKET_SOURCES[name]},
        forecast_due_date=_FORECAST_DUE,
    )

    assert resolved["id"].tolist() == ["q1"]  # never vacuous over an empty frame
    values = resolved["resolved_to"].dropna()
    assert len(values) == 1
    assert ((values >= 0) & (values <= 1)).all()
    res_dates = pd.to_datetime(resolved["resolution_date"])
    assert (res_dates >= pd.Timestamp(_FORECAST_DUE)).all()
