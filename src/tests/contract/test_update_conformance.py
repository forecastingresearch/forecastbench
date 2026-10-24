"""Contract: every source with a real ``update()`` returns output conforming to the data schemas.

Parametrized over ``IMPLEMENTED_SOURCES + UPDATE_ONLY_SOURCES`` (see ``tests/_sources.py``). Most
sources reach the network inside ``update()``, so the offline adapter (``offline_update_case``)
patches each one's resolution-building seam (``_build_resolution_df``, plus ``_get_market`` where
question fields come from a per-market request); ``dbnomics``, ``fred``, ``polymarket`` and
``serpapi`` need no seam because their ``update()`` reads everything out of the fetch frame. An
update-only source (``manifold``) has no fetch frame at all, so its case seeds the question bank
instead and passes ``dff=None``. This test asserts update()'s *assembly* contract, not its
resolution *content*.

The two assertions are of unequal strength — read them that way:

- **dfq columns (load-bearing).** ``update()`` assembles the question frame and chooses its
  columns, so pinning it to exactly ``QUESTION_FILE_COLUMNS`` catches real column leaks/drift.
  Schemas are ``strict=False`` (see ``_schemas.py``), so ``validate()`` alone won't catch a leaked
  column — hence the explicit column-set assertion alongside it. The one sanctioned extension is
  listed in ``EXTRA_QUESTION_COLUMNS``: ``serpapi`` persists a per-question
  ``resolution_criteria`` (its source-wide template is empty, and curation reads the column).
- **resolution files (packaging check only).** Wherever ``_build_resolution_df`` is stubbed it
  returns a frame that *already* conforms, so this half does not validate real resolution
  construction — it only proves ``update()`` keys the resolution dict and passes the frame through
  without mangling its columns. Resolution *content* (the real ``_build_resolution_df`` logic) is
  covered in ``unit/sources/``. ``acled`` is the one source that writes no resolution files at all
  (``NO_RESOLUTION_FILE_SOURCES``): its questions resolve straight off the aggregated fetch file
  (``_io._read_acled_dfr``), so for it the contract is that ``update()`` returns *none*, not an
  empty dict a driver would happily upload as nothing.
"""

from contextlib import ExitStack

import pytest

from _schemas import QuestionFrame, ResolutionFrame
from helpers import constants
from tests._sources import IMPLEMENTED_SOURCES, UPDATE_ONLY_SOURCES

# Columns a source's update() persists beyond QUESTION_FILE_COLUMNS, by design (see module doc).
EXTRA_QUESTION_COLUMNS = {"serpapi": ["resolution_criteria"]}
# Sources whose questions resolve off the fetch data, so update() produces no resolution files.
NO_RESOLUTION_FILE_SOURCES = {"acled"}


@pytest.mark.parametrize("name", IMPLEMENTED_SOURCES + UPDATE_ONLY_SOURCES)
def test_update_output_conforms(name, offline_update_case):
    """update() returns a QuestionFrame (exact columns) and ResolutionFrame resolution files."""
    with ExitStack() as stack:
        source, dfq, dff = offline_update_case(name, stack)
        result = source.update(dfq, dff)

    QuestionFrame.validate(result.dfq)
    # strict=False -> schema misses extra/leaked columns; pin the persisted column set exactly.
    expected_columns = constants.QUESTION_FILE_COLUMNS + EXTRA_QUESTION_COLUMNS.get(name, [])
    assert list(result.dfq.columns) == expected_columns

    if name in NO_RESOLUTION_FILE_SOURCES:
        assert result.resolution_files is None
        return
    # Non-empty so the resolution-file validation below can't be silently skipped.
    assert result.resolution_files, f"{name} update() produced no resolution files"
    for question_id, df in result.resolution_files.items():
        ResolutionFrame.validate(df)
        assert list(df.columns) == ["id", "date", "value"], question_id
