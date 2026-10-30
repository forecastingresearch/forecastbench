"""Contract: every source with a real ``update()`` returns output conforming to the data schemas.

Run offline through ``offline_update_case``. The exact column set is the load-bearing check. Where
``_build_resolution_df`` is stubbed, the resolution-file check only proves that ``update()``
packages the frame; resolution content is covered in ``unit/sources/``.
"""

from contextlib import ExitStack

import pytest

from _schemas import QuestionFrame, ResolutionFrame
from helpers import constants
from tests._sources import IMPLEMENTED_SOURCES, UPDATE_ONLY_SOURCES

# serpapi persists a per-question resolution_criteria (its source-wide template is empty).
EXTRA_QUESTION_COLUMNS = {"serpapi": ["resolution_criteria"]}
# acled's questions resolve off its fetch file (``_io._read_acled_dfr``), so update() returns
# no resolution files, not an empty dict.
NO_RESOLUTION_FILE_SOURCES = {"acled"}


@pytest.mark.parametrize("name", IMPLEMENTED_SOURCES + UPDATE_ONLY_SOURCES)
def test_update_output_conforms(name, offline_update_case):
    """update() returns a QuestionFrame (exact columns) and ResolutionFrame resolution files."""
    with ExitStack() as stack:
        source, dfq, dff = offline_update_case(name, stack)
        result = source.update(dfq, dff)

    QuestionFrame.validate(result.dfq)
    # The schemas are strict=False and miss a leaked column, so pin the column set exactly.
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
